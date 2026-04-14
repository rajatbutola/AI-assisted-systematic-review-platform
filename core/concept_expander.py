# core/concept_expander.py
#
# PICO concept normalisation and vocabulary-aware query expansion.
#
# This is the central translation layer between the user's PICO text and
# optimised, high-recall database-specific query strings.
#
# Pipeline for each PICO field:
#   1. UMLS /search → best matching CUI + semantic type
#   2. UMLS /atoms  → MeSH preferred term (for PubMed)
#   3. UMLS /atoms  → synonyms from all source vocabularies
#   4. ChEMBL fallback → if UMLS returns nothing (new drugs, not in UMLS yet)
#   5. Assemble ConceptSet with per-database query strings
#
# ConceptSet → query string per database:
#   PubMed:    ({MeSH term}[MeSH Terms]) OR ({syn1}[tiab]) OR ({syn2}[tiab])
#   Europe PMC: (TITLE:"{term}" OR ABSTRACT:"{term}") OR ...
#   CORE:       "{term}" OR "{syn1}" OR "{syn2}"  (free text)
#
# Design principles:
#   - Each PICO field is expanded independently
#   - Synonym count is capped to avoid query explosion (default top 5)
#   - Falls back gracefully if UMLS unavailable (returns original text)
#   - All results cached (UMLS calls cached in umls_client, ChEMBL cached here)
#   - New drugs: ChEMBL REST API (no licence needed, updates continuously)

import re
import time
import logging
import hashlib
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple

import requests

from core.umls_client import get_umls_client, UMLSConcept

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

MAX_SYNONYMS_PUBMED   = 5   # per concept in PubMed [tiab] expansion
MAX_SYNONYMS_FREE     = 4   # per concept in free-text (EPMC / CORE)
MIN_SYNONYM_LENGTH    = 3   # ignore 1-2 char synonyms
MAX_SYNONYM_LENGTH    = 80  # ignore very long synonyms (likely full sentences)

_CHEMBL_BASE = "https://www.ebi.ac.uk/chembl/api/data"
_CHEMBL_TIMEOUT = 10

# Simple in-process cache for ChEMBL fallback results
_chembl_cache: Dict[str, List[str]] = {}


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class FieldExpansion:
    """
    Expanded concept data for a single PICO field.
    Contains the original text, resolved UMLS concept, and
    ready-to-use query strings for each target database.
    """
    original:     str                          # User's raw text
    concept:      Optional[UMLSConcept] = None # Best UMLS match
    synonyms:     List[str] = field(default_factory=list)  # Expanded synonyms
    mesh_term:    Optional[str] = None         # MeSH preferred term (for PubMed)
    source:       str = "original"             # "umls" / "chembl" / "original"
    confidence:   str = "low"                  # "high" / "moderate" / "low"

    # Ready-to-use query fragments (populated by ConceptExpander)
    pubmed_fragment:  str = ""   # e.g. "venetoclax[MeSH Terms] OR venetoclax[tiab]"
    epmc_fragment:    str = ""   # e.g. 'TITLE:"venetoclax" OR ABSTRACT:"venetoclax"'
    core_fragment:    str = ""   # e.g. '"venetoclax" OR "ABT-199"'

    @property
    def display_name(self) -> str:
        """Human-readable name for UI display."""
        if self.concept:
            return self.concept.mesh_term or self.concept.name
        return self.original

    @property
    def all_terms(self) -> List[str]:
        """All terms (preferred + synonyms), deduplicated."""
        terms = []
        seen  = set()
        for t in ([self.mesh_term or ""] +
                  [self.concept.name if self.concept else ""] +
                  self.synonyms +
                  [self.original]):
            tc = t.strip()
            if tc and tc.lower() not in seen:
                seen.add(tc.lower())
                terms.append(tc)
        return terms


@dataclass
class ConceptSet:
    """
    Fully expanded PICO concepts for one search.
    Contains per-field expansions and assembled full query strings.
    """
    population:   Optional[FieldExpansion] = None
    intervention: Optional[FieldExpansion] = None
    comparison:   Optional[FieldExpansion] = None
    outcome:      Optional[FieldExpansion] = None

    # Assembled full query strings per database
    pubmed_query:  str = ""
    epmc_query:    str = ""
    core_query:    str = ""

    # Metadata
    umls_used:    bool = False   # was UMLS successfully called?
    expansion_log: List[str] = field(default_factory=list)  # audit trail

    @property
    def fields(self) -> List[Tuple[str, Optional[FieldExpansion]]]:
        return [
            ("Population",   self.population),
            ("Intervention", self.intervention),
            ("Comparison",   self.comparison),
            ("Outcome",      self.outcome),
        ]

    def summary(self) -> str:
        """Human-readable expansion summary for UI display."""
        lines = []
        for label, fe in self.fields:
            if fe:
                src_badge = {"umls": "🔬 UMLS", "chembl": "💊 ChEMBL",
                             "original": "📝 Original"}.get(fe.source, fe.source)
                mesh = f" → MeSH: **{fe.mesh_term}**" if fe.mesh_term else ""
                syns = f" + {len(fe.synonyms)} synonyms" if fe.synonyms else ""
                lines.append(f"**{label}:** {fe.original!r}{mesh}{syns} ({src_badge})")
        return "\n".join(lines)


# ── Main expander ──────────────────────────────────────────────────────────────

class ConceptExpander:
    """
    Expands PICO fields to high-recall, vocabulary-aware query strings.

    Usage
    -----
    expander = ConceptExpander()
    cs = expander.expand(
        population="adults with acute myeloid leukemia",
        intervention="venetoclax",
        comparison="azacitidine",
        outcome="overall survival",
    )
    pubmed_query  = cs.pubmed_query   # full ready-to-use PubMed query
    epmc_query    = cs.epmc_query     # full Europe PMC query
    core_query    = cs.core_query     # full CORE free-text query
    """

    def __init__(self):
        self._umls = get_umls_client()

    @property
    def umls_available(self) -> bool:
        return self._umls.available

    def expand(self,
               population:   str = "",
               intervention: str = "",
               comparison:   str = "",
               outcome:      str = "",
               year_from:    int = 2015,
               year_to:      int = 2025,
               study_type_filter_pubmed: str = "",
               study_type_filter_epmc:  str = "") -> ConceptSet:
        """
        Expand PICO fields and assemble database-specific queries.

        Parameters
        ----------
        population, intervention, comparison, outcome : PICO fields (raw text)
        year_from, year_to : Publication year range
        study_type_filter_pubmed : e.g. '("randomized controlled trial"[pt])'
        study_type_filter_epmc   : e.g. 'PUB_TYPE:"clinical trial"'

        Returns
        -------
        ConceptSet with populated query strings for each database.
        """
        cs = ConceptSet()
        log = cs.expansion_log

        log.append(f"UMLS available: {self.umls_available}")

        # Expand each non-empty PICO field
        if population.strip():
            cs.population   = self._expand_field(population, "population", log)
        if intervention.strip():
            cs.intervention = self._expand_field(intervention, "intervention", log)
        if comparison.strip():
            cs.comparison   = self._expand_field(comparison, "comparison", log)
        if outcome.strip():
            cs.outcome      = self._expand_field(outcome, "outcome", log)

        cs.umls_used = any(
            fe and fe.source == "umls"
            for fe in [cs.population, cs.intervention, cs.comparison, cs.outcome]
        )

        # Assemble full query strings
        cs.pubmed_query = self._build_pubmed_query(
            cs, year_from, year_to, study_type_filter_pubmed
        )
        cs.epmc_query = self._build_epmc_query(
            cs, year_from, year_to, study_type_filter_epmc
        )
        cs.core_query = self._build_core_query(cs, year_from, year_to)

        log.append(f"PubMed query length: {len(cs.pubmed_query)} chars")
        return cs

    # ── Field expansion ────────────────────────────────────────────────────────

    def _expand_field(self, text: str, field_type: str,
                      log: List[str]) -> FieldExpansion:
        """
        Expand a single PICO field through UMLS → ChEMBL → fallback.
        """
        fe = FieldExpansion(original=text.strip())

        # ── Step 1: UMLS concept lookup ────────────────────────────────────────
        if self.umls_available:
            try:
                concept = self._umls_lookup(text, field_type)
                if concept:
                    fe.concept    = concept
                    fe.mesh_term  = concept.mesh_term
                    fe.source     = "umls"
                    fe.confidence = "high" if concept.mesh_term else "moderate"
                    # Get synonym expansion
                    fe.synonyms   = self._get_synonyms(concept, field_type)
                    log.append(
                        f"{field_type}: UMLS CUI={concept.cui} "
                        f"mesh={concept.mesh_term!r} "
                        f"synonyms={len(fe.synonyms)}"
                    )
            except Exception as e:
                logger.warning("UMLS lookup failed for %r: %s", text, e)
                log.append(f"{field_type}: UMLS error ({e}), trying ChEMBL")

        # ── Step 2: ChEMBL fallback (new drugs not in UMLS yet) ──────────────
        if not fe.concept and field_type == "intervention":
            try:
                chembl_syns = self._chembl_synonyms(text)
                if chembl_syns:
                    fe.synonyms   = chembl_syns
                    fe.source     = "chembl"
                    fe.confidence = "moderate"
                    log.append(
                        f"{field_type}: ChEMBL found {len(chembl_syns)} names"
                    )
            except Exception as e:
                logger.debug("ChEMBL lookup failed for %r: %s", text, e)

        # ── Step 3: Original text fallback (always works) ─────────────────────
        if fe.source == "original":
            log.append(f"{field_type}: no expansion found, using raw text")

        # ── Assemble per-database fragments ───────────────────────────────────
        fe.pubmed_fragment = self._pubmed_fragment(fe)
        fe.epmc_fragment   = self._epmc_fragment(fe)
        fe.core_fragment   = self._core_fragment(fe)

        return fe

    def _umls_lookup(self, text: str, field_type: str) -> Optional[UMLSConcept]:
        """
        Find the best UMLS concept for a PICO field text.
        Uses field_type to bias semantic type preference.
        """
        # Try exact phrase first, then individual words
        concept = self._umls.search_best(text)
        if concept:
            return concept

        # If multi-word, try the first two content words
        words = [w for w in text.split() if len(w) > 3]
        if len(words) >= 2:
            concept = self._umls.search_best(" ".join(words[:2]))
            if concept:
                return concept

        return None

    def _get_synonyms(self, concept: UMLSConcept,
                      field_type: str) -> List[str]:
        """
        Get cleaned, ranked synonyms for a concept.
        Caps based on field type (drugs get more synonyms than outcomes).
        """
        n = MAX_SYNONYMS_PUBMED
        raw = concept.top_synonyms(n=n * 3)   # fetch extra, filter down

        cleaned = []
        seen    = {concept.name.lower(), (concept.mesh_term or "").lower()}

        for s in raw:
            s = s.strip()
            if not s:
                continue
            if len(s) < MIN_SYNONYM_LENGTH or len(s) > MAX_SYNONYM_LENGTH:
                continue
            if s.lower() in seen:
                continue
            # Skip pure numeric strings
            if re.match(r'^\d+$', s):
                continue
            # Skip strings that look like codes (e.g. "D015464")
            if re.match(r'^[A-Z]\d{5,}$', s):
                continue
            seen.add(s.lower())
            cleaned.append(s)
            if len(cleaned) >= n:
                break

        return cleaned

    # ── ChEMBL fallback ────────────────────────────────────────────────────────

    def _chembl_synonyms(self, drug_name: str) -> List[str]:
        """
        Query ChEMBL for drug synonyms (trade names, INN, research codes).
        ChEMBL is free, no API key needed, updates faster than UMLS.
        Used as fallback for drugs not yet in UMLS (new approvals).
        """
        cache_key = f"chembl:{drug_name.lower().strip()}"
        if cache_key in _chembl_cache:
            return _chembl_cache[cache_key]

        synonyms = []
        try:
            # Search by preferred name
            resp = requests.get(
                f"{_CHEMBL_BASE}/molecule",
                params={
                    "pref_name__iexact": drug_name.strip(),
                    "format": "json",
                    "limit": 3,
                },
                timeout=_CHEMBL_TIMEOUT,
                headers={"User-Agent": "SR-Platform/1.0"},
            )
            if resp.status_code == 200:
                mols = resp.json().get("molecules", [])
                for mol in mols[:1]:   # take the best match only
                    # Preferred name
                    pref = mol.get("pref_name", "")
                    if pref and pref.lower() != drug_name.lower():
                        synonyms.append(pref)
                    # Synonyms from molecule_synonyms
                    for syn in mol.get("molecule_synonyms", []):
                        name = syn.get("molecule_synonym", "")
                        syn_type = syn.get("syn_type", "")
                        if (name and len(name) >= 3 and len(name) <= 60
                                and name.lower() not in {s.lower() for s in synonyms}
                                and syn_type in ("INN", "TRADE_NAME", "USAN",
                                                  "BAN", "JAN", "AAN", "FDA")):
                            synonyms.append(name)
                    # Research codes (e.g. ABT-199 for venetoclax)
                    for xref in mol.get("cross_references", []):
                        if xref.get("xref_src") == "PubChem":
                            pass  # skip numeric PubChem IDs
                        elif xref.get("xref_src") in ("ClinicalTrials", "FDA"):
                            code = xref.get("xref_id", "")
                            if code and re.match(r'^[A-Z]{2,}-\d+', code):
                                synonyms.append(code)

        except Exception as e:
            logger.debug("ChEMBL query failed: %s", e)

        # Cap and cache
        synonyms = synonyms[:MAX_SYNONYMS_FREE]
        _chembl_cache[cache_key] = synonyms
        return synonyms

    # ── Query fragment builders ────────────────────────────────────────────────

    def _pubmed_fragment(self, fe: FieldExpansion) -> str:
        """
        Build PubMed query fragment for one PICO field.

        Structure:
          If MeSH term known:
            ({mesh_term}[MeSH Terms]) OR ({original}[tiab]) OR ({syn1}[tiab]) ...
          Else:
            ({original}[tiab]) OR ({syn1}[tiab]) ...

        MeSH Terms search is explosion by default (includes narrower terms).
        """
        parts = []

        # MeSH term (most precise, high recall due to MeSH explosion)
        if fe.mesh_term:
            parts.append(f'"{fe.mesh_term}"[MeSH Terms]')

        # Original text as [tiab] (title/abstract)
        orig = fe.original.strip()
        if orig:
            orig_q = f'"{orig}"[tiab]' if " " in orig else f'{orig}[tiab]'
            parts.append(orig_q)

        # Synonyms as [tiab]
        for syn in fe.synonyms[:MAX_SYNONYMS_PUBMED]:
            syn_q = f'"{syn}"[tiab]' if " " in syn else f'{syn}[tiab]'
            if syn_q not in parts:
                parts.append(syn_q)

        if not parts:
            return ""
        return "(" + " OR ".join(parts) + ")"

    def _epmc_fragment(self, fe: FieldExpansion) -> str:
        """
        Build Europe PMC query fragment for one PICO field.

        Europe PMC searches TITLE and ABSTRACT fields.
        No MeSH — use all terms as free text.
        """
        parts = []
        seen  = set()

        def _add(term: str):
            tc = term.strip()
            if not tc or tc.lower() in seen:
                return
            seen.add(tc.lower())
            if " " in tc:
                parts.append(f'(TITLE:"{tc}" OR ABSTRACT:"{tc}")')
            else:
                parts.append(f'(TITLE:{tc} OR ABSTRACT:{tc})')

        _add(fe.mesh_term or fe.original)
        if fe.mesh_term:
            _add(fe.original)
        for syn in fe.synonyms[:MAX_SYNONYMS_FREE]:
            _add(syn)

        if not parts:
            return ""
        return "(" + " OR ".join(parts) + ")"

    def _core_fragment(self, fe: FieldExpansion) -> str:
        """
        Build CORE query fragment (free-text Boolean).
        CORE has no controlled vocabulary — use expanded term set.
        """
        terms = []
        seen  = set()

        def _add(t: str):
            tc = t.strip()
            if not tc or tc.lower() in seen:
                return
            seen.add(tc.lower())
            terms.append(f'"{tc}"' if " " in tc else tc)

        _add(fe.mesh_term or fe.original)
        if fe.mesh_term:
            _add(fe.original)
        for syn in fe.synonyms[:MAX_SYNONYMS_FREE]:
            _add(syn)

        if not terms:
            return ""
        return "(" + " OR ".join(terms) + ")"

    # ── Full query assemblers ──────────────────────────────────────────────────

    def _build_pubmed_query(self, cs: ConceptSet,
                            year_from: int, year_to: int,
                            study_type_filter: str) -> str:
        """Assemble complete PubMed Boolean query from ConceptSet."""
        fragments = []
        for _, fe in cs.fields:
            if fe and fe.pubmed_fragment:
                fragments.append(fe.pubmed_fragment)

        if not fragments:
            return ""

        query = " AND ".join(fragments)
        query += f" AND {year_from}:{year_to}[dp]"
        if study_type_filter:
            query += f" AND {study_type_filter}"

        return query

    def _build_epmc_query(self, cs: ConceptSet,
                          year_from: int, year_to: int,
                          study_type_filter: str) -> str:
        """Assemble complete Europe PMC query from ConceptSet."""
        fragments = []
        for _, fe in cs.fields:
            if fe and fe.epmc_fragment:
                fragments.append(fe.epmc_fragment)

        if not fragments:
            return ""

        query = " AND ".join(fragments)
        query += (f" AND FIRST_PDATE:[{year_from}-01-01 TO {year_to}-12-31]")
        if study_type_filter:
            query += f" AND {study_type_filter}"

        return query

    def _build_core_query(self, cs: ConceptSet,
                          year_from: int, year_to: int) -> str:
        """Assemble complete CORE free-text query from ConceptSet."""
        fragments = []
        for _, fe in cs.fields:
            if fe and fe.core_fragment:
                fragments.append(fe.core_fragment)

        if not fragments:
            return ""

        query = " AND ".join(fragments)
        query += f" AND yearPublished>={year_from} AND yearPublished<={year_to}"
        return query


# ── Module-level singleton ─────────────────────────────────────────────────────

_expander: Optional[ConceptExpander] = None

def get_concept_expander() -> ConceptExpander:
    """Get the module-level singleton ConceptExpander."""
    global _expander
    if _expander is None:
        _expander = ConceptExpander()
    return _expander


