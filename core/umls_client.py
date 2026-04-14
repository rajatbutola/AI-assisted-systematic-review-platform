# core/umls_client.py
#
# UMLS REST API client for cross-vocabulary concept lookup.
#
# Reference: https://documentation.uts.nlm.nih.gov/rest/home.html
#
# Authentication:
#   UMLS uses a two-step ticket-granting system (CAS):
#     Step 1 — POST apikey to UTS → get TGT (Ticket Granting Ticket, 8hr valid)
#     Step 2 — POST TGT → get ST (Service Ticket, single-use, expires in seconds)
#     Step 3 — Append ST to every API request
#   We cache the TGT for up to 7.5 hours (conservative margin on 8hr expiry).
#   Service tickets are fetched on demand — they expire after one use.
#
# Key endpoints used:
#   /search/current          — map free text to CUIs
#   /content/current/CUI/{} /atoms  — get all vocabulary atoms for a CUI
#   /content/current/CUI/{} /relations — get hierarchical relations
#
# Rate limit: 20 requests/second (generous for our use case)
# Caching: concept lookups cached for 30 days (concepts change slowly)

import time
import logging
import threading
import hashlib
import json
from typing import Optional, List, Dict, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field

import requests

from config.settings import UMLS_API_KEY

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

_UTS_BASE    = "https://uts-ws.nlm.nih.gov/rest"
_TGT_URL     = "https://utslogin.nlm.nih.gov/cas/v1/api-key"
_SERVICE_URL = "http://umlsks.nlm.nih.gov"
_TIMEOUT     = 15    # seconds per request
_TGT_TTL     = 7.5 * 3600   # 7.5 hours — conservative on 8hr CAS TTL
_CACHE_TTL   = 30 * 24 * 3600   # 30 days for concept lookups

# Vocabulary source abbreviations we care about
# Full list: https://www.nlm.nih.gov/research/umls/sourcereleasedocs/index.html
VOCAB_MSH    = "MSH"       # MeSH — PubMed
VOCAB_EMBASE = "MTH"       # UMLS Metathesaurus (covers many sources incl. Emtree)
VOCAB_SNOMED = "SNOMEDCT_US"
VOCAB_NCI    = "NCI"       # NCI Thesaurus
VOCAB_RXNORM = "RXNORM"    # Drug names
VOCAB_ICD10  = "ICD10CM"

# Semantic types that represent biomedical concepts we want
# Full list: https://metamap.nlm.nih.gov/Docs/SemanticTypes_2018AB.txt
_USEFUL_SEMTYPES = {
    "T047",  # Disease or Syndrome
    "T191",  # Neoplastic Process
    "T121",  # Pharmacologic Substance
    "T116",  # Amino Acid, Peptide, or Protein
    "T123",  # Biologically Active Substance
    "T125",  # Hormone
    "T126",  # Enzyme
    "T129",  # Immunologic Factor
    "T200",  # Clinical Drug
    "T061",  # Therapeutic or Preventive Procedure
    "T060",  # Diagnostic Procedure
    "T074",  # Medical Device
    "T039",  # Physiologic Function
    "T040",  # Organism Function
    "T042",  # Organ or Tissue Function
    "T201",  # Clinical Attribute
    "T033",  # Finding
    "T034",  # Laboratory or Test Result
    "T184",  # Sign or Symptom
    "T080",  # Qualitative Concept (for severity terms)
    "T081",  # Quantitative Concept
}


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class UMLSConcept:
    """A single UMLS concept with its vocabulary representations."""
    cui:           str
    name:          str                        # preferred name in UMLS
    semantic_types: List[str] = field(default_factory=list)  # e.g. ["T047"]
    semantic_names: List[str] = field(default_factory=list)  # e.g. ["Disease or Syndrome"]
    # Per-vocabulary preferred terms
    mesh_term:     Optional[str] = None       # MeSH preferred term
    mesh_ui:       Optional[str] = None       # MeSH UI code e.g. D015464
    rxnorm_id:     Optional[str] = None       # RxNorm code for drugs
    snomed_id:     Optional[str] = None       # SNOMED CT concept ID
    # All synonyms from all source vocabularies
    synonyms:      List[str] = field(default_factory=list)
    # Confidence from the /search endpoint (0.0–1.0 proxy)
    score:         float = 1.0

    @property
    def is_drug(self) -> bool:
        return any(t in ("T121","T200","T125","T126") for t in self.semantic_types)

    @property
    def is_disease(self) -> bool:
        return any(t in ("T047","T191","T033","T184") for t in self.semantic_types)

    @property
    def is_procedure(self) -> bool:
        return any(t in ("T061","T060","T074") for t in self.semantic_types)

    @property
    def best_search_term(self) -> str:
        """Return the best term to use in a database query."""
        return self.mesh_term or self.name

    def top_synonyms(self, n: int = 5) -> List[str]:
        """Return top N synonyms, deduplicated, excluding the preferred name."""
        seen = {self.name.lower(), (self.mesh_term or "").lower()}
        result = []
        for s in self.synonyms:
            sl = s.lower()
            if sl not in seen and len(s) > 2:
                seen.add(sl)
                result.append(s)
            if len(result) >= n:
                break
        return result


# ── In-process concept cache ───────────────────────────────────────────────────
# Simple dict-based cache — persists for the lifetime of the Python process.
# For a production deployment, replace with Redis or SQLite-backed cache.

_concept_cache: Dict[str, Tuple[UMLSConcept, float]] = {}   # key → (concept, timestamp)
_cache_lock = threading.Lock()

def _cache_get(key: str) -> Optional[UMLSConcept]:
    with _cache_lock:
        entry = _concept_cache.get(key)
        if entry and (time.time() - entry[1]) < _CACHE_TTL:
            return entry[0]
        return None

def _cache_set(key: str, concept: UMLSConcept) -> None:
    with _cache_lock:
        _concept_cache[key] = (concept, time.time())

def _cache_key(text: str) -> str:
    return hashlib.md5(text.lower().strip().encode()).hexdigest()


# ── UMLS Client (singleton per process) ───────────────────────────────────────

class UMLSClient:
    """
    Thread-safe UMLS REST API client.

    Usage
    -----
    client = UMLSClient()                        # reads UMLS_API_KEY from .env
    concepts = client.search("venetoclax")       # → List[UMLSConcept]
    concept  = client.search_best("CLL")         # → Optional[UMLSConcept]
    atoms     = client.get_atoms(cui, sabs="MSH") # → List[dict]
    """

    def __init__(self, api_key: str = ""):
        self._api_key  = api_key or UMLS_API_KEY
        self._tgt:     Optional[str] = None
        self._tgt_time: float = 0.0
        self._lock     = threading.Lock()
        self._session  = requests.Session()
        self._session.headers.update({"User-Agent": "SR-Platform/1.0"})

        if not self._api_key:
            logger.warning(
                "UMLS_API_KEY not set. Add it to your .env file. "
                "Register free at: https://uts.nlm.nih.gov/uts/signup-login"
            )

    # ── Authentication ─────────────────────────────────────────────────────────

    def _get_tgt(self) -> Optional[str]:
        """
        Get or refresh the Ticket Granting Ticket.
        Cached for 7.5 hours (CAS tickets last 8 hours).
        Thread-safe.
        """
        with self._lock:
            if self._tgt and (time.time() - self._tgt_time) < _TGT_TTL:
                return self._tgt

            if not self._api_key:
                return None

            try:
                resp = self._session.post(
                    _TGT_URL,
                    data={"apikey": self._api_key},
                    timeout=_TIMEOUT,
                )
                if resp.status_code == 201:
                    # TGT URL is in the Location header or response body href
                    tgt_url = resp.headers.get("location") or resp.url
                    if "tickets" in (resp.text or ""):
                        # Extract from HTML response body (older CAS format)
                        import re
                        m = re.search(r'action="([^"]+)"', resp.text)
                        if m:
                            tgt_url = m.group(1)

                    self._tgt      = tgt_url
                    self._tgt_time = time.time()
                    logger.info("UMLS TGT acquired successfully.")
                    return self._tgt
                else:
                    logger.error(
                        "UMLS TGT request failed: %d — %s",
                        resp.status_code, resp.text[:200]
                    )
                    return None
            except Exception as e:
                logger.error("UMLS TGT acquisition error: %s", e)
                return None

    def _get_service_ticket(self) -> Optional[str]:
        """Get a single-use service ticket from the TGT."""
        tgt = self._get_tgt()
        if not tgt:
            return None
        try:
            resp = self._session.post(
                tgt,
                data={"service": _SERVICE_URL},
                timeout=_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.text.strip()
            logger.error(
                "UMLS service ticket failed: %d — %s",
                resp.status_code, resp.text[:200]
            )
            return None
        except Exception as e:
            logger.error("UMLS service ticket error: %s", e)
            return None

    def _get(self, path: str, params: dict = None) -> Optional[dict]:
        """
        Make an authenticated GET request to the UMLS REST API.
        Automatically refreshes service ticket on 401.
        """
        st = self._get_service_ticket()
        if not st:
            return None

        p = dict(params or {})
        p["ticket"] = st

        try:
            resp = self._session.get(
                f"{_UTS_BASE}{path}",
                params=p,
                timeout=_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 401:
                # ST expired (race condition) — invalidate TGT and retry once
                with self._lock:
                    self._tgt = None
                st2 = self._get_service_ticket()
                if not st2:
                    return None
                p["ticket"] = st2
                resp2 = self._session.get(
                    f"{_UTS_BASE}{path}", params=p, timeout=_TIMEOUT
                )
                return resp2.json() if resp2.status_code == 200 else None
            if resp.status_code == 404:
                return None   # concept not found — not an error
            logger.warning("UMLS GET %s → %d", path, resp.status_code)
            return None
        except Exception as e:
            logger.error("UMLS GET %s error: %s", path, e)
            return None

    # ── Public search API ──────────────────────────────────────────────────────

    def search(self, query: str,
               max_results: int = 5,
               search_type: str = "words") -> List[UMLSConcept]:
        """
        Search UMLS for concepts matching a free-text query.

        Parameters
        ----------
        query       : Free text e.g. "chronic myeloid leukemia"
        max_results : Max concepts to return (default 5)
        search_type : "words" (default), "exact", "approximate", "normalizedWords"

        Returns
        -------
        List[UMLSConcept] sorted by relevance, filtered to useful semantic types.
        """
        if not query or not query.strip():
            return []

        cache_key = _cache_key(f"search:{query}:{max_results}")
        # For search we don't cache the full list — too variable
        # Individual concept enrichment is cached below

        data = self._get("/search/current", params={
            "string":        query.strip(),
            "searchType":    search_type,
            "pageSize":      max_results * 2,   # over-fetch, filter by semtype
            "returnIdType":  "concept",
        })

        if not data:
            return []

        results_raw = data.get("result", {}).get("results", [])
        concepts    = []

        for r in results_raw:
            cui  = r.get("ui", "")
            name = r.get("name", "")
            if not cui or cui == "NONE" or not name:
                continue

            # Enrich with atoms (vocabulary terms + synonyms)
            concept = self._enrich_concept(cui, name)
            if concept:
                concepts.append(concept)
            if len(concepts) >= max_results:
                break

        return concepts

    def search_best(self, query: str) -> Optional[UMLSConcept]:
        """
        Return the single best UMLS concept for a query.
        Filters to biomedically relevant semantic types.
        """
        results = self.search(query, max_results=3)
        if not results:
            return None

        # Prefer concepts with MeSH terms (most useful for PubMed)
        with_mesh = [c for c in results if c.mesh_term]
        return with_mesh[0] if with_mesh else results[0]

    def get_atoms(self, cui: str,
                  sabs: str = "",
                  language: str = "ENG") -> List[dict]:
        """
        Get all vocabulary atoms for a CUI.
        Optionally filter by source vocabulary abbreviation (sabs).

        Parameters
        ----------
        cui      : UMLS Concept Unique Identifier e.g. "C0023473"
        sabs     : Source vocab e.g. "MSH" for MeSH, "" for all
        language : Default "ENG" (English only)

        Returns
        -------
        List of atom dicts with: ui, name, rootSource, termType, language
        """
        params = {"language": language, "pageSize": 100}
        if sabs:
            params["sabs"] = sabs

        data = self._get(f"/content/current/CUI/{cui}/atoms", params=params)
        if not data:
            return []
        return data.get("result", [])

    def get_mesh_term(self, cui: str) -> Optional[str]:
        """Get the MeSH preferred term for a CUI, or None if not in MeSH."""
        atoms = self.get_atoms(cui, sabs=VOCAB_MSH)
        # MeSH Main Headings have termType "MH" or "PM" (entry term)
        # Prefer MH (Main Heading)
        for atom in atoms:
            if atom.get("termType") == "MH":
                return atom.get("name")
        # Fall back to any MeSH atom
        if atoms:
            return atoms[0].get("name")
        return None

    def get_synonyms(self, cui: str,
                     max_synonyms: int = 10,
                     preferred_vocabs: List[str] = None) -> List[str]:
        """
        Get English synonyms for a CUI from all source vocabularies.
        Ordered by: preferred vocabs first, then alphabetical.

        Parameters
        ----------
        cui            : UMLS CUI
        max_synonyms   : Maximum synonyms to return
        preferred_vocabs: Return these vocabs' terms first
                         Defaults to [MSH, RXNORM, NCI]
        """
        if preferred_vocabs is None:
            preferred_vocabs = [VOCAB_MSH, VOCAB_RXNORM, VOCAB_NCI, VOCAB_SNOMED]

        all_atoms = self.get_atoms(cui, language="ENG")
        if not all_atoms:
            return []

        preferred, others = [], []
        seen = set()

        for atom in all_atoms:
            name = atom.get("name", "").strip()
            if not name or name.lower() in seen:
                continue
            # Skip very long strings (titles, not terms)
            if len(name) > 100:
                continue
            # Skip atoms that look like codes
            if name.isupper() and len(name) < 8:
                continue
            seen.add(name.lower())

            src = atom.get("rootSource", "")
            if src in preferred_vocabs:
                preferred.append(name)
            else:
                others.append(name)

        combined = preferred + others
        return combined[:max_synonyms]

    def get_broader_concepts(self, cui: str) -> List[str]:
        """
        Get broader (parent) CUIs for hierarchy explosion.
        Useful for including narrower disease subtypes in a query.
        """
        data = self._get(f"/content/current/CUI/{cui}/relations", params={
            "relationLabels": "CHD",   # CHD = has child (narrower terms)
            "pageSize": 20,
        })
        if not data:
            return []
        relations = data.get("result", [])
        return [r.get("relatedId", "").split("/")[-1]
                for r in relations if r.get("relatedId")]

    # ── Internal enrichment ────────────────────────────────────────────────────

    def _enrich_concept(self, cui: str, name: str) -> Optional[UMLSConcept]:
        """
        Build a full UMLSConcept by fetching atoms and semantic types.
        Results are cached for 30 days.
        """
        cached = _cache_get(cui)
        if cached:
            return cached

        # Get semantic types from concept details
        sem_types, sem_names = self._get_semantic_types(cui)

        # Filter: only keep biomedically relevant concepts
        if sem_types and not any(t in _USEFUL_SEMTYPES for t in sem_types):
            logger.debug("CUI %s (%s) filtered out: semtypes %s",
                         cui, name, sem_types)
            return None

        # Get all English atoms
        all_atoms = self.get_atoms(cui, language="ENG")

        # Extract per-vocabulary data
        mesh_term  = None
        mesh_ui    = None
        rxnorm_id  = None
        snomed_id  = None
        synonyms   = []
        seen_names = {name.lower()}

        for atom in all_atoms:
            atom_name = atom.get("name", "").strip()
            src       = atom.get("rootSource", "")
            tt        = atom.get("termType", "")
            atom_ui   = atom.get("ui", "")

            # Collect synonym (deduplicated)
            if atom_name and atom_name.lower() not in seen_names:
                if len(atom_name) <= 100 and not atom_name.isupper():
                    synonyms.append(atom_name)
                    seen_names.add(atom_name.lower())

            # Per-vocabulary IDs
            if src == VOCAB_MSH:
                if tt == "MH" and not mesh_term:
                    mesh_term = atom_name
                    mesh_ui   = atom_ui
                elif not mesh_term:
                    mesh_term = atom_name

            elif src == VOCAB_RXNORM and not rxnorm_id:
                rxnorm_id = atom_ui

            elif src == VOCAB_SNOMED and not snomed_id:
                snomed_id = atom_ui

        concept = UMLSConcept(
            cui            = cui,
            name           = name,
            semantic_types = sem_types,
            semantic_names = sem_names,
            mesh_term      = mesh_term,
            mesh_ui        = mesh_ui,
            rxnorm_id      = rxnorm_id,
            snomed_id      = snomed_id,
            synonyms       = synonyms[:30],   # cap stored synonyms
        )

        _cache_set(cui, concept)
        return concept

    def _get_semantic_types(self, cui: str) -> Tuple[List[str], List[str]]:
        """Get semantic type codes and labels for a CUI."""
        data = self._get(f"/content/current/CUI/{cui}", params={})
        if not data:
            return [], []
        sem_types = data.get("result", {}).get("semanticTypes", [])
        codes  = [st.get("uri", "").split("/")[-1] for st in sem_types]
        labels = [st.get("name", "") for st in sem_types]
        return codes, labels

    @property
    def available(self) -> bool:
        """Returns True if UMLS API key is configured."""
        return bool(self._api_key)


# ── Module-level singleton ─────────────────────────────────────────────────────
# Import this instance everywhere — TGT is cached across all calls

_client: Optional[UMLSClient] = None

def get_umls_client() -> UMLSClient:
    """Get the module-level singleton UMLSClient."""
    global _client
    if _client is None:
        _client = UMLSClient()
    return _client


