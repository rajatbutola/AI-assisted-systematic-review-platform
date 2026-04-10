# core/pmc_client.py

import re
import time
import logging
import xml.etree.ElementTree as ET
from http.client import IncompleteRead
from typing import List, Optional

from Bio import Entrez

from config.settings import NCBI_EMAIL, NCBI_API_KEY
from core.base_client import BaseLiteratureClient
from models.schemas import Article, ArticleSource, ResearchDomain

logger = logging.getLogger(__name__)

Entrez.email   = NCBI_EMAIL
Entrez.api_key = NCBI_API_KEY


class PMCClient(BaseLiteratureClient):

    @property
    def domain(self) -> ResearchDomain:
        return ResearchDomain.MEDICAL

    @property
    def source_name(self) -> str:
        return "PMC Full-Text"

    def search(self, query: str, max_results: int = 1000) -> List[str]:
        if not query.strip():
            raise ValueError("Search query cannot be empty.")

        # Strip PubMed-only date syntax e.g. "2015:2024[dp]" — invalid in PMC db
        cleaned = re.sub(r'\s+AND\s+\d{4}:\d{4}\[dp\]', '', query.strip()).strip()

        # Extract year range and re-add in PMC-compatible format
        date_match = re.search(r'(\d{4}):(\d{4})\[dp\]', query)
        if date_match:
            y1, y2 = date_match.group(1), date_match.group(2)
            cleaned += f' AND ("{y1}/01/01"[PDAT] : "{y2}/12/31"[PDAT])'

        pmc_query = f"({cleaned}) AND open access[filter]"

        logger.info("PMC search: query = %r", pmc_query)

        for attempt in range(3):
            try:
                handle = Entrez.esearch(db="pmc", term=pmc_query, retmax=max_results)
                try:
                    record = Entrez.read(handle)
                finally:
                    handle.close()

                pmc_ids = record.get("IdList", [])
                logger.info("PMC search: %d IDs returned", len(pmc_ids))
                return pmc_ids

            except Exception as e:
                wait = 2 ** attempt
                logger.warning("PMC search attempt %d failed: %s. Retry in %ds.", attempt + 1, e, wait)
                if attempt < 2:
                    time.sleep(wait)

        raise RuntimeError("PMC search failed after all retries.")

    def fetch(self, ids: List[str], batch_size: int = 5) -> List[Article]:
        articles: List[Article] = []
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            raw_xml = self._fetch_batch_xml(batch)
            if raw_xml:
                articles.extend(self._parse_xml(raw_xml))
            time.sleep(0.34)  # stay under NCBI 3 req/sec limit
        logger.info("PMC fetch complete: %d articles from %d IDs", len(articles), len(ids))
        return articles

    def _fetch_batch_xml(self, pmc_ids: List[str], max_retries: int = 3) -> Optional[bytes]:
        for attempt in range(max_retries):
            try:
                handle = Entrez.efetch(
                    db="pmc",
                    id=",".join(pmc_ids),
                    rettype="full",
                    retmode="xml"
                )
                try:
                    raw_xml = handle.read()
                finally:
                    handle.close()
                logger.debug("PMC batch fetched: %d IDs, %d bytes", len(pmc_ids), len(raw_xml))
                return raw_xml
            except IncompleteRead:
                wait = 2 ** attempt
                logger.warning("IncompleteRead, retry in %ss", wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("PMC efetch error: %s", e)
                return None
        return None

    def _parse_xml(self, raw_xml: bytes) -> List[Article]:
        text = raw_xml.decode("utf-8", errors="replace")

        # Remove DOCTYPE to avoid external entity issues
        text = re.sub(r'<!DOCTYPE[^>]*>', '', text)

        # Strip namespace declarations
        text = re.sub(r'\s+xmlns(?::\w+)?="[^"]*"', '', text)

        # Strip namespace prefixes from BOTH opening and closing tags
        # e.g. <mml:math> → <math>, </xlink:href> → </href>
        text = re.sub(r'<(\w+):(\w)', r'<\2', text)
        text = re.sub(r'</(\w+):(\w)', r'</\2', text)

        # Strip namespace prefixes from attributes too
        # e.g. xlink:href="..." → href="..."
        text = re.sub(r'\b\w+:(\w+)=', r'\1=', text)

        # Replace common named entities
        _ENTITIES = {
            "&ndash;": "–", "&mdash;": "—", "&nbsp;": " ", "&hellip;": "…",
            "&alpha;": "α", "&beta;": "β", "&gamma;": "γ", "&delta;": "δ",
            "&mu;": "μ", "&sigma;": "σ", "&kappa;": "κ", "&lambda;": "λ",
            "&plusmn;": "±", "&ge;": "≥", "&le;": "≤", "&ne;": "≠",
            "&times;": "×", "&divide;": "÷", "&deg;": "°", "&middot;": "·",
            "&reg;": "®", "&copy;": "©", "&trade;": "™",
            "&rsquo;": "'", "&lsquo;": "\u2018",
            "&ldquo;": "\u201c", "&rdquo;": "\u201d",
            "&bull;": "•", "&dagger;": "†", "&rarr;": "→", "&larr;": "←",
            "&infin;": "∞", "&prime;": "′", "&sup2;": "²", "&sup3;": "³",
        }
        for entity, char in _ENTITIES.items():
            text = text.replace(entity, char)

        # Catch-all: remove any remaining unknown named entities
        text = re.sub(r'&(?!amp;|lt;|gt;|apos;|quot;)[a-zA-Z][a-zA-Z0-9]*;', '', text)

        raw_xml = text.encode("utf-8")

        parsed: List[Article] = []
        try:
            root = ET.fromstring(raw_xml)
        except ET.ParseError as e:
            logger.error("PMC XML parse error: %s", e)
            return parsed

        articles_el = root.findall("article")
        if not articles_el:
            articles_el = root.findall(".//article")

        logger.debug("PMC XML: found %d <article> elements", len(articles_el))

        for article_el in articles_el:
            try:
                a = self._parse_one_article(article_el)
                if a:
                    parsed.append(a)
            except Exception as e:
                logger.warning("PMC: failed to parse one article: %s", e, exc_info=True)

        logger.info("PMC XML parsed: %d/%d articles", len(parsed), len(articles_el))
        return parsed

    def _parse_one_article(self, article_el: ET.Element) -> Optional[Article]:
        front = article_el.find("front")
        if front is None:
            return None
        meta = front.find("article-meta")
        if meta is None:
            return None

        pmcid, pmid_val, doi_val = "", "", ""
        for aid in meta.findall("article-id"):
            t = (aid.text or "").strip()
            if aid.get("pub-id-type") == "pmc":
                pmcid = f"PMC{t}"
            elif aid.get("pub-id-type") == "pmid":
                pmid_val = t
            elif aid.get("pub-id-type") == "doi":
                doi_val = t

        uid = pmcid or pmid_val or doi_val
        if not uid:
            return None

        title = "No title"
        tg = meta.find("title-group")
        if tg is not None:
            at = tg.find("article-title")
            if at is not None:
                title = self._element_text(at).strip() or "No title"

        abstract = ""
        ab = meta.find("abstract")
        if ab is not None:
            abstract = self._element_text(ab).strip()

        authors: List[str] = []
        for cg in meta.findall("contrib-group"):
            for contrib in cg.findall("contrib"):
                if contrib.get("contrib-type") != "author":
                    continue
                name_el = contrib.find("name")
                if name_el is None:
                    continue
                sn = (name_el.findtext("surname") or "").strip()
                gn = (name_el.findtext("given-names") or "").strip()
                if sn or gn:
                    authors.append(f"{sn} {gn}".strip())

        journal = ""
        jm = front.find("journal-meta")
        if jm is not None:
            jtg = jm.find("journal-title-group")
            if jtg is not None:
                jt = jtg.find("journal-title")
                if jt is not None:
                    journal = (jt.text or "").strip()

        year = "N/A"
        for pd in meta.findall("pub-date"):
            ye = pd.find("year")
            if ye is not None and ye.text:
                year = ye.text.strip()
                break

        full_text = ""
        body = article_el.find("body")
        if body is not None:
            full_text = self._element_text(body).strip()

        url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/" if pmcid else None

        return Article(
            pmid=uid, title=title,
            abstract=abstract if abstract else "(see full text)",
            authors=authors, journal=journal, year=year,
            doi=doi_val or None,
            source=ArticleSource.PMC,
            domain=ResearchDomain.MEDICAL,
            url=url,
            full_text=full_text or None,
        )

    def _element_text(self, el: ET.Element) -> str:
        parts = []
        if el.text:
            parts.append(el.text)
        for child in el:
            parts.append(self._element_text(child))
            if child.tail:
                parts.append(child.tail)
        return " ".join(p.strip() for p in parts if p.strip())