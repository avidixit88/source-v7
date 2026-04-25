from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urljoin, urlparse
import re
import requests
from bs4 import BeautifulSoup

from services.supplier_adapters import ADAPTERS, direct_search_results, supplier_name_for_url

DEFAULT_SUPPLIER_DOMAINS = [domain for adapter in ADAPTERS for domain in adapter.domains]
SUPPLIER_NAME_HINTS = {domain: adapter.name for adapter in ADAPTERS for domain in adapter.domains}


@dataclass(frozen=True)
class SearchResult:
    title: str
    url: str
    snippet: str
    source: str
    supplier_hint: str = ""


def supplier_hint_from_url(url: str) -> str:
    return supplier_name_for_url(url)


def build_cas_supplier_queries(cas_number: str, chemical_name: str | None = None) -> list[str]:
    cas = cas_number.strip()
    chem = (chemical_name or "").strip()
    base_terms = [
        f'"{cas}" supplier price',
        f'"{cas}" catalog price',
        f'"{cas}" buy chemical',
        f'"{cas}" quote',
        f'"{cas}" "pack size" price',
        f'"{cas}" "CAS" "Price"',
        f'"{cas}" MedChemExpress OR MolPort OR eMolecules',
        f'"{cas}" Fisher Sigma TCI price',
    ]
    if chem:
        base_terms.extend([
            f'"{cas}" "{chem}" supplier',
            f'"{chem}" "{cas}" price',
            f'"{chem}" "{cas}" "pack size"',
        ])
    return base_terms


def direct_supplier_search_urls(cas_number: str) -> list[SearchResult]:
    """v7 adapter registry seed URLs. This replaces hardcoded direct links with a maintained supplier-source catalog."""
    return direct_search_results(cas_number.strip())


def serpapi_search(
    queries: Iterable[str],
    api_key: str,
    max_results_per_query: int = 8,
    timeout: int = 20,
) -> list[SearchResult]:
    if not api_key:
        return []
    results: list[SearchResult] = []
    seen_urls: set[str] = set()
    endpoint = "https://serpapi.com/search.json"
    for query in queries:
        params = {"engine": "google", "q": query, "api_key": api_key, "num": max_results_per_query}
        try:
            response = requests.get(endpoint, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
        except Exception:
            continue
        for item in payload.get("organic_results", [])[:max_results_per_query]:
            url = item.get("link") or ""
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            results.append(SearchResult(
                title=item.get("title") or "Untitled search result",
                url=url,
                snippet=item.get("snippet") or "",
                source="serpapi",
                supplier_hint=supplier_hint_from_url(url),
            ))
    return results


def filter_likely_supplier_results(results: list[SearchResult]) -> list[SearchResult]:
    filtered: list[SearchResult] = []
    seen: set[str] = set()
    for result in results:
        if result.url in seen:
            continue
        seen.add(result.url)
        haystack = f"{result.title} {result.url} {result.snippet}".lower()
        if any(domain in haystack for domain in DEFAULT_SUPPLIER_DOMAINS):
            filtered.append(result)
            continue
        if any(term in haystack for term in ["supplier", "price", "quote", "buy", "catalog", "chemical", "cas"]):
            filtered.append(result)
    return filtered


_PRODUCT_HINT_RE = re.compile(r"(product|catalog|item|sku|compound|chemical|shop|store|/p/|/pd/|details|order|cart)", re.I)
_BAD_LINK_RE = re.compile(
    r"(privacy|terms|basket|login|signin|register|contact|about|careers|linkedin|facebook|twitter|youtube|instagram|cookie|pdf|orders$|order-status|quick-order|promotions|sustainable|all-product-categories|clear-all-filters|clear filters)",
    re.I,
)


def _same_domain(url_a: str, url_b: str) -> bool:
    try:
        a = urlparse(url_a).netloc.replace("www.", "")
        b = urlparse(url_b).netloc.replace("www.", "")
        return a and b and (a == b or a.endswith("." + b) or b.endswith("." + a))
    except Exception:
        return False


def _clean_short(text: str, limit: int = 180) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text[:limit]


def _node_context(a_tag, limit: int = 1500) -> str:
    contexts = []
    for parent in [a_tag, a_tag.parent, a_tag.find_parent("li"), a_tag.find_parent("tr"), a_tag.find_parent("div")]:
        if parent is None:
            continue
        txt = parent.get_text(" ", strip=True)
        if txt and txt not in contexts:
            contexts.append(txt)
    return _clean_short(" | ".join(contexts), limit)


def _link_score(href: str, text: str, context: str, cas_number: str) -> int:
    hay = f"{href} {text} {context}".lower()
    score = 0
    if cas_number.lower() in hay:
        score += 70
    if _PRODUCT_HINT_RE.search(hay):
        score += 15
    if any(term in hay for term in ["price", "pricing", "$", "pack", "size", "purity", "assay", "cas"]):
        score += 10
    if _BAD_LINK_RE.search(hay):
        score -= 100
    if len(text.strip()) < 3 and cas_number.lower() not in href.lower():
        score -= 20
    return score


def discover_product_links_from_page(result: SearchResult, cas_number: str, timeout: int = 12, max_links: int = 8) -> list[SearchResult]:
    """Open a supplier/search page and pull only strong product-detail candidates.

    v7 keeps v5 strictness but supplies cleaner supplier hints from the adapter registry.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; CAS-Sourcing-MVP/7.0; procurement research; human reviewed)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        resp = requests.get(result.url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
    except Exception:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    candidates: list[tuple[int, SearchResult]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(resp.url, a.get("href", ""))
        if not href.startswith("http") or href in seen:
            continue
        if not _same_domain(resp.url, href):
            continue
        text = _clean_short(a.get_text(" ", strip=True))
        context = _node_context(a)
        score = _link_score(href, text, context, cas_number)
        if score < 70:
            continue
        seen.add(href)
        candidates.append((score, SearchResult(
            title=text or result.title,
            url=href,
            snippet=f"Expanded from {result.url}. Context: {context[:500]}",
            source="expanded_product_link_v7",
            supplier_hint=result.supplier_hint or supplier_hint_from_url(result.url),
        )))
    candidates.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in candidates[:max_links]]
