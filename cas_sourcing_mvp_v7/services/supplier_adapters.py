from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import re
from typing import Iterable

PRICE_PUBLIC = "Public price extracted"
PRICE_SNIPPET = "Search-snippet price only"
PRICE_LOGIN = "Login/account price required"
PRICE_QUOTE = "Quote required"
PRICE_HIDDEN = "No public price detected"
PRICE_FAILED = "Extraction failed"

CATALOG_PATTERNS = [
    re.compile(r"\b(?:catalog|cat\.?|sku|item|part|product)\s*(?:no\.?|number|#)?\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]{3,35})\b", re.I),
    re.compile(r"/([A-Z]{1,6}\d{3,}[A-Z0-9-]*)\b", re.I),
]

QUOTE_RE = re.compile(r"(?i)(request\s+a?\s*quote|ask\s+for\s+quotation|quote\s+only|pricing\s+on\s+request|price\s+on\s+request|inquire)")
LOGIN_RE = re.compile(r"(?i)(sign\s*in\s*(?:or\s*register)?\s*to\s*(?:check|view|see)\s*(?:your\s*)?price|login\s*to\s*view\s*price|log\s*in\s*to\s*view\s*price|account\s*specific\s*price|your\s*price)")
PRICE_RE = re.compile(r"(?:US\$|\$)\s?([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)|\b([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)\s?(?:USD)\b", re.I)

@dataclass(frozen=True)
class SupplierAdapter:
    name: str
    domains: tuple[str, ...]
    search_url_templates: tuple[str, ...]
    notes: str
    public_price_likelihood: str = "mixed"
    search_priority: int = 50

    def matches(self, url: str) -> bool:
        host = urlparse(url).netloc.lower().replace("www.", "")
        return any(domain in host for domain in self.domains)


ADAPTERS: tuple[SupplierAdapter, ...] = (
    SupplierAdapter("Fisher Scientific", ("fishersci.com",), ("https://www.fishersci.com/us/en/catalog/search/products?keyword={cas}",), "Large catalog; often exposes product and pack data, but prices are frequently account/login specific.", "low", 95),
    SupplierAdapter("Thermo Fisher", ("thermofisher.com", "alfa.com"), ("https://www.thermofisher.com/search/results?keyword={cas}",), "Thermo/Alfa/Acros search pages often require JS and account-aware pricing.", "low", 90),
    SupplierAdapter("Sigma-Aldrich", ("sigmaaldrich.com", "milliporesigma.com"), ("https://www.sigmaaldrich.com/US/en/search/{cas}",), "Strong catalog coverage; prices can be country/account/session dependent.", "low", 90),
    SupplierAdapter("TCI Chemicals", ("tcichemicals.com",), ("https://www.tcichemicals.com/US/en/search?text={cas}",), "Good reagent catalog; some public pack/price data may be accessible depending on region.", "medium", 85),
    SupplierAdapter("MedChemExpress", ("medchemexpress.com",), ("https://www.medchemexpress.com/search.html?q={cas}",), "Life-science catalog with public price tables for many small molecules.", "high", 85),
    SupplierAdapter("MolPort", ("molport.com",), ("https://www.molport.com/shop/find-chemicals-by-cas-number/{cas}",), "Marketplace-style supplier aggregation; often useful for pack/price discovery.", "high", 82),
    SupplierAdapter("eMolecules", ("emolecules.com",), ("https://search.emolecules.com/search/#?query={cas}",), "Marketplace/catalog; often JS-heavy and may need API/partner access for robust pricing.", "medium", 80),
    SupplierAdapter("Chem-Impex", ("chemimpex.com",), ("https://www.chemimpex.com/search?search={cas}",), "Specialty catalog; often quote/public mixed.", "medium", 78),
    SupplierAdapter("Combi-Blocks", ("combi-blocks.com",), ("https://www.combi-blocks.com/cgi-bin/find.cgi?search={cas}",), "Building-block catalog; public search can reveal product links, pricing may be gated.", "medium", 78),
    SupplierAdapter("Oakwood Chemical", ("oakwoodchemical.com",), ("https://oakwoodchemical.com/Search?term={cas}",), "Specialty chemical catalog; public price availability varies.", "medium", 75),
    SupplierAdapter("Ambeed", ("ambeed.com",), ("https://www.ambeed.com/search.html?search={cas}",), "Building-block/intermediate catalog; often public product pages.", "medium", 75),
    SupplierAdapter("BLD Pharm", ("bldpharm.com",), ("https://www.bldpharm.com/search?search={cas}",), "Chemical building-block catalog; often relevant for organic electronics intermediates.", "medium", 72),
    SupplierAdapter("A2B Chem", ("a2bchem.com",), ("https://www.a2bchem.com/search.aspx?search={cas}",), "Building-block catalog; useful supplier candidate source.", "medium", 70),
    SupplierAdapter("Enamine", ("enaminestore.com", "enamine.net"), ("https://enaminestore.com/catalogsearch/result/?q={cas}",), "Screening/building-block catalog; quote/API may be needed for reliable pricing.", "medium", 70),
    SupplierAdapter("VWR / Avantor", ("vwr.com", "avantorsciences.com"), ("https://us.vwr.com/store/search?keyword={cas}",), "Distributor catalog; pricing often account-specific.", "low", 68),
    SupplierAdapter("SelleckChem", ("selleckchem.com",), ("https://www.selleckchem.com/search.html?searchDTO.searchParam={cas}",), "Bioactive compound catalog with public price tables for many products.", "high", 66),
    SupplierAdapter("Cayman Chemical", ("caymanchem.com",), ("https://www.caymanchem.com/search?q={cas}",), "Bioactive/lipid/biochemical catalog; pricing often visible.", "high", 65),
    SupplierAdapter("TargetMol", ("targetmol.com",), ("https://www.targetmol.com/search?keyword={cas}",), "Bioactive catalog; often public price tiers.", "high", 64),
    SupplierAdapter("ChemBlink", ("chemblink.com",), ("https://www.chemblink.com/search.aspx?search={cas}",), "Supplier directory; useful for discovery, pricing usually not source-of-truth.", "directory", 55),
    SupplierAdapter("ChemicalBook", ("chemicalbook.com",), ("https://www.chemicalbook.com/Search_EN.aspx?keyword={cas}",), "Supplier directory; useful for broad supplier discovery and RFQ shortlist.", "directory", 55),
    SupplierAdapter("ChemExper", ("chemexper.com",), ("https://www.chemexper.com/search/cas/{cas}.html",), "Chemical/supplier directory; useful for supplier discovery, not reliable for public pricing.", "directory", 50),
    SupplierAdapter("LookChem", ("lookchem.com",), ("https://www.lookchem.com/cas-{cas}.html",), "Supplier directory; useful for supplier leads/RFQ pipeline.", "directory", 45),
)


def adapter_for_url(url: str) -> SupplierAdapter | None:
    for adapter in ADAPTERS:
        if adapter.matches(url):
            return adapter
    return None


def supplier_name_for_url(url: str, fallback: str = "Unknown supplier") -> str:
    adapter = adapter_for_url(url)
    if adapter:
        return adapter.name
    host = urlparse(url).netloc.lower().replace("www.", "")
    return host.split(".")[0].replace("-", " ").title() if host else fallback


def direct_search_results(cas: str):
    from services.search_service import SearchResult
    out = []
    for adapter in sorted(ADAPTERS, key=lambda a: a.search_priority, reverse=True):
        for template in adapter.search_url_templates:
            out.append(SearchResult(
                title=f"{adapter.name} CAS search",
                url=template.format(cas=cas),
                snippet=f"Adapter seed: {adapter.notes} Public-price likelihood: {adapter.public_price_likelihood}.",
                source="adapter_seed_link_v7",
                supplier_hint=adapter.name,
            ))
    return out


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    query_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() not in {"keyword", "search", "q", "text", "utm_source", "utm_medium", "utm_campaign"}]
    return urlunparse((parsed.scheme, parsed.netloc.lower().replace("www.", ""), parsed.path.rstrip("/"), "", urlencode(query_pairs), ""))


def extract_catalog_number(*texts: str) -> str | None:
    hay = " | ".join(t for t in texts if t)
    for pattern in CATALOG_PATTERNS:
        m = pattern.search(hay)
        if m:
            token = m.group(1).strip().strip(".,;:)")
            if len(token) >= 4 and not re.fullmatch(r"\d{2,7}-\d{2}-\d", token):
                return token[:60]
    return None


def extract_snippet_price(snippet: str) -> float | None:
    m = PRICE_RE.search(snippet or "")
    if not m:
        return None
    try:
        return float((m.group(1) or m.group(2)).replace(",", ""))
    except Exception:
        return None


def classify_price_visibility(listed_price: float | None, text: str = "", snippet_price: float | None = None, extraction_status: str = "success") -> str:
    hay = text or ""
    if extraction_status.startswith("failed"):
        return PRICE_FAILED
    if listed_price is not None:
        return PRICE_PUBLIC
    if snippet_price is not None:
        return PRICE_SNIPPET
    if LOGIN_RE.search(hay):
        return PRICE_LOGIN
    if QUOTE_RE.search(hay):
        return PRICE_QUOTE
    return PRICE_HIDDEN


def best_action_for_status(price_visibility_status: str) -> str:
    if price_visibility_status == PRICE_PUBLIC:
        return "Use as catalog price evidence"
    if price_visibility_status == PRICE_SNIPPET:
        return "Open source and verify snippet price"
    if price_visibility_status == PRICE_LOGIN:
        return "Login/check account price or RFQ"
    if price_visibility_status == PRICE_QUOTE:
        return "Send RFQ"
    if price_visibility_status == PRICE_FAILED:
        return "Open source manually"
    return "Check source / RFQ"
