from __future__ import annotations

from urllib.parse import urlparse
import pandas as pd

from services.search_service import (
    build_cas_supplier_queries,
    direct_supplier_search_urls,
    filter_likely_supplier_results,
    serpapi_search,
    discover_product_links_from_page,
)
from services.page_extractor import extract_product_data_from_url
from services.supplier_adapters import canonicalize_url, extract_snippet_price, classify_price_visibility, best_action_for_status


def _dedupe_results(results):
    seen = set()
    unique = []
    for result in results:
        key = canonicalize_url(result.url)
        if key in seen:
            continue
        seen.add(key)
        unique.append(result)
    return unique


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def _clean_pack(row: pd.Series) -> str:
    size = row.get("pack_size")
    unit = row.get("pack_unit")
    if pd.isna(size) or not unit or pd.isna(unit):
        return ""
    try:
        return f"{float(size):g} {unit}"
    except Exception:
        return f"{size} {unit}"


def _collapse_price_status(statuses: list[str]) -> str:
    priority = [
        "Public price extracted",
        "Search-snippet price only",
        "Login/account price required",
        "Quote required",
        "No public price detected",
        "Extraction failed",
    ]
    for status in priority:
        if status in statuses:
            return status
    return statuses[0] if statuses else "No public price detected"


def summarize_supplier_rows(detail_df: pd.DataFrame) -> pd.DataFrame:
    """One supplier card per supplier/CAS with merged pack/product/source intelligence.

    v7 deliberately separates product detail rows from supplier-level decision rows so the UI does
    not show ten duplicate Fisher/Sigma rows when a supplier has many catalog variants.
    """
    if detail_df.empty:
        return detail_df.copy()
    df = detail_df.copy()
    if "canonical_url" not in df.columns:
        df["canonical_url"] = df["product_url"].apply(canonicalize_url)
    df["pack_label"] = df.apply(_clean_pack, axis=1)

    records = []
    for supplier, g in df.groupby("supplier", dropna=False):
        visible = g[g.get("listed_price_usd").notna()] if "listed_price_usd" in g.columns else pd.DataFrame()
        statuses = [str(x) for x in g.get("price_visibility_status", pd.Series(dtype=str)).dropna().tolist()]
        status = _collapse_price_status(statuses)
        pack_options = sorted({x for x in g["pack_label"].tolist() if x})
        purities = sorted({str(x) for x in g.get("purity", pd.Series(dtype=str)).dropna().tolist() if str(x) and str(x) != "Not visible"})
        urls = list(dict.fromkeys(g["product_url"].dropna().astype(str).tolist()))[:5]
        cat_nums = sorted({str(x) for x in g.get("catalog_number", pd.Series(dtype=str)).dropna().tolist() if str(x) and str(x) != "nan"})
        row = {
            "supplier": supplier,
            "cas_number": g["cas_number"].iloc[0],
            "cas_exact_match": bool(g.get("cas_exact_match", pd.Series([False])).fillna(False).astype(bool).any()),
            "products_found": int(g["canonical_url"].nunique()),
            "catalog_numbers": ", ".join(cat_nums[:8]) if cat_nums else "Not extracted",
            "purities_found": ", ".join(purities[:8]) if purities else "Not visible",
            "pack_options": ", ".join(pack_options[:12]) if pack_options else "Not visible",
            "visible_price_count": int(len(visible)),
            "best_visible_price_usd": float(visible["listed_price_usd"].min()) if not visible.empty else None,
            "price_visibility_status": status,
            "best_action": best_action_for_status(status),
            "stock_summary": "; ".join(list(dict.fromkeys(g.get("stock_status", pd.Series(dtype=str)).dropna().astype(str).tolist()))[:5]) or "Not visible",
            "max_extraction_confidence": int(g.get("extraction_confidence", pd.Series([0])).fillna(0).max()),
            "source_urls": " | ".join(urls),
            "representative_url": urls[0] if urls else "",
            "notes": "Supplier-level grouped row. Expand product details below for evidence.",
            "data_source": "live_supplier_adapter_summary_v7",
        }
        records.append(row)
    out = pd.DataFrame(records)
    # Stable decision ordering: confirmed CAS, public price, confidence, product coverage.
    if not out.empty:
        out["_has_public_price"] = out["visible_price_count"] > 0
        out = out.sort_values(["cas_exact_match", "_has_public_price", "max_extraction_confidence", "products_found"], ascending=[False, False, False, False])
        out = out.drop(columns=["_has_public_price"])
    return out


def discover_live_suppliers(
    cas_number: str,
    chemical_name: str | None = None,
    serpapi_key: str | None = None,
    max_pages_to_extract: int = 12,
    include_direct_links: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Discover supplier pages and extract visible product/pricing fields.

    v7 behavior:
    - Preserves v1-v6 baseline.
    - Adds supplier-source adapter registry and better source targeting.
    - Separates product detail rows from supplier summary rows to remove duplicate supplier clutter.
    - Labels pricing reality: public price, snippet price, login/account price, quote required, hidden, failed.
    """
    queries = build_cas_supplier_queries(cas_number, chemical_name)
    serp_results = filter_likely_supplier_results(serpapi_search(queries, serpapi_key or ""))
    direct_results = direct_supplier_search_urls(cas_number) if include_direct_links else []

    seed_results = _dedupe_results(serp_results + direct_results)

    expanded = []
    for result in seed_results[:35]:
        expanded.extend(discover_product_links_from_page(result, cas_number, max_links=6))
    expanded = _dedupe_results(expanded)

    # Product-detail candidates first. Direct/search pages are still included as fallback because some suppliers render products on search pages.
    candidate_results = _dedupe_results(expanded + serp_results + direct_results)

    discovery_records = []
    for r in _dedupe_results(expanded + seed_results):
        rec = r.__dict__.copy()
        rec["canonical_url"] = canonicalize_url(r.url)
        rec["domain"] = _domain(r.url)
        rec["snippet_price_usd"] = extract_snippet_price(r.snippet)
        discovery_records.append(rec)
    discovery_df = pd.DataFrame(discovery_records)

    extracted_rows = []
    for result in candidate_results[:max_pages_to_extract]:
        extracted = extract_product_data_from_url(
            cas_number,
            result.url,
            supplier_hint=result.supplier_hint or None,
            discovery_title=result.title,
            discovery_snippet=result.snippet,
        )
        snippet_price = extract_snippet_price(result.snippet)
        price_visibility_status = extracted.price_visibility_status
        if extracted.listed_price_usd is None and snippet_price is not None and extracted.cas_exact_match:
            price_visibility_status = classify_price_visibility(None, result.snippet, snippet_price, extracted.extraction_status)

        keep = (
            extracted.cas_exact_match
            or extracted.listed_price_usd is not None
            or extracted.stock_status not in ["Not visible", "Extraction failed"]
            or price_visibility_status in ["Search-snippet price only", "Login/account price required", "Quote required"]
            or result.source.startswith("serpapi")
        )
        if not keep:
            continue
        extracted_rows.append({
            "cas_number": cas_number,
            "chemical_name": chemical_name or "",
            "supplier": extracted.supplier,
            "region": "Unknown",
            "purity": extracted.purity or "Not visible",
            "pack_size": extracted.pack_size,
            "pack_unit": extracted.pack_unit,
            "listed_price_usd": extracted.listed_price_usd,
            "snippet_price_usd": snippet_price,
            "price_visibility_status": price_visibility_status,
            "best_action": best_action_for_status(price_visibility_status),
            "stock_status": extracted.stock_status,
            "lead_time": "Not visible",
            "product_url": extracted.product_url,
            "canonical_url": canonicalize_url(extracted.product_url),
            "domain": _domain(extracted.product_url),
            "catalog_number": extracted.catalog_number,
            "notes": extracted.evidence,
            "page_title": extracted.title,
            "cas_exact_match": extracted.cas_exact_match,
            "extraction_status": extracted.extraction_status,
            "extraction_confidence": extracted.confidence,
            "extraction_method": extracted.extraction_method,
            "raw_matches": extracted.raw_matches,
            "data_source": "live_extraction_v7_supplier_adapters",
        })

    detail_df = pd.DataFrame(extracted_rows)
    if not detail_df.empty:
        # Strong product-row dedupe: same supplier + canonical product page + pack/purity/catalog is one evidence row.
        dedupe_cols = [c for c in ["supplier", "canonical_url", "catalog_number", "purity", "pack_size", "pack_unit", "price_visibility_status"] if c in detail_df.columns]
        detail_df = detail_df.drop_duplicates(subset=dedupe_cols, keep="first")
        detail_df = detail_df.sort_values(["cas_exact_match", "listed_price_usd", "extraction_confidence"], ascending=[False, False, False])

    summary_df = summarize_supplier_rows(detail_df)
    return detail_df, discovery_df, summary_df
