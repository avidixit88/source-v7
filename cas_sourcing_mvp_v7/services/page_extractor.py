from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse
import json
import re
from typing import Any

import requests
from bs4 import BeautifulSoup
from services.supplier_adapters import (supplier_name_for_url, extract_catalog_number, extract_snippet_price, classify_price_visibility, best_action_for_status)


# Broad but conservative extraction patterns. We still store source URL/evidence so a human can verify.
PRICE_RE = re.compile(
    r"(?:USD\s*)?(?:US\$|\$)\s?([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)|"
    r"\b([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)\s?(?:USD|US\s?dollars)\b",
    re.I,
)
PACK_RE = re.compile(
    r"\b(?:pack\s*size|size|quantity|qty|amount|unit)?\s*[:\-]?\s*"
    r"([0-9]+(?:\.[0-9]+)?)\s?(mg|milligram|milligrams|g|gram|grams|kg|kilogram|kilograms|ml|mL|milliliter|milliliters|L|l|liter|liters)\b",
    re.I,
)
PURITY_RE = re.compile(
    r"(?:purity|assay|grade|concentration)?\s*[:\-]?\s*(?:>|≥|>=)?\s?([0-9]{2,3}(?:\.[0-9]+)?\s?%)",
    re.I,
)
STOCK_RE = re.compile(
    r"\b(in stock|available|ships in [^.;,]{1,45}|usually ships[^.;,]{0,45}|lead time[^.;,]{0,45}|out of stock|request quote|request a quote|ask for quotation|quote only|login to view price|sign in to view price|price on request)\b",
    re.I,
)
JS_PRICE_KEY_RE = re.compile(r'''(?i)["'](?:price|unitprice|unit_price|listprice|list_price|saleprice|sale_price|catalogprice|catalog_price|customerprice|customer_price|yourprice|your_price|amount)["']\s*[:=]\s*["']?\$?\s*([0-9]{1,5}(?:,[0-9]{3})*(?:\.[0-9]{1,4})?|[0-9]+(?:\.[0-9]{1,4})?)["']?''')
PRICE_GUARD_RE = re.compile(r"(?i)(price|unitprice|listprice|saleprice|catalogprice|customerprice|yourprice|usd|\$)")
LOGIN_PRICE_RE = re.compile(r"(?i)(sign in to view price|login to view price|log in to view price|price unavailable|request a quote|request quote|price on request)")
CAS_CONTEXT_RE = re.compile(r"\bCAS(?:\s*(?:No\.?|Number|#))?\s*[:\-]?\s*([0-9]{2,7}-[0-9]{2}-[0-9])\b", re.I)


def _normalize_unit(unit: str | None) -> str | None:
    if not unit:
        return None
    u = unit.strip().lower()
    mapping = {
        "milligram": "mg",
        "milligrams": "mg",
        "gram": "g",
        "grams": "g",
        "kilogram": "kg",
        "kilograms": "kg",
        "milliliter": "mL",
        "milliliters": "mL",
        "ml": "mL",
        "liter": "L",
        "liters": "L",
        "l": "L",
    }
    return mapping.get(u, unit)


@dataclass(frozen=True)
class ExtractedProductData:
    supplier: str
    title: str
    cas_exact_match: bool
    purity: str | None
    pack_size: float | None
    pack_unit: str | None
    listed_price_usd: float | None
    stock_status: str
    product_url: str
    extraction_status: str
    confidence: int
    evidence: str
    extraction_method: str
    raw_matches: str
    catalog_number: str | None = None
    price_visibility_status: str = "No public price detected"
    best_action: str = "Check source / RFQ"
    adapter_name: str | None = None


def supplier_name_from_url(url: str) -> str:
    return supplier_name_for_url(url)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(str(value).replace(",", "").replace("$", "").strip())
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _clean_text(html: str) -> tuple[str, str, BeautifulSoup]:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else "Untitled page"
    for tag in soup(["script", "style", "noscript", "svg"]):
        # Keep JSON-LD scripts before decomposing handled elsewhere.
        if tag.name == "script" and tag.get("type", "").lower() == "application/ld+json":
            continue
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return title, text[:180_000], soup


def _json_loads_loose(raw: str) -> list[Any]:
    try:
        parsed = json.loads(raw.strip())
        return parsed if isinstance(parsed, list) else [parsed]
    except Exception:
        return []


def _walk_json(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk_json(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_json(item)



def _extract_from_embedded_scripts(html: str, cas_number: str) -> dict[str, Any]:
    out: dict[str, Any] = {"method": None, "raw": []}
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[dict[str, Any]] = []
    for script in soup.find_all("script"):
        raw = script.string or script.get_text(" ", strip=True) or ""
        if not raw or len(raw) < 40:
            continue
        if cas_number not in raw and not PRICE_GUARD_RE.search(raw):
            continue
        raw = raw[:1_000_000]
        windows: list[str] = []
        for m in re.finditer(re.escape(cas_number), raw, flags=re.I):
            windows.append(raw[max(0, m.start() - 5000): min(len(raw), m.end() + 8000)])
        if not windows:
            for m in PRICE_GUARD_RE.finditer(raw):
                windows.append(raw[max(0, m.start() - 2500): min(len(raw), m.end() + 4500)])
                if len(windows) >= 8:
                    break
        for window in windows[:10]:
            price = None
            pm = PRICE_RE.search(window)
            if pm:
                price = _safe_float(pm.group(1) or pm.group(2))
            if price is None:
                km = JS_PRICE_KEY_RE.search(window)
                if km:
                    price = _safe_float(km.group(1))
            pack = PACK_RE.search(window)
            purity = PURITY_RE.search(window)
            stock = STOCK_RE.search(window) or LOGIN_PRICE_RE.search(window)
            pack_size = _safe_float(pack.group(1)) if pack else None
            pack_unit = _normalize_unit(pack.group(2)) if pack else None
            if pack and not _pack_is_reasonable(pack_size, pack_unit):
                pack_size, pack_unit = None, None
            score = (35 if cas_number in window else 0) + (30 if price is not None else 0) + (20 if pack_size is not None else 0) + (10 if purity else 0) + (5 if stock else 0)
            if score >= 30:
                candidates.append({
                    "method": "embedded_script",
                    "cas_exact": cas_number in window,
                    "price": price,
                    "pack_size": pack_size,
                    "pack_unit": pack_unit,
                    "purity": purity.group(1).replace(" ", "") if purity else None,
                    "stock": stock.group(1).title() if stock else None,
                    "raw": [re.sub(r"\s+", " ", window[:1200])],
                    "_score": score,
                })
    if not candidates:
        return out
    candidates.sort(key=lambda x: x.get("_score", 0), reverse=True)
    best = candidates[0]
    best.pop("_score", None)
    return best

def _extract_from_json_ld(soup: BeautifulSoup, cas_number: str) -> dict[str, Any]:
    out: dict[str, Any] = {"method": None, "raw": []}
    for script in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        for root in _json_loads_loose(script.get_text(" ", strip=True)):
            for node in _walk_json(root):
                node_text = json.dumps(node, ensure_ascii=False)[:5000]
                if cas_number not in node_text and not any(k in node for k in ["offers", "price", "sku", "name"]):
                    continue

                # Schema.org Product / Offer often stores price in offers.
                name = node.get("name") or node.get("headline")
                offers = node.get("offers")
                offer_nodes = offers if isinstance(offers, list) else [offers] if isinstance(offers, dict) else []
                price = _safe_float(node.get("price"))
                availability = node.get("availability")

                for offer in offer_nodes:
                    if not isinstance(offer, dict):
                        continue
                    price = price or _safe_float(offer.get("price") or offer.get("lowPrice") or offer.get("highPrice"))
                    availability = availability or offer.get("availability")

                if name:
                    out["title"] = name
                if price is not None:
                    out["price"] = price
                if availability:
                    out["stock"] = str(availability).split("/")[-1].replace("InStock", "In Stock")
                if cas_number in node_text:
                    out["cas_exact"] = True
                if price is not None or availability or cas_number in node_text:
                    out["method"] = "json_ld"
                    out["raw"].append(node_text[:800])
    return out


def _extract_from_meta(soup: BeautifulSoup) -> dict[str, Any]:
    out: dict[str, Any] = {"method": None, "raw": []}
    meta_map = {}
    for tag in soup.find_all("meta"):
        key = tag.get("property") or tag.get("name") or tag.get("itemprop")
        val = tag.get("content")
        if key and val:
            meta_map[key.lower()] = val

    for key in ["product:price:amount", "og:price:amount", "twitter:data1", "price", "sale_price"]:
        if key in meta_map:
            price = _safe_float(meta_map[key])
            if price is not None:
                out["price"] = price
                out["method"] = "meta_tags"
                out["raw"].append(f"{key}={meta_map[key]}")
                break
    for key in ["og:title", "twitter:title", "title"]:
        if key in meta_map:
            out["title"] = meta_map[key]
            break
    return out


def _extract_from_tables(soup: BeautifulSoup, cas_number: str) -> dict[str, Any]:
    """Parse product/variant tables where pack size and price are often in sibling cells."""
    candidates: list[dict[str, Any]] = []
    for table in soup.find_all("table"):
        table_text = table.get_text(" ", strip=True)
        if not table_text:
            continue
        # Product pages may not repeat CAS in the price table, so include all tables but score CAS tables higher.
        cas_bonus = 1 if cas_number in table_text else 0
        rows = table.find_all("tr")
        headers: list[str] = []
        if rows:
            ths = rows[0].find_all(["th", "td"])
            headers = [th.get_text(" ", strip=True).lower() for th in ths]
        for row in rows:
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
            row_text = " | ".join(cells)
            if not row_text or len(row_text) < 3:
                continue
            pack = PACK_RE.search(row_text)
            price_match = PRICE_RE.search(row_text)
            price = None
            if price_match:
                price = _safe_float(price_match.group(1) or price_match.group(2))
            # Some tables have numeric price columns without $ signs.
            if price is None and headers:
                for h, cell in zip(headers, cells):
                    if any(word in h for word in ["price", "usd", "cost"]):
                        price = _safe_float(cell)
            if pack or price is not None:
                candidates.append({
                    "pack_size": _safe_float(pack.group(1)) if pack else None,
                    "pack_unit": _normalize_unit(pack.group(2)) if pack else None,
                    "price": price,
                    "row_text": row_text[:500],
                    "score": cas_bonus + (1 if pack else 0) + (1 if price is not None else 0),
                })
    if not candidates:
        return {"method": None, "raw": []}
    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]
    best["method"] = "html_table"
    best["raw"] = [best.pop("row_text")]
    return best


def _pack_is_reasonable(size: float | None, unit: str | None) -> bool:
    if size is None or unit is None:
        return False
    u = unit.lower()
    caps = {"mg": 1_000_000, "g": 100_000, "kg": 10_000, "ml": 1_000_000, "ml": 1_000_000, "l": 10_000}
    return 0 < size <= caps.get(u, 100_000)


def _extract_from_cas_neighborhoods(text: str, cas_number: str) -> dict[str, Any]:
    best: dict[str, Any] = {"method": None, "raw": []}
    for match in re.finditer(re.escape(cas_number), text, flags=re.I):
        start = max(0, match.start() - 1200)
        end = min(len(text), match.end() + 2200)
        window = text[start:end]
        pack = PACK_RE.search(window)
        price_match = PRICE_RE.search(window)
        purity = PURITY_RE.search(window)
        stock = STOCK_RE.search(window)
        pack_size = _safe_float(pack.group(1)) if pack else None
        pack_unit = _normalize_unit(pack.group(2)) if pack else None
        if pack and not _pack_is_reasonable(pack_size, pack_unit):
            pack_size, pack_unit = None, None
        price = _safe_float(price_match.group(1) or price_match.group(2)) if price_match else None
        candidate = {
            "method": "cas_neighborhood",
            "pack_size": pack_size,
            "pack_unit": pack_unit,
            "price": price,
            "purity": purity.group(1).replace(" ", "") if purity else None,
            "stock": stock.group(1).title() if stock else None,
            "raw": [window[:1000]],
        }
        score = (30 if price is not None else 0) + (20 if pack_size is not None else 0) + (10 if purity else 0) + (5 if stock else 0)
        if score > 0 and (best["method"] is None or score > best.get("_score", 0)):
            candidate["_score"] = score
            best = candidate
    best.pop("_score", None)
    return best

def _extract_from_visible_text(text: str, cas_number: str) -> dict[str, Any]:
    windows: list[tuple[str, str]] = []
    for match in re.finditer(re.escape(cas_number), text, flags=re.I):
        start = max(0, match.start() - 1500)
        end = min(len(text), match.end() + 2500)
        windows.append(("cas_window", text[start:end]))
    for match in PACK_RE.finditer(text):
        start = max(0, match.start() - 500)
        end = min(len(text), match.end() + 1000)
        windows.append(("pack_window", text[start:end]))
    windows.append(("page_head", text[:9000]))

    best: dict[str, Any] = {"method": None, "raw": []}
    for method, window in windows:
        pack = PACK_RE.search(window)
        price_match = PRICE_RE.search(window)
        purity = PURITY_RE.search(window)
        stock = STOCK_RE.search(window)
        if not any([pack, price_match, purity, stock]):
            continue
        candidate = {
            "method": method,
            "pack_size": _safe_float(pack.group(1)) if pack else None,
            "pack_unit": _normalize_unit(pack.group(2)) if pack else None,
            "price": _safe_float(price_match.group(1) or price_match.group(2)) if price_match else None,
            "purity": purity.group(1).replace(" ", "") if purity else None,
            "stock": stock.group(1).title() if stock else None,
            "raw": [window[:800]],
        }
        # Prefer windows that found both pack and price; otherwise keep first useful signal.
        if candidate.get("price") is not None and candidate.get("pack_size") is not None:
            return candidate
        if best["method"] is None:
            best = candidate
    return best


def _first_cas_match(text: str, cas_number: str) -> bool:
    if cas_number in text:
        return True
    for match in CAS_CONTEXT_RE.finditer(text):
        if match.group(1) == cas_number:
            return True
    return False


def _merge_extractions(*parts: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {"raw": [], "methods": []}
    for part in parts:
        if not part:
            continue
        if part.get("method"):
            merged["methods"].append(part["method"])
        for key in ["title", "cas_exact", "purity", "pack_size", "pack_unit", "price", "stock"]:
            if merged.get(key) in [None, "", []] and part.get(key) not in [None, "", []]:
                merged[key] = part[key]
        merged["raw"].extend(part.get("raw", [])[:2])
    return merged


def extract_product_data_from_url(cas_number: str, url: str, timeout: int = 18, supplier_hint: str | None = None, discovery_title: str | None = None, discovery_snippet: str | None = None) -> ExtractedProductData:
    supplier = supplier_hint or supplier_name_from_url(url)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; CAS-Sourcing-MVP/6.0; human-reviewed procurement research)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }

    try:
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        title, text, soup = _clean_text(response.text)
    except Exception as exc:
        return ExtractedProductData(
            supplier=supplier,
            title="Could not extract page",
            cas_exact_match=False,
            purity=None,
            pack_size=None,
            pack_unit=None,
            listed_price_usd=None,
            stock_status="Extraction failed",
            product_url=url,
            extraction_status=f"failed: {type(exc).__name__}",
            confidence=10,
            evidence="Page could not be fetched or parsed. Use source link for manual review.",
            extraction_method="fetch_failed",
            raw_matches="",
            catalog_number=None,
            price_visibility_status="Extraction failed",
            best_action="Open source manually",
            adapter_name=supplier,
        )

    embedded = _extract_from_embedded_scripts(response.text, cas_number)
    json_ld = _extract_from_json_ld(soup, cas_number)
    meta = _extract_from_meta(soup)
    tables = _extract_from_tables(soup, cas_number)
    cas_neighborhood = _extract_from_cas_neighborhoods(text, cas_number)
    visible = _extract_from_visible_text(text, cas_number)
    merged = _merge_extractions(embedded, json_ld, meta, tables, cas_neighborhood, visible)

    discovery_context = f"{discovery_title or ''} {discovery_snippet or ''}"
    cas_exact = bool(merged.get("cas_exact")) or _first_cas_match(text, cas_number) or (cas_number in discovery_context)

    # v5 safety gate: never let random product/search-page prices leak into the result
    # unless the requested CAS is actually supported by the page or search snippet.
    purity = merged.get("purity") if cas_exact else None
    pack_size = merged.get("pack_size") if cas_exact else None
    pack_unit = merged.get("pack_unit") if cas_exact else None
    price = merged.get("price") if cas_exact else None
    stock = merged.get("stock") or "Not visible"
    extraction_methods = ", ".join(dict.fromkeys(merged.get("methods", []))) or "visible_text"

    confidence = 15
    evidence_bits = []
    if cas_exact:
        confidence += 30
        evidence_bits.append("requested CAS found on page/search evidence")
    else:
        evidence_bits.append("requested CAS not confirmed; pricing ignored for safety")
    if price is not None:
        confidence += 20
        evidence_bits.append("price extracted")
    if pack_size is not None:
        confidence += 15
        evidence_bits.append("pack size extracted")
    if purity is not None:
        confidence += 10
        evidence_bits.append("purity/assay extracted")
    if stock != "Not visible":
        confidence += 8
        evidence_bits.append("availability/quote language found")
    if "json_ld" in extraction_methods or "html_table" in extraction_methods or "meta_tags" in extraction_methods:
        confidence += 12
        evidence_bits.append(f"structured method: {extraction_methods}")

    raw_matches = "\n---\n".join(merged.get("raw", [])[:4])[:2500]
    evidence = "; ".join(evidence_bits) if evidence_bits else "No strong structured evidence found. Manual review recommended."

    snippet_price = extract_snippet_price(discovery_snippet or "") if cas_exact else None
    if price is None and snippet_price is not None:
        evidence += "; search snippet appears to contain a price; verify source manually"

    catalog_number = extract_catalog_number(url, title, str(merged.get("title") or ""), raw_matches, discovery_title or "")
    price_visibility_status = classify_price_visibility(
        listed_price=price,
        text=f"{text[:12000]} {discovery_snippet or ''} {raw_matches}",
        snippet_price=snippet_price,
        extraction_status="success",
    )
    best_action = best_action_for_status(price_visibility_status)

    return ExtractedProductData(
        supplier=supplier,
        title=str(merged.get("title") or title)[:300],
        cas_exact_match=cas_exact,
        purity=purity,
        pack_size=pack_size,
        pack_unit=pack_unit,
        listed_price_usd=price,
        stock_status=stock,
        product_url=url,
        extraction_status="success",
        confidence=min(confidence, 100),
        evidence=evidence,
        extraction_method=extraction_methods if cas_exact else f"unconfirmed_{extraction_methods}",
        raw_matches=raw_matches,
        catalog_number=catalog_number,
        price_visibility_status=price_visibility_status,
        best_action=best_action,
        adapter_name=supplier,
    )
