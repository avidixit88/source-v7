from __future__ import annotations

import pandas as pd


def rank_supplier_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["score"] = 0
    out.loc[out["cas_number"].notna(), "score"] += 30
    out.loc[out.get("has_visible_price", False) == True, "score"] += 25
    out.loc[out["purity"].astype(str).str.contains("99|98|95", regex=True, na=False), "score"] += 15
    out.loc[out["stock_status"].astype(str).str.contains("visible|stock|available", case=False, na=False), "score"] += 10
    out.loc[out["region"].astype(str).str.contains("US", case=False, na=False), "score"] += 10
    out.loc[out["product_url"].notna(), "score"] += 10

    out["ranking_reason"] = out.apply(_reason, axis=1)
    return out.sort_values(["score", "has_visible_price"], ascending=[False, False])


def _reason(row: pd.Series) -> str:
    reasons = []
    if row.get("has_visible_price"):
        reasons.append("visible price")
    else:
        reasons.append("quote/check required")
    if "US" in str(row.get("region", "")):
        reasons.append("US-accessible")
    if any(x in str(row.get("purity", "")) for x in ["99", "98", "95"]):
        reasons.append("purity listed")
    return ", ".join(reasons)
