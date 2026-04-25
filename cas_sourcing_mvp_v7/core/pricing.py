from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
import math
import pandas as pd

UNIT_TO_GRAMS = {
    "mg": 0.001,
    "g": 1.0,
    "kg": 1000.0,
    "mL": None,
    "L": None,
}

Scenario = Literal["Conservative", "Base", "Aggressive"]

SCALING_EXPONENTS: dict[Scenario, float] = {
    "Conservative": 0.92,
    "Base": 0.82,
    "Aggressive": 0.72,
}

@dataclass(frozen=True)
class BulkEstimate:
    scenario: Scenario
    estimated_total_price: float
    estimated_unit_price_per_g: float
    discount_vs_anchor_pct: float
    confidence: str
    explanation: str


def quantity_to_grams(quantity: float, unit: str) -> float | None:
    multiplier = UNIT_TO_GRAMS.get(unit)
    if multiplier is None:
        return None
    return float(quantity) * multiplier


def normalize_price_points(df: pd.DataFrame) -> pd.DataFrame:
    """Add pack_size_g, price_per_g, and normalized data quality fields."""
    if df.empty:
        return df.copy()

    out = df.copy()
    out["pack_size_g"] = out.apply(
        lambda r: quantity_to_grams(r.get("pack_size", 0), str(r.get("pack_unit", "g"))),
        axis=1,
    )
    out["price_per_g"] = out.apply(
        lambda r: (float(r["listed_price_usd"]) / r["pack_size_g"])
        if pd.notna(r.get("listed_price_usd")) and r.get("pack_size_g") and r.get("pack_size_g") > 0
        else None,
        axis=1,
    )
    out["has_visible_price"] = out["price_per_g"].notna()
    return out


def choose_anchor_price(price_points: pd.DataFrame, desired_qty_g: float) -> pd.Series | None:
    """Choose the largest visible pack at or below desired qty; otherwise largest visible pack."""
    visible = price_points[price_points["has_visible_price"] & price_points["pack_size_g"].notna()].copy()
    if visible.empty:
        return None

    below = visible[visible["pack_size_g"] <= desired_qty_g]
    if not below.empty:
        return below.sort_values(["pack_size_g", "price_per_g"], ascending=[False, True]).iloc[0]
    return visible.sort_values(["pack_size_g", "price_per_g"], ascending=[False, True]).iloc[0]


def estimate_bulk_price(
    anchor_pack_g: float,
    anchor_total_price: float,
    desired_qty_g: float,
    scenario: Scenario,
    visible_price_points: int,
) -> BulkEstimate:
    """Estimate larger-order price using a quantity scaling curve.

    Formula: estimated_total = anchor_total * (desired_qty / anchor_qty) ** exponent
    where exponent < 1 implies unit-price discount as order size increases.
    """
    if anchor_pack_g <= 0 or anchor_total_price <= 0 or desired_qty_g <= 0:
        raise ValueError("anchor_pack_g, anchor_total_price, and desired_qty_g must be positive")

    exponent = SCALING_EXPONENTS[scenario]
    ratio = desired_qty_g / anchor_pack_g
    estimated_total = anchor_total_price * math.pow(ratio, exponent)
    estimated_unit = estimated_total / desired_qty_g
    anchor_unit = anchor_total_price / anchor_pack_g
    discount_pct = (1 - (estimated_unit / anchor_unit)) * 100

    if visible_price_points >= 3:
        confidence = "Medium"
        explanation = "Multiple visible pack prices exist, so the curve has some support. Confirm with RFQ before purchasing."
    elif visible_price_points == 2:
        confidence = "Low-Medium"
        explanation = "Only two visible price points exist. Treat as directional until supplier confirms bulk pricing."
    else:
        confidence = "Low"
        explanation = "Only one visible price point exists. This is a rough catalog-to-bulk estimate, not a confirmed quote."

    return BulkEstimate(
        scenario=scenario,
        estimated_total_price=round(estimated_total, 2),
        estimated_unit_price_per_g=round(estimated_unit, 4),
        discount_vs_anchor_pct=round(discount_pct, 1),
        confidence=confidence,
        explanation=explanation,
    )
