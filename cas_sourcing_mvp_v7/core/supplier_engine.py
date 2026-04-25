from __future__ import annotations

import pandas as pd

MOCK_SUPPLIER_DATA = [
    {
        "cas_number": "64-17-5",
        "chemical_name": "Ethanol",
        "supplier": "Sigma-Aldrich",
        "region": "US/EU",
        "purity": "99.5%",
        "pack_size": 500,
        "pack_unit": "mL",
        "listed_price_usd": None,
        "stock_status": "Quote / catalog dependent",
        "lead_time": "Varies",
        "product_url": "https://www.sigmaaldrich.com/",
        "notes": "Liquid unit pricing needs density conversion before $/g normalization.",
    },
    {
        "cas_number": "64-17-5",
        "chemical_name": "Ethanol",
        "supplier": "Fisher Scientific",
        "region": "US",
        "purity": "200 proof",
        "pack_size": 1,
        "pack_unit": "L",
        "listed_price_usd": None,
        "stock_status": "Quote / catalog dependent",
        "lead_time": "Varies",
        "product_url": "https://www.fishersci.com/",
        "notes": "Listed price often requires account/login.",
    },
    {
        "cas_number": "103-90-2",
        "chemical_name": "Acetaminophen",
        "supplier": "TCI Chemicals",
        "region": "US/JP/EU",
        "purity": ">98%",
        "pack_size": 25,
        "pack_unit": "g",
        "listed_price_usd": 35.00,
        "stock_status": "Visible catalog example",
        "lead_time": "Varies",
        "product_url": "https://www.tcichemicals.com/",
        "notes": "Mock visible price for testing workflow only.",
    },
    {
        "cas_number": "103-90-2",
        "chemical_name": "Acetaminophen",
        "supplier": "TCI Chemicals",
        "region": "US/JP/EU",
        "purity": ">98%",
        "pack_size": 100,
        "pack_unit": "g",
        "listed_price_usd": 95.00,
        "stock_status": "Visible catalog example",
        "lead_time": "Varies",
        "product_url": "https://www.tcichemicals.com/",
        "notes": "Mock visible price for testing workflow only.",
    },
    {
        "cas_number": "103-90-2",
        "chemical_name": "Acetaminophen",
        "supplier": "Combi-Blocks",
        "region": "US",
        "purity": "95%+",
        "pack_size": 10,
        "pack_unit": "g",
        "listed_price_usd": 55.00,
        "stock_status": "Visible catalog example",
        "lead_time": "Varies",
        "product_url": "https://www.combi-blocks.com/",
        "notes": "Mock visible price for testing workflow only.",
    },
    {
        "cas_number": "50-00-0",
        "chemical_name": "Formaldehyde",
        "supplier": "Thermo Fisher / Acros",
        "region": "US/EU",
        "purity": "37% solution",
        "pack_size": 1,
        "pack_unit": "L",
        "listed_price_usd": None,
        "stock_status": "Quote / catalog dependent",
        "lead_time": "Varies",
        "product_url": "https://www.thermofisher.com/",
        "notes": "Solution concentration and density needed for accurate active-material pricing.",
    },
]

KNOWN_SUPPLIER_SEARCH_LINKS = [
    ("Sigma-Aldrich", "https://www.sigmaaldrich.com/US/en/search/{cas}"),
    ("Fisher Scientific", "https://www.fishersci.com/us/en/catalog/search/products?keyword={cas}"),
    ("TCI Chemicals", "https://www.tcichemicals.com/US/en/search?text={cas}"),
    ("Combi-Blocks", "https://www.combi-blocks.com/cgi-bin/find.cgi?search={cas}"),
    ("VWR / Avantor", "https://us.vwr.com/store/search?keyword={cas}"),
    ("Oakwood Chemical", "https://oakwoodchemical.com/Search?term={cas}"),
    ("Chem-Impex", "https://www.chemimpex.com/search?search={cas}"),
]


def load_mock_supplier_data() -> pd.DataFrame:
    return pd.DataFrame(MOCK_SUPPLIER_DATA)


def find_suppliers_by_cas(cas_number: str) -> pd.DataFrame:
    cas_clean = cas_number.strip()
    df = load_mock_supplier_data()
    return df[df["cas_number"].str.lower() == cas_clean.lower()].copy()


def supplier_search_links(cas_number: str) -> pd.DataFrame:
    cas = cas_number.strip()
    return pd.DataFrame(
        [{"supplier": name, "search_url": url.format(cas=cas)} for name, url in KNOWN_SUPPLIER_SEARCH_LINKS]
    )
