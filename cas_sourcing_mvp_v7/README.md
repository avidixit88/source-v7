# CAS Sourcing & Procurement Intelligence MVP v7

This build preserves v1-v6 and pivots the live layer from generic scraping toward a supplier-adapter architecture.

## What changed in v7

- Keeps stable mock mode intact.
- Keeps live supplier discovery, CAS-confirmed extraction, and source evidence links.
- Adds a maintained supplier adapter registry for major catalog/specialty sources:
  - Fisher Scientific, Thermo Fisher/Alfa, Sigma-Aldrich, TCI, MedChemExpress, MolPort, eMolecules, Chem-Impex, Combi-Blocks, Oakwood, Ambeed, BLD Pharm, A2B Chem, Enamine, VWR/Avantor, SelleckChem, Cayman, TargetMol, ChemBlink, ChemicalBook, ChemExper, LookChem.
- Adds pricing-reality labels:
  - Public price extracted
  - Search-snippet price only
  - Login/account price required
  - Quote required
  - No public price detected
  - Extraction failed
- Adds supplier-level grouped cards so one supplier appears once, with merged products, catalog numbers, pack options, pricing status, and best action.
- Preserves product-level extraction evidence in an expander and export.
- Adds stronger deduplication using canonical URL + supplier + catalog/pack/purity/status.

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Optional live discovery

Live discovery can run from direct supplier adapter links only. For broader search, add SerpAPI in Streamlit secrets:

```toml
SERPAPI_KEY = "your_key_here"
```

## Important procurement rule

Visible catalog prices are evidence. Bulk prices are estimates. RFQ pricing is confirmed truth.

For many large distributors, no visible price is a valid output because price is often hidden behind account login, regional catalog session, or quote gate.
