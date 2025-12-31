Data Validator (Rule-Based CSV Validation Tool)

A configurable Python tool for validating structured CSV datasets using external rule packs.  
The validator detects missing columns, invalid numeric ranges, and logical inconsistencies without hard-coding domain logic.

---

Overview

This project implements a reusable data validation engine designed to work with any tabular CSV dataset. Validation rules are defined externally in JSON rule packs, allowing the same codebase to validate data across multiple domains without modification.

The tool focuses on data quality checks that commonly precede analytics, machine learning, or backend processing pipelines.

---

Key Features

- Validates required columns
- Enforces numeric range constraints
- Detects non-numeric values in numeric fields
- Applies cross-field logical rules (e.g., comparisons and consistency checks)
- Generates a structured JSON report summarizing data quality issues
- Supports multiple domain rule packs without changing code

---

Project Structure

data-validator/
├── README.md
├── requirements.txt
├── validate.py
└── rulepacks/
    ├── real_estate.json
    └── inventory.json

- validate.py contains the generic validation engine
- rulepacks/ contains domain-specific validation rules defined in JSON
- requirements.txt lists project dependencies

---

Rule Packs

Rule packs define validation logic outside the codebase.

Included rule packs:
- real_estate.json — validates real estate listing data (prices, square footage, year built, etc.)
- inventory.json — example rule pack for inventory datasets (quantity, pricing, bounds)

Rule packs specify:
- Required columns
- Numeric min/max constraints
- Logical cross-field rules

Additional rule packs can be added without modifying the validator logic.

---

Running the Validator

1. Install dependencies:

   pip install -r requirements.txt

2. Run validation against a CSV:

   python validate.py \
     --input path/to/data.csv \
     --rules rulepacks/real_estate.json \
     --out out_directory

3. Review results:

   A JSON report is written to the output directory:
   out_directory/data_quality_report.json

The report includes:
- Rows checked
- Missing or invalid columns
- Count of row-level issues
- Sample validation errors for inspection

---

Notes on Inventory Rule Pack

The inventory rule pack is provided as an example of cross-domain reuse.  
An inventory CSV is not included in this repository, but the rule pack can be applied to any compatible dataset with matching column names.

---

Design Notes

- Validation logic is intentionally decoupled from domain rules
- Rule packs are loaded dynamically at runtime
- The validator can be extended with additional logical rule types
- This pattern mirrors real-world ETL and data quality workflows

---

Technologies Used

- Python
- pandas
- JSON
- argparse

---

Summary

This project demonstrates defensive data processing, configurability, and separation of concerns.  
It highlights how structured validation can be applied consistently across different datasets without rewriting application logic.