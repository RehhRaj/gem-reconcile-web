
gem_reconciliation/
│
├── data/
│   ├── gem_invoices.xlsx
│   └── payments.xlsx
│
├── output/
│   ├── matched_invoices.xlsx
│   ├── unpaid_invoices.xlsx
│   └── unmatched_payments.xlsx
│
├── reconcile.py        ← MAIN SCRIPT
├── rules.py            ← matching logic
├── utils.py            ← date & helper functions
└── README.md
