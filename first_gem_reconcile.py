import pandas as pd
from itertools import combinations
import os

# ================= CONFIG =================
INVOICE_FILE = "data/gem_invoices.xlsx"
PAYMENT_FILE = "data/payments.xlsx"
OUTPUT_DIR = "output"
MAX_COMBINATION_SIZE = 3
AMOUNT_TOLERANCE = 0.01

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ================= HELPERS =================
def normalize_columns(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace("\n", " ")
        .str.upper()
        .str.replace(" ", "_")
    )
    return df

def find_column(df, possible_names):
    for name in possible_names:
        if name in df.columns:
            return name
    raise KeyError(f"None of these columns found: {possible_names}")

def safe_to_date(series):
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def safe_to_amount(series):
    return (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace("", "0")
        .astype(float)
    )

def financial_year(date):
    if pd.isna(date):
        return None
    return date.year if date.month >= 4 else date.year - 1

# ================= LOAD =================
invoice_df = normalize_columns(pd.read_excel(INVOICE_FILE))
payment_df = normalize_columns(pd.read_excel(PAYMENT_FILE))

# ================= FIND COLUMNS =================
INVOICE_DATE_COL = find_column(invoice_df, ["INVOICE_DATE"])
PRC_DATE_COL = find_column(invoice_df, ["PRC_DATE"])
CRAC_AMOUNT_COL = find_column(invoice_df, ["CRAC_AMOUNT"])
INVOICE_NO_COL = find_column(invoice_df, ["INVOICE_NUMBER"])

PAO_DATE_COL = find_column(payment_df, ["PAO_PASS_DATE", "PAO_DATE"])
BILL_AMOUNT_COL = find_column(payment_df, ["BILLAMOUNT", "BILL_AMOUNT"])
BILL_NO_COL = find_column(payment_df, ["BILLNO", "BILL_NO"])

# ================= CLEAN DATA =================
invoice_df["INVOICE_DATE"] = safe_to_date(invoice_df[INVOICE_DATE_COL])
invoice_df["PRC_DATE"] = safe_to_date(invoice_df[PRC_DATE_COL])
invoice_df["ELIGIBLE_DATE"] = invoice_df[["INVOICE_DATE", "PRC_DATE"]].max(axis=1)
invoice_df["CRAC_AMOUNT"] = safe_to_amount(invoice_df[CRAC_AMOUNT_COL])
invoice_df["FY"] = invoice_df["ELIGIBLE_DATE"].apply(financial_year)

payment_df["PAO_PASS_DATE"] = safe_to_date(payment_df[PAO_DATE_COL])
payment_df["BILL_AMOUNT"] = safe_to_amount(payment_df[BILL_AMOUNT_COL])
payment_df["FY"] = payment_df["PAO_PASS_DATE"].apply(financial_year)

# ================= FLAGS =================
invoice_df["PAID_FLAG"] = False
invoice_df["MATCH_GROUP_ID"] = ""
invoice_df["MATCH_TYPE"] = ""
invoice_df["CONFIDENCE"] = ""
invoice_df["PAO_PASS_DATE"] = pd.NaT
invoice_df["BILLNO"] = ""
invoice_df["REJECTION_REASON"] = ""

matched_summary = []
unmatched_payments = []
group_counter = 1

# ================= MATCHING =================
for _, pay in payment_df.iterrows():
    bill_amt = pay["BILL_AMOUNT"]
    pay_date = pay["PAO_PASS_DATE"]
    bill_no = pay[BILL_NO_COL]
    pay_fy = pay["FY"]

    if pd.isna(bill_amt) or pd.isna(pay_date):
        unmatched_payments.append(
            {"BILLNO": bill_no, "REASON": "INVALID_PAYMENT_DATA"}
        )
        continue

    eligible = invoice_df[
        (~invoice_df["PAID_FLAG"]) &
        (invoice_df["ELIGIBLE_DATE"] <= pay_date) &
        (invoice_df["FY"] == pay_fy)
    ]

    # ---- Exact match (tolerance-based) ----
    exact = eligible[
        (eligible["CRAC_AMOUNT"] - bill_amt).abs() < AMOUNT_TOLERANCE
    ]

    if not exact.empty:
        idx = exact.index[0]
        gid = f"MG{group_counter:05d}"
        group_counter += 1

        invoice_df.loc[idx, [
            "PAID_FLAG",
            "MATCH_GROUP_ID",
            "MATCH_TYPE",
            "CONFIDENCE",
            "PAO_PASS_DATE",
            "BILLNO"
        ]] = [
            True,
            gid,
            "AUTO_SINGLE",
            "HIGH",
            pay_date,
            bill_no
        ]

        matched_summary.append({"MATCH_GROUP_ID": gid, "BILLNO": bill_no})
        continue

    # ---- Combination match ----
    found = False
    candidates = eligible.to_dict("records")

    for r in range(2, MAX_COMBINATION_SIZE + 1):
        for combo in combinations(candidates, r):
            if abs(sum(x["CRAC_AMOUNT"] for x in combo) - bill_amt) < AMOUNT_TOLERANCE:
                gid = f"MG{group_counter:05d}"
                group_counter += 1

                for x in combo:
                    idx = invoice_df.index[
                        invoice_df[INVOICE_NO_COL] == x[INVOICE_NO_COL]
                    ][0]

                    invoice_df.loc[idx, [
                        "PAID_FLAG",
                        "MATCH_GROUP_ID",
                        "MATCH_TYPE",
                        "CONFIDENCE",
                        "PAO_PASS_DATE",
                        "BILLNO"
                    ]] = [
                        True,
                        gid,
                        "AUTO_COMBINATION",
                        "MEDIUM",
                        pay_date,
                        bill_no
                    ]

                matched_summary.append({"MATCH_GROUP_ID": gid, "BILLNO": bill_no})
                found = True
                break
        if found:
            break

    if not found:
        reason = "NO_MATCH_IN_SAME_FINANCIAL_YEAR"
        unmatched_payments.append(
            {"BILLNO": bill_no, "REASON": reason}
        )

# ================= OUTPUT =================
invoice_df[invoice_df["PAID_FLAG"]] \
    .sort_values("MATCH_GROUP_ID") \
    .to_excel(f"{OUTPUT_DIR}/matched_invoices.xlsx", index=False)

invoice_df[~invoice_df["PAID_FLAG"]] \
    .to_excel(f"{OUTPUT_DIR}/unpaid_invoices.xlsx", index=False)

pd.DataFrame(unmatched_payments) \
    .to_excel(f"{OUTPUT_DIR}/unmatched_payments.xlsx", index=False)

pd.DataFrame(matched_summary) \
    .to_excel(f"{OUTPUT_DIR}/payment_invoice_map.xlsx", index=False)

print("✅ Reconciliation complete")
print(f"✔ Matched groups      : {group_counter - 1}")
print(f"⚠ Unmatched payments : {len(unmatched_payments)}")
