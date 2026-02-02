import pandas as pd
from itertools import combinations
import os

# ================= CONFIG =================
INVOICE_FILE = "data/gem_reports_bulk_payment.xlsx"      # can be .csv or .xlsx
PAYMENT_FILE = "data/ContingencyBillsPassedbyPAO.xlsx"   # can be .csv or .xlsx
OUTPUT_DIR = "output"

MAX_COMBINATION_SIZE = 4
AMOUNT_TOLERANCE = 0.01

os.makedirs(OUTPUT_DIR, exist_ok=True)    # no error if dir exist   xist_ok=True

# ================= HELPERS =================
def read_file(path):
    if path.lower().endswith(".csv"):
        return pd.read_csv(path, encoding="utf-8-sig")
    elif path.lower().endswith((".xls", ".xlsx")):
        return pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file format: {path}")

# "cleanup crew" for pandas DataFrame column names.
def normalize_columns(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)  #"one or more whitespace characters" and collapses them into a single space
        .str.replace("\n", " ")
        .str.upper()
    )
    return df


def find_any(df, possible_names):
    for name in possible_names:
        if name in df.columns:
            print(f"✔ Using column: {name}")
            return name
    print("Available columns:", list(df.columns))
    raise KeyError(f"None of these columns found: {possible_names}")

def safe_to_date(series):
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def safe_to_amount(series):
    # ---- NEW: handle case where user accidentally passes a string ----
    if isinstance(series, str):
        series = pd.Series([series])

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

def is_blacklisted_bill(bill_no):
    if pd.isna(bill_no):
        return False
    bill_no = str(bill_no).upper().strip()
    return bill_no.startswith("ACB") or bill_no.startswith("DCB")


# ================= LOAD FILES =================
invoice_df = normalize_columns(read_file(INVOICE_FILE))
payment_df = normalize_columns(read_file(PAYMENT_FILE))

# ================= MAP REQUIRED COLUMNS =================
# ---- Invoice (GEM Bulk Payment) ----
PRC_DATE_COL = find_any(invoice_df, ["PRC DATE", "PRC_DATE"])
CRAC_AMOUNT_COL = find_any(invoice_df, ["CRAC AMOUNT", "CRAC_AMOUNT"])
PAID_AMOUNT_COL = find_any(invoice_df, ["PAID AMOUNT", "PAID_AMOUNT"])

invoice_df["PRC_DATE"] = safe_to_date(invoice_df[PRC_DATE_COL])
invoice_df["CRAC_AMOUNT"] = safe_to_amount(invoice_df[CRAC_AMOUNT_COL])
invoice_df["PAID_AMOUNT"] = safe_to_amount(invoice_df[PAID_AMOUNT_COL])

invoice_df["FY"] = invoice_df["PRC_DATE"].apply(financial_year)

# ---- Payment (PAO Bills) ----
BILL_NO_COL = find_any(
    payment_df,
    ["BILL NO.", "BILLNO", "BILL NO", "BILLNO."]
)

BILL_AMOUNT_COL = find_any(
    payment_df,
    ["BILLAMOUNT", "BILL AMOUNT"]
)

BILL_DATE_COL = find_any(
    payment_df,
    [
        "BILLDATE",
        "BILL DATE",
        "PAO PASS DATE",
        "PAO_PASS_DATE",
        "PAO PASSING DATE",
        "DDO APPROVAL DATE"
    ]
)

HEAD_OF_ACCOUNT_COL = find_any(
    payment_df,
    ["HEAD OF ACCCOUNT", "HEAD OF ACCOUNT"]
)

payment_df["BILLNO"] = payment_df[BILL_NO_COL].astype(str).str.strip()
payment_df["BILL_AMOUNT"] = safe_to_amount(payment_df[BILL_AMOUNT_COL])
payment_df["BILL_DATE"] = safe_to_date(payment_df[BILL_DATE_COL])
payment_df["HEAD_OF_ACCOUNT"] = payment_df[HEAD_OF_ACCOUNT_COL]

payment_df["FY"] = payment_df["BILL_DATE"].apply(financial_year)

# ================= INITIAL FLAGS =================
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

# ================= MATCHING ENGINE =================
for _, pay in payment_df.iterrows():

    bill_no = pay["BILLNO"]
    bill_amt = pay["BILL_AMOUNT"]
    pay_date = pay["BILL_DATE"]
    pay_fy = pay["FY"]
    head_of_account = pay["HEAD_OF_ACCOUNT"]

    # ---- Ignore ACB / DCB bills ----
    if is_blacklisted_bill(bill_no):
        unmatched_payments.append({
            "BILLNO": bill_no,
            "REASON": "IGNORED_ACB_DCB_BILL",
            "HEAD_OF_ACCOUNT": head_of_account
        })
        continue

    if pd.isna(bill_amt) or pd.isna(pay_date):
        unmatched_payments.append({
            "BILLNO": bill_no,
            "REASON": "INVALID_PAYMENT_DATA",
            "HEAD_OF_ACCOUNT": head_of_account
        })
        continue

    # Same Financial Year only
    eligible = invoice_df[
        (~invoice_df["PAID_FLAG"]) &
        (invoice_df["FY"] == pay_fy)
    ]

    # ========== PRIORITY 1: EXACT MATCH ==========
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

        matched_summary.append({
            "MATCH_GROUP_ID": gid,
            "BILLNO": bill_no,
            "MATCH_MODE": "EXACT",
            "HEAD_OF_ACCOUNT": head_of_account
        })
        continue

    # ========== PRIORITY 2: COMBINATION MATCH ==========
    found = False
    candidates = eligible.to_dict("records")

    for r in range(2, MAX_COMBINATION_SIZE + 1):
        for combo in combinations(candidates, r):
            total = sum(x["CRAC_AMOUNT"] for x in combo)

            if abs(total - bill_amt) < AMOUNT_TOLERANCE:
                gid = f"MG{group_counter:05d}"
                group_counter += 1

                for x in combo:
                    idx = invoice_df.index[
                        (invoice_df["CRAC_AMOUNT"] == x["CRAC_AMOUNT"]) &
                        (invoice_df["PRC_DATE"] == x["PRC_DATE"])
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

                matched_summary.append({
                    "MATCH_GROUP_ID": gid,
                    "BILLNO": bill_no,
                    "MATCH_MODE": "COMBINATION",
                    "HEAD_OF_ACCOUNT": head_of_account
                })

                found = True
                break
        if found:
            break

    if not found:
        unmatched_payments.append({
            "BILLNO": bill_no,
            "REASON": "NO_MATCH_IN_SAME_FINANCIAL_YEAR",
            "HEAD_OF_ACCOUNT": head_of_account
        })

# ================= OUTPUT FILES =================
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
