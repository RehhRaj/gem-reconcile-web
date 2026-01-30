import pandas as pd
from itertools import combinations

MAX_COMBINATION_SIZE = 3

def normalize_columns(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace("\n", " ")
        .str.upper()
        .str.replace(" ", "_")
        .str.replace(".", "", regex=False)
    )
    return df

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

def reconcile(invoice_file, payment_file):
    # ----- Load invoice -----
    invoice_df = normalize_columns(pd.read_excel(invoice_file))

    # ----- Load payment (fix header row) -----
    payment_raw = pd.read_excel(payment_file, header=None)
    payment_raw.columns = payment_raw.iloc[0]
    payment_df = normalize_columns(payment_raw.iloc[1:].copy())

    # ----- Prepare invoice -----
    invoice_df["INVOICE_DATE"] = safe_to_date(invoice_df["INVOICE_DATE"])
    invoice_df["PRC_DATE"] = safe_to_date(invoice_df["PRC_DATE"])
    invoice_df["ELIGIBLE_DATE"] = invoice_df[["INVOICE_DATE", "PRC_DATE"]].max(axis=1)
    invoice_df["CRAC_AMOUNT"] = safe_to_amount(invoice_df["CRAC_AMOUNT"])

    # ----- Prepare payment -----
    payment_df["PAO_PASS_DATE"] = safe_to_date(payment_df["PAO_PASS_DATE"])
    payment_df["BILL_AMOUNT"] = safe_to_amount(payment_df["BILLAMOUNT"])
    payment_df["BILLNO"] = payment_df["BILLNO"].astype(str)

    # ----- Flags -----
    invoice_df["PAID_FLAG"] = False
    invoice_df["MATCH_GROUP_ID"] = ""
    invoice_df["MATCH_TYPE"] = ""

    matched = []
    unmatched = []
    gid_counter = 1

    for _, pay in payment_df.iterrows():
        bill_amt = pay["BILL_AMOUNT"]
        pay_date = pay["PAO_PASS_DATE"]
        bill_no = pay["BILLNO"]

        eligible = invoice_df[
            (~invoice_df["PAID_FLAG"]) &
            (invoice_df["ELIGIBLE_DATE"] <= pay_date)
        ]

        exact = eligible[eligible["CRAC_AMOUNT"] == bill_amt]
        if not exact.empty:
            idx = exact.index[0]
            gid = f"MG{gid_counter:05d}"
            gid_counter += 1
            invoice_df.loc[idx, ["PAID_FLAG", "MATCH_GROUP_ID", "MATCH_TYPE"]] = \
                [True, gid, "SINGLE"]
            matched.append({"BILLNO": bill_no, "MATCH_GROUP_ID": gid})
            continue

        found = False
        candidates = eligible.to_dict("records")

        for r in range(2, MAX_COMBINATION_SIZE + 1):
            for combo in combinations(candidates, r):
                if sum(x["CRAC_AMOUNT"] for x in combo) == bill_amt:
                    gid = f"MG{gid_counter:05d}"
                    gid_counter += 1
                    for x in combo:
                        idx = invoice_df.index[
                            invoice_df["INVOICE_NUMBER"] == x["INVOICE_NUMBER"]
                        ][0]
                        invoice_df.loc[idx, ["PAID_FLAG", "MATCH_GROUP_ID", "MATCH_TYPE"]] = \
                            [True, gid, "COMBINATION"]
                    matched.append({"BILLNO": bill_no, "MATCH_GROUP_ID": gid})
                    found = True
                    break
            if found:
                break

        if not found:
            unmatched.append(bill_no)

    return invoice_df, matched, unmatched
