import pandas as pd

import app
from fastapi.responses import HTMLResponse

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
        <body style="font-family: Arial; padding:40px">
            <h2>GeM Reconciliation</h2>
            <form action="/reconcile" method="post" enctype="multipart/form-data">
                <p>Invoice Excel</p>
                <input type="file" name="invoice_file" required>

                <p>Payment Excel</p>
                <input type="file" name="payment_file" required>

                <br><br>
                <button type="submit">Reconcile & Download</button>
            </form>
        </body>
    </html>
    """


def reconcile(invoice_df: pd.DataFrame, payment_df: pd.DataFrame):
    """
    Core reconciliation logic (SAFE BASE VERSION)
    """

    # ---- NORMALIZE COLUMN NAMES ----
    invoice_df.columns = [c.strip().upper() for c in invoice_df.columns]
    payment_df.columns = [c.strip().upper() for c in payment_df.columns]

    # ---- FIND AMOUNT COLUMN ----
    amount_col = None
    for c in ["PAID AMOUNT", "CRAC AMOUNT", "INVOICE AMOUNT"]:
        if c in invoice_df.columns:
            amount_col = c
            break

    if amount_col is None:
        raise ValueError("No invoice amount column found")

    # ---- BASIC DEMO LOGIC ----
    max_payment = payment_df["BILLAMOUNT"].max()

    matched_df = invoice_df[invoice_df[amount_col] <= max_payment].copy()
    unmatched_df = invoice_df[invoice_df[amount_col] > max_payment].copy()

    matched_df["MATCH_TYPE"] = "AUTO_SIMPLE"
    unmatched_df["MATCH_TYPE"] = "UNMATCHED"

    return matched_df, unmatched_df
