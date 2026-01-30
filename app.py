# app.py
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import tempfile
import os
import zipfile

from reconcile_core import reconcile

app = FastAPI(title="GeM Payment Reconciliation")

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>GeM Reconciliation</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                background: #f4f6f8;
                padding: 40px;
            }
            .box {
                background: white;
                padding: 25px;
                max-width: 500px;
                margin: auto;
                border-radius: 8px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
            }
            h2 {
                text-align: center;
            }
            input, button {
                width: 100%;
                margin-top: 10px;
                padding: 8px;
            }
            button {
                background: #2563eb;
                color: white;
                border: none;
                cursor: pointer;
            }
        </style>
    </head>
    <body>
        <div class="box">
            <h2>GeM Payment Reconciliation</h2>
            <form action="/reconcile" method="post" enctype="multipart/form-data">
                <label>GeM Invoice File</label>
                <input type="file" name="invoice_file" required>

                <label>Payment File</label>
                <input type="file" name="payment_file" required>

                <button type="submit">Reconcile & Download</button>
            </form>
            <p style="margin-top:15px; font-size: 12px; text-align:center;">
                Internal tool • No data stored
            </p>
        </div>
    </body>
    </html>
    """


@app.post("/reconcile")
async def reconcile_api(
    invoice_file: UploadFile = File(...),
    payment_file: UploadFile = File(...)
):
    with tempfile.TemporaryDirectory() as tmpdir:
        # ---- SAVE UPLOADED FILES ----
        invoice_path = os.path.join(tmpdir, "gem_invoices.xlsx")
        payment_path = os.path.join(tmpdir, "payments.xlsx")

        with open(invoice_path, "wb") as f:
            f.write(await invoice_file.read())

        with open(payment_path, "wb") as f:
            f.write(await payment_file.read())

        # ---- READ EXCEL ----
        invoice_df = pd.read_excel(invoice_path)
        payment_df = pd.read_excel(payment_path)

        # ---- RECONCILE ----
        matched_df, unmatched_df = reconcile(invoice_df, payment_df)

        # ---- WRITE RESULTS ----
        zip_path = os.path.join(tmpdir, "reconciliation_result.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:

            matched_path = os.path.join(tmpdir, "matched_invoices.xlsx")
            matched_df.to_excel(matched_path, index=False)
            zipf.write(matched_path, "matched_invoices.xlsx")

            unmatched_path = os.path.join(tmpdir, "unmatched_invoices.xlsx")
            unmatched_df.to_excel(unmatched_path, index=False)
            zipf.write(unmatched_path, "unmatched_invoices.xlsx")

        # ---- RETURN ZIP ----
        return FileResponse(
            zip_path,
            media_type="application/zip",
            filename="gem_reconciliation_result.zip"
        )
