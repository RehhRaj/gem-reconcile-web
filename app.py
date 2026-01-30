# app.py
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import tempfile
import os
import zipfile

from reconcile_core import reconcile

app = FastAPI(title="GeM Payment Reconciliation")


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
