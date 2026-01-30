from fastapi import UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import tempfile
import os
import zipfile

@app.post("/reconcile")
async def reconcile(
    invoice_file: UploadFile = File(...),
    payment_file: UploadFile = File(...)
):
    with tempfile.TemporaryDirectory() as tmpdir:
        invoice_path = os.path.join(tmpdir, "gem_invoices.xlsx")
        payment_path = os.path.join(tmpdir, "payments.xlsx")

        with open(invoice_path, "wb") as f:
            f.write(await invoice_file.read())

        with open(payment_path, "wb") as f:
            f.write(await payment_file.read())

        # READ FILES
        invoice_df = pd.read_excel(invoice_path)
        payment_df = pd.read_excel(payment_path)

        # ---- YOUR RECONCILIATION LOGIC HERE ----
        matched_df = invoice_df.head(10)        # example
        unmatched_df = invoice_df.tail(10)      # example

        zip_path = os.path.join(tmpdir, "reconciliation_result.zip")

        with zipfile.ZipFile(zip_path, "w") as zipf:
            if not matched_df.empty:
                matched_file = os.path.join(tmpdir, "matched_invoices.xlsx")
                matched_df.to_excel(matched_file, index=False)
                zipf.write(matched_file, "matched_invoices.xlsx")

            if not unmatched_df.empty:
                unmatched_file = os.path.join(tmpdir, "unmatched_invoices.xlsx")
                unmatched_df.to_excel(unmatched_file, index=False)
                zipf.write(unmatched_file, "unmatched_invoices.xlsx")

        return FileResponse(
            zip_path,
            media_type="application/zip",
            filename="gem_reconciliation_result.zip"
        )
