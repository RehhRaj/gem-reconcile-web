from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse
import pandas as pd
import tempfile
import os
import zipfile
import shutil

from reconcile_core import reconcile

app = FastAPI(title="GeM Payment Reconciliation")


# -------- HOME PAGE --------
@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
        <body style="font-family:Arial; padding:40px">
            <h2>GeM Payment Reconciliation</h2>
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


# -------- RECONCILE API --------
@app.post("/reconcile")
async def reconcile_api(
    invoice_file: UploadFile = File(...),
    payment_file: UploadFile = File(...)
):
    tmpdir = tempfile.mkdtemp()

    try:
        invoice_path = os.path.join(tmpdir, "invoice.xlsx")
        payment_path = os.path.join(tmpdir, "payment.xlsx")

        with open(invoice_path, "wb") as f:
            f.write(await invoice_file.read())

        with open(payment_path, "wb") as f:
            f.write(await payment_file.read())

        invoice_df = pd.read_excel(invoice_path)
        payment_df = pd.read_excel(payment_path)

        matched_df, unmatched_df = reconcile(invoice_df, payment_df)

        zip_path = os.path.join(tmpdir, "gem_reconciliation_result.zip")

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            matched_path = os.path.join(tmpdir, "matched.xlsx")
            unmatched_path = os.path.join(tmpdir, "unmatched.xlsx")

            matched_df.to_excel(matched_path, index=False)
            unmatched_df.to_excel(unmatched_path, index=False)

            zipf.write(matched_path, "matched_invoices.xlsx")
            zipf.write(unmatched_path, "unmatched_invoices.xlsx")

        return FileResponse(
            zip_path,
            media_type="application/zip",
            filename="gem_reconciliation_result.zip"
        )

    finally:
        shutil.rmtree(tmpdir)
