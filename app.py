from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse
import tempfile
import pandas as pd
from reconcile_core import reconcile
import os

app = FastAPI(title="GeM Payment Reconciliation")

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h2>GeM Payment Reconciliation</h2>
    <form action="/reconcile" method="post" enctype="multipart/form-data">
      Invoice File: <input type="file" name="invoice"><br><br>
      Payment File: <input type="file" name="payment"><br><br>
      <button type="submit">Reconcile</button>
    </form>
    """

@app.post("/reconcile")
async def reconcile_files(
    invoice: UploadFile = File(...),
    payment: UploadFile = File(...)
):
    with tempfile.TemporaryDirectory() as tmp:
        inv_path = os.path.join(tmp, invoice.filename)
        pay_path = os.path.join(tmp, payment.filename)

        with open(inv_path, "wb") as f:
            f.write(await invoice.read())
        with open(pay_path, "wb") as f:
            f.write(await payment.read())

        invoice_df, matched, unmatched = reconcile(inv_path, pay_path)

        output_path = os.path.join(tmp, "matched_invoices.xlsx")
        invoice_df.to_excel(output_path, index=False)

        return FileResponse(
            output_path,
            filename="matched_invoices.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
