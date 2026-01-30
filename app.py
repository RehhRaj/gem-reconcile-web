from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse
import tempfile
import pandas as pd
from reconcile_core import reconcile
import os

app = FastAPI(title="GeM Payment Reconciliation")

# @app.get("/", response_class=HTMLResponse)
# def home():
#     return """
#     <h2>GeM Payment Reconciliation</h2>
#     <form action="/reconcile" method="post" enctype="multipart/form-data">
#       Invoice File: <input type="file" name="invoice"><br><br>
#       Payment File: <input type="file" name="payment"><br><br>
#       <button type="submit">Reconcile</button>
#     </form>
#     """

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

