import time
from fpdf import FPDF
import os
from datetime import datetime
from logs.logging import log_message

# ====================== PDF GENERATION REPORT ======================
def create_pdf_report(data, timestamp_file):

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(0, 10, f"Weather - Report {data.get('City', '')}", ln=True, align="C")
    pdf.ln(10)

    pdf.set_font("Arial", size=12)
    for key, value in data.items():
        pdf.cell(50, 10, f"{key.capitalize()}:", border=0)
        pdf.cell(0, 10, str(value), ln=True, border=0)
    
    os.makedirs("reports/pdf", exist_ok=True)
    pdf_filename = f"reports/pdf/weather_report_{timestamp_file}.pdf"
    pdf.output(pdf_filename)
    log_message(f"PDF Report Generated: {pdf_filename}", run_id=timestamp_file)


