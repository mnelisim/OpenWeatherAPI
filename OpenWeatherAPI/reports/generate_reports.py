import time
from fpdf import FPDF
import os
from datetime import datetime
from OpenWeatherAPI.logs.logging import log_message

# ====================== PDF GENERATION REPORT ======================

def create_pdf_report(data, timestamp_file):
    """
    Generates a clean PDF report from a pandas DataFrame.
    """
    pdf = FPDF(orientation='L')  # Use landscape mode
    pdf.add_page()
    pdf.set_font("Arial", 'B', 10)
    pdf.cell(0, 10, "Weather Report", ln=True, align="C")
    pdf.ln(5)

    # Set column widths (adjust based on your data)
    col_widths = {
        "city": 35,
        "weather": 35,
        "windspeed": 25,
        "humidity": 20,
        "temperature": 25,
        "longitude": 25,
        "latitude": 25,
        "timestamp": 35
    }

    # Table header
    pdf.set_font("Arial", 'B', 11)
    for col in data.columns:
        width = col_widths.get(col.lower(), 30)  # default width if not defined
        pdf.cell(width, 10, col.capitalize(), border=1, align="C")
    pdf.ln()

    # Table rows
    pdf.set_font("Arial", '', 8)
    for _, row in data.iterrows():
        for col in data.columns:
            width = col_widths.get(col.lower(), 30)
            value = str(row[col])
            pdf.cell(width, 10, value, border=1, align="C")
        pdf.ln()

    # Save PDF
    os.makedirs("OpenWeatherAPI/reports/pdf", exist_ok=True)
    pdf_filename = f"OpenWeatherAPI/reports/pdf/weather_report_{timestamp_file}.pdf"
    pdf.output(pdf_filename)
    log_message(f"PDF Report Generated: {pdf_filename}", run_id=timestamp_file)



