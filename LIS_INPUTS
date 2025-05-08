import blpapi
import openpyxl
import shutil
import os
import numpy as np
from datetime import datetime

# Bloomberg API setup
def setup_bloomberg_session():
    options = blpapi.SessionOptions()
    options.setServerHost("localhost")
    options.setServerPort(8194)
    session = blpapi.Session(options)
    if not session.start():
        print("Failed to start Bloomberg session.")
        return None
    if not session.openService("//blp/refdata"):
        print("Failed to open Bloomberg reference data service.")
        return None
    return session

# Fetch historical data from Bloomberg
def fetch_bloomberg_data(session, ticker, fields, start_year=2014, end_year=2024):
    ref_data_service = session.getService("//blp/refdata")
    request = ref_data_service.createRequest("HistoricalDataRequest")
    
    # Append exchange code if needed (e.g., "US" for US equities)
    security = f"{ticker} US Equity"
    request.getElement("securities").appendValue(security)
    
    for field in fields:
        request.getElement("fields").appendValue(field)
    
    request.set("periodicitySelection", "YEARLY")
    request.set("startDate", f"{start_year}0101")
    request.set("endDate", f"{end_year}1231")
    
    session.sendRequest(request)
    
    data = {field: {} for field in fields}
    while True:
        event = session.nextEvent()
        if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
            for msg in event:
                security_data = msg.getElement("securityData")
                field_data = security_data.getElement("fieldData")
                for i in range(field_data.numValues()):
                    datum = field_data.getValue(i)
                    date = datum.getElement("date").getValue()
                    year = date.year
                    for field in fields:
                        if datum.hasElement(field):
                            value = datum.getElement(field).getValue()
                            data[field][year] = value
        if event.eventType() == blpapi.Event.RESPONSE:
            break
    
    return data

# Calculate CAGR
def calculate_cagr(start_value, end_value, years):
    if start_value == 0 or end_value == 0 or years <= 0:
        return 0
    return ((end_value / start_value) ** (1 / years) - 1) * 100

# Map Inputs sheet fields to Bloomberg fields
bloomberg_field_map = {
    # Income Statement
    "Revenue (Sales)": "SALES_REV_TURN",
    "COGS (Cost of Goods Sold)": "COGS",
    "Gross Profit": "GROSS_PROFIT",
    "SG&A (Selling, General & Administrative)": "SGA_EXP",
    "R&D (Research & Development)": "RD_EXP",
    "EBITDA": "EBITDA",
    "D&A (Depreciation & Amortization)": "DEPR_AMORT_EXP",
    "Depreciation Expense": "DEPRECIATION_EXP",
    "Amortization Expense": "AMORT_INTAN_EXP",
    "Operating Income (EBIT)": "OPER_INC",
    "Net Interest Expense (Income)": "NET_INT_EXP",
    "Interest Expense": "INT_EXP",
    "Interest Income": "NON_OPER_INT_INC",
    "Pre-Tax Income (EBT)": "INC_BEF_XO_ITEMS",
    "Tax Expense (Benefits)": "TOT_PROV_INC_TAX",
    "Net Income": "NET_INCOME",
    "EPS Basic": "BASIC_EPS",
    "EPS Diluted": "DILUTED_EPS",
    "Basic Weighted Average Shares": "BASIC_AVG_SHS",
    "Diluted Weighted Average Shares": "DILUTED_AVG_SHS",
    # Balance Sheet
    "Cash & Cash Equivalents & ST Investments": "CASH_AND_ST_INVEST",
    "Cash & Cash Equivalents": "CASH_AND_EQUIV",
    "Short-Term Investments": "ST_INVEST",
    "Accounts Receivable": "ACCT_RCV",
    "Inventory": "INVENTORIES",
    "Prepaid Expenses and Other Current Assets": "OTH_CUR_ASSETS",
    "Current Assets": "TOT_CUR_ASSETS",
    "Net PP&E (Property, Plant and Equipment)": "NET_PPE",
    "Gross PP&E (Property, Plant and Equipment)": "GROSS_PPE",
    "Accumulated Depreciation": "ACCUM_DEPR",
    "Right-of-Use Assets": "OPER_LEASE_ASSETS",
    "Intangibles": "INTANGIBLE_ASSETS",
    "Goodwill": "GOODWILL",
    "Intangibles excl. Goodwill": "NET_OTHER_INTAN_ASSETS",
    "Other Non-Current Assets": "OTH_NON_CUR_ASSETS",
    "Non-Current Assets": "TOT_NON_CUR_ASSETS",
    "Total Assets": "TOT_ASSETS",
    "Accounts Payable": "ACCT_PAYABLE",
    "Short-Term Debt": "ST_DEBT",
    "Short-Term Borrowings": "ST_BORROWINGS",
    "Current Portion of Lease Liabilities": "CUR_PORT_LT_LEASE_LIAB",
    "Accated Expenses and Other Current Liabilities": "OTH_CUR_LIAB",
    "Current Liabilities": "TOT_CUR_LIAB",
    "Long-Term Debt": "LT_DEBT",
    "Long-Term Borrowings": "LT_BORROWINGS",
    "Long-Term Operating Lease Liabilities": "LT_LEASE_LIAB",
    "Other Non-Current Liabilities": "OTH_NON_CUR_LIAB",
    "  Non-Current Liabilities": "TOT_NON_CUR_LIAB",
    "Total Liabilities": "TOT_LIAB",
    "Shareholder's Equity": "TOT_COMMON_EQY",
    "Non-Controlling Interest": "MINORITY_NONCONT_INT",
    # Cash Flow
    "Operating Cash Flow": "CF_CASH_FROM_OPER",
    "Net Capex": "CF_CAP_EXPEND",
    "Acquisition of Fixed & Intangibles": "CF_CAPITAL_EXPEND",
    "Investing Cash Flow": "CF_CASH_FROM_INV_ACT",
    "Debt Borrowing": "CF_LT_BORROW",
    "Debt Repayment": "CF_REPAY_LT_DEBT",
    "Dividends": "CF_CASH_DIV_PAID",
    "Financing Cash Flow": "CF_CASH_FROM_FIN_ACT",
    "Net Changes in Cash": "CF_NET_CHNG_CASH",
    # Capital Structure
    "Market Capitalization": "CUR_MKT_CAP",
    "Total Debt": "TOT_DEBT",
    "Enterprise Value": "ENTERPRISE_VALUE",
    # Additional
    "Net Debt": "NET_DEBT",
    "Effective Tax Rate": "EFF_TAX_RATE",
    "NOPAT": "NOPAT"
}

# Fields not directly available in Bloomberg (approximated or manual)
manual_fields = [
    "Other Operating (Income) Expenses",
    "FX (Gain) Loss",
    "Other Non-Operating (Income) Expenses",
    "Changes in Net Working Capital",
    "(Increase) Decrease in Accounts Receivable",
    "(Increase) Decrease in Inventories",
    "Increase (Decrease) in Other",
    "Stock Based Compensation",
    "Other Operating Adjustments",
    "Disposal of Fixed & Intangibles",
    "Net Cash from Investments & Acquisitions",
    "Acquisitions",
    "Divestitures",
    "Increase in LT Investment",
    "Decrease in LT Investment",
    "Other Investing Inflows (Outflows)",
    "Lease Payments",
    "Increase (Repurchase) of Shares",
    "Other Financing Inflows (Outflows)",
    "Effect of Foreign Exchange",
    "DSO",
    "DIH",
    "DPO"
]

# Main function to process the valuation model
def populate_valuation_model(template_path, ticker):
    # Setup Bloomberg session
    session = setup_bloomberg_session()
    if not session:
        return
    
    # Fetch data
    fields = [v for k, v in bloomberg_field_map.items() if k not in manual_fields]
    data = fetch_bloomberg_data(session, ticker, fields)
    
    # Create a copy of the template
    output_path = f"{ticker}_valuation_model.xlsx"
    shutil.copy(template_path, output_path)
    
    # Load the workbook
    wb = openpyxl.load_workbook(output_path)
    ws = wb["Inputs"]
    
    # Map of row labels to row numbers (assuming labels are in column A)
    row_map = {}
    for row in range(1, ws.max_row + 1):
        cell_value = ws[f"A{row}"].value
        if cell_value in bloomberg_field_map or cell_value in manual_fields:
            row_map[cell_value] = row
    
    # Populate data
    year_columns = {2014: "B", 2015: "C", 2016: "D", 2017: "E", 2018: "F",
                    2019: "G", 2020: "H", 2021: "I", 2022: "J", 2023: "K", 2024: "L"}
    cagr_column = "M"
    
    for field, bloomberg_field in bloomberg_field_map.items():
        if field in manual_fields:
            continue
        if field not in row_map:
            print(f"Field {field} not found in Inputs sheet.")
            continue
        row = row_map[field]
        values = data.get(bloomberg_field, {})
        
        # Write data for each year
        for year, col in year_columns.items():
            if year in values:
                # Convert to millions if necessary (Bloomberg data may be in thousands)
                value = values[year] / 1000  # Adjust based on Bloomberg unit
                ws[f"{col}{row}"] = value
        
        # Calculate CAGR
        start_value = values.get(2014, 0)
        end_value = values.get(2024, 0)
        if start_value and end_value:
            cagr = calculate_cagr(start_value, end_value, 10)
            ws[f"{cagr_column}{row}"] = cagr / 100  # Store as decimal
    
    # Save the workbook
    wb.save(output_path)
    print(f"Valuation model saved as {output_path}")
    
    # Close Bloomberg session
    session.stop()

# Run the program
if __name__ == "__main__":
    template_path = "LIS_Valuation_Empty.xlsx"  # Path to your template
    ticker = input("Enter the ticker symbol (e.g., AAPL): ").strip().upper()
    populate_valuation_model(template_path, ticker)
