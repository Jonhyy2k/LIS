import blpapi
import openpyxl
import shutil
import os
import numpy as np
import time
from datetime import datetime

def setup_bloomberg_session(ticker_symbol):
    """Initialize Bloomberg API session with detailed logging."""
    options = blpapi.SessionOptions()
    options.setServerHost("localhost")
    options.setServerPort(8194)
    session = blpapi.Session(options)
    
    print(f"[INFO] Attempting to connect to Bloomberg for {ticker_symbol}...")
    if not session.start():
        print("[WARNING] Failed to start Bloomberg session. Ensure Bloomberg Terminal is running.")
        return None
    if not session.openService("//blp/refdata"):
        print("[WARNING] Failed to open Bloomberg reference data service.")
        return None
    print("[INFO] Bloomberg session started successfully.")
    return session

def fetch_bloomberg_data(session, ticker, fields, start_year=2014, end_year=2024, timeout=10):
    """Fetch historical data from Bloomberg with timeout and error handling."""
    ref_data_service = session.getService("//blp/refdata")
    request = ref_data_service.createRequest("HistoricalDataRequest")
    security = f"{ticker} US Equity"  # Adapt for non-US equities if needed (e.g., 'LN Equity')
    request.getElement("securities").appendValue(security)
    for field in fields:
        request.getElement("fields").appendValue(field)
    request.set("periodicitySelection", "YEARLY")
    request.set("startDate", f"{start_year}0101")
    request.set("endDate", f"{end_year}1231")
    session.sendRequest(request)
    
    data = {field: {} for field in fields}
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        event = session.nextEvent(500)  # Check every 500ms
        if event.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
            for msg in event:
                print(f"[DEBUG] Received message: {msg}")
                if msg.hasElement("responseError"):
                    error = msg.getElement("responseError")
                    error_message = error.getElement("message").getValue()
                    raise ValueError(f"Bloomberg API error: {error_message}")
                if not msg.hasElement("securityData"):
                    raise ValueError("No securityData element in response. Check ticker or data availability.")
                
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
        elif event.eventType() in [blpapi.Event.SESSION_STATUS, blpapi.Event.SERVICE_STATUS]:
            for msg in event:
                if msg.messageType() == blpapi.Name("SessionTerminated"):
                    print("[WARNING] Bloomberg session terminated unexpectedly.")
                    return None
        elif event.eventType() == blpapi.Event.TIMEOUT:
            print("[DEBUG] Bloomberg event timeout.")
            continue
        if event.eventType() == blpapi.Event.RESPONSE:
            break
    
    if not any(data[field] for field in data):
        print(f"[WARNING] No data received for {ticker} within {timeout}s.")
    return data

def calculate_derived_metrics(data, start_year=2014, end_year=2024):
    """Calculate derived metrics like DSO, DIH, DPO, and Net Working Capital changes."""
    derived = {
        "Changes in Net Working Capital": {},
        "DSO": {},
        "DIH": {},
        "DPO": {},
        "Net Cash from Investments & Acquisitions": {},
        "Increase (Decrease) in Other": {}
    }
    
    for year in range(start_year, end_year + 1):
        if year in data.get("TOT_CUR_ASSETS", {}) and year in data.get("TOT_CUR_LIAB", {}) and \
           year - 1 in data.get("TOT_CUR_ASSETS", {}) and year - 1 in data.get("TOT_CUR_LIAB", {}):
            nwc_t = data["TOT_CUR_ASSETS"][year] - data["TOT_CUR_LIAB"][year]
            nwc_t1 = data["TOT_CUR_ASSETS"][year - 1] - data["TOT_CUR_LIAB"][year - 1]
            derived["Changes in Net Working Capital"][year] = nwc_t - nwc_t1
        
        if year in data.get("ACCT_RCV", {}) and year in data.get("SALES_REV_TURN", {}) and \
           year in data.get("INVENTORIES", {}) and year in data.get("COGS", {}) and \
           year in data.get("ACCT_PAYABLE", {}):
            revenue = data["SALES_REV_TURN"][year]
            cogs = data["COGS"][year]
            ar = data["ACCT_RCV"][year]
            inv = data["INVENTORIES"][year]
            ap = data["ACCT_PAYABLE"][year]
            derived["DSO"][year] = (ar / revenue * 365) if revenue else 0
            derived["DIH"][year] = (inv / cogs * 365) if cogs else 0
            derived["DPO"][year] = (ap / cogs * 365) if cogs else 0
        
        if year in data.get("CF_ACQUISITIONS", {}) and year in data.get("CF_DISPOSALS", {}) and \
           year in data.get("CF_OTHER_INVEST_ACT", {}):
            derived["Net Cash from Investments & Acquisitions"][year] = (
                data["CF_ACQUISITIONS"][year] +
                data["CF_DISPOSALS"][year] +
                data["CF_OTHER_INVEST_ACT"][year]
            )
        
        if year in derived["Changes in Net Working Capital"] and \
           year in data.get("CF_CHG_ACCT_RCV", {}) and year in data.get("CF_CHG_INVENTORIES", {}) and \
           year in data.get("CF_CHG_ACCT_PAYABLE", {}):
            derived["Increase (Decrease) in Other"][year] = (
                derived["Changes in Net Working Capital"][year] -
                (data["CF_CHG_ACCT_RCV"][year] +
                 data["CF_CHG_INVENTORIES"][year] +
                 data["CF_CHG_ACCT_PAYABLE"][year])
            )
    
    return derived

def calculate_cagr(start_value, end_value, years):
    """Calculate Compound Annual Growth Rate."""
    if start_value == 0 or end_value == 0 or years <= 0:
        return 0
    return ((end_value / start_value) ** (1 / years) - 1) * 100

# Full field map for all required fields
field_map = {
    # Income Statement
    "Revenue (Sales)": {"source": "BDH", "field": "SALES_REV_TURN"},
    "COGS (Cost of Goods Sold)": {"source": "BDH", "field": "COGS"},
    "Gross Profit": {"source": "BDH", "field": "GROSS_PROFIT"},
    "SG&A (Selling, General & Administrative)": {"source": "BDH", "field": "SGA_EXP"},
    "R&D (Research & Development)": {"source": "BDH", "field": "RD_EXP"},
    "Other Operating (Income) Expenses": {"source": "BDH", "field": "IS_OTHER_OPER_EXP"},
    "EBITDA": {"source": "BDH", "field": "EBITDA"},
    "D&A (Depreciation & Amortization)": {"source": "BDH", "field": "DEPR_AMORT_EXP"},
    "Depreciation Expense": {"source": "BDH", "field": "DEPRECIATION_EXP"},
    "Amortization Expense": {"source": "BDH", "field": "AMORT_INTAN_EXP"},
    "Operating Income (EBIT)": {"source": "BDH", "field": "OPER_INC"},
    "Net Interest Expense (Income)": {"source": "BDH", "field": "NET_INT_EXP"},
    "Interest Expense": {"source": "BDH", "field": "INT_EXP"},
    "Interest Income": {"source": "BDH", "field": "NON_OPER_INT_INC"},
    "FX (Gain) Loss": {"source": "BDH", "field": "IS_FX_GAIN_LOSS"},
    "Other Non-Operating (Income) Expenses": {"source": "BDH", "field": "IS_NON_OPER_INC_EXP"},
    "Pre-Tax Income (EBT)": {"source": "BDH", "field": "INC_BEF_XO_ITEMS"},
    "Tax Expense (Benefits)": {"source": "BDH", "field": "TOT_PROV_INC_TAX"},
    "Net Income": {"source": "BDH", "field": "NET_INCOME"},
    "EPS Basic": {"source": "BDH", "field": "BASIC_EPS"},
    "EPS Diluted": {"source": "BDH", "field": "DILUTED_EPS"},
    "Basic Weighted Average Shares": {"source": "BDH", "field": "BASIC_AVG_SHS"},
    "Diluted Weighted Average Shares": {"source": "BDH", "field": "DILUTED_AVG_SHS"},
    # Balance Sheet
    "Cash & Cash Equivalents & ST Investments": {"source": "BDH", "field": "CASH_AND_ST_INVEST"},
    "Cash & Cash Equivalents": {"source": "BDH", "field": "CASH_AND_EQUIV"},
    "Short-Term Investments": {"source": "BDH", "field": "ST_INVEST"},
    "Accounts Receivable": {"source": "BDH", "field": "ACCT_RCV"},
    "Inventory": {"source": "BDH", "field": "INVENTORIES"},
    "Prepaid Expenses and Other Current Assets": {"source": "BDH", "field": "OTH_CUR_ASSETS"},
    "Current Assets": {"source": "BDH", "field": "TOT_CUR_ASSETS"},
    "Net PP&E (Property, Plant and Equipment)": {"source": "BDH", "field": "NET_PPE"},
    "Gross PP&E (Property, Plant and Equipment)": {"source": "BDH", "field": "GROSS_PPE"},
    "Accumulated Depreciation": {"source": "BDH", "field": "ACCUM_DEPR"},
    "Right-of-Use Assets": {"source": "BDH", "field": "OPER_LEASE_ASSETS"},
    "Intangibles": {"source": "BDH", "field": "INTANGIBLE_ASSETS"},
    "Goodwill": {"source": "BDH", "field": "GOODWILL"},
    "Intangibles excl. Goodwill": {"source": "BDH", "field": "NET_OTHER_INTAN_ASSETS"},
    "Other Non-Current Assets": {"source": "BDH", "field": "OTH_NON_CUR_ASSETS"},
    "Non-Current Assets": {"source": "BDH", "field": "TOT_NON_CUR_ASSETS"},
    "Total Assets": {"source": "BDH", "field": "TOT_ASSETS"},
    "Accounts Payable": {"source": "BDH", "field": "ACCT_PAYABLE"},
    "Short-Term Debt": {"source": "BDH", "field": "ST_DEBT"},
    "Short-Term Borrowings": {"source": "BDH", "field": "ST_BORROWINGS"},
    "Current Portion of Lease Liabilities": {"source": "BDH", "field": "CUR_PORT_LT_LEASE_LIAB"},
    "Accrued Expenses and Other Current Liabilities": {"source": "BDH", "field": "OTH_CUR_LIAB"},
    "Current Liabilities": {"source": "BDH", "field": "TOT_CUR_LIAB"},
    "Long-Term Debt": {"source": "BDH", "field": "LT_DEBT"},
    "Long-Term Borrowings": {"source": "BDH", "field": "LT_BORROWINGS"},
    "Long-Term Operating Lease Liabilities": {"source": "BDH", "field": "LT_LEASE_LIAB"},
    "Other Non-Current Liabilities": {"source": "BDH", "field": "OTH_NON_CUR_LIAB"},
    "Non-Current Liabilities": {"source": "BDH", "field": "TOT_NON_CUR_LIAB"},
    "Total Liabilities": {"source": "BDH", "field": "TOT_LIAB"},
    "Shareholder's Equity": {"source": "BDH", "field": "TOT_COMMON_EQY"},
    "Non-Controlling Interest": {"source": "BDH", "field": "MINORITY_NONCONT_INT"},
    # Cash Flow
    "(Increase) Decrease in Accounts Receivable": {"source": "BDH", "field": "CF_CHG_ACCT_RCV"},
    "(Increase) Decrease in Inventories": {"source": "BDH", "field": "CF_CHG_INVENTORIES"},
    "Increase (Decrease) in Other": {"source": "derived", "field": "Increase (Decrease) in Other"},
    "Stock Based Compensation": {"source": "BDH", "field": "CF_STOCK_BASED_COMP"},
    "Other Operating Adjustments": {"source": "BDH", "field": "CF_OTHER_OPER_ADJUSTMENTS"},
    "Operating Cash Flow": {"source": "BDH", "field": "CF_CASH_FROM_OPER"},
    "Net Capex": {"source": "BDH", "field": "CF_CAP_EXPEND"},
    "Acquisition of Fixed & Intangibles": {"source": "BDH", "field": "CF_CAPITAL_EXPEND"},
    "Disposal of Fixed & Intangibles": {"source": "BDH", "field": "CF_DISPOSAL_PPE_INTAN"},
    "Acquisitions": {"source": "BDH", "field": "CF_ACQUISITIONS"},
    "Divestitures": {"source": "BDH", "field": "CF_DISPOSALS"},
    "Increase in LT Investment": {"source": "BDH", "field": "CF_PURCH_LT_INVEST"},
    "Decrease in LT Investment": {"source": "BDH", "field": "CF_SALE_LT_INVEST"},
    "Other Investing Inflows (Outflows)": {"source": "BDH", "field": "CF_OTHER_INVEST_ACT"},
    "Investing Cash Flow": {"source": "BDH", "field": "CF_CASH_FROM_INV_ACT"},
    "Lease Payments": {"source": "BDH", "field": "CF_LEASE_PAYMENTS"},
    "Debt Borrowing": {"source": "BDH", "field": "CF_LT_BORROW"},
    "Debt Repayment": {"source": "BDH", "field": "CF_REPAY_LT_DEBT"},
    "Dividends": {"source": "BDH", "field": "CF_CASH_DIV_PAID"},
    "Increase (Repurchase) of Shares": {"source": "BDH", "field": "CF_SHARE_REPURCHASE"},
    "Other Financing Inflows (Outflows)": {"source": "BDH", "field": "CF_OTHER_FIN_ACT"},
    "Financing Cash Flow": {"source": "BDH", "field": "CF_CASH_FROM_FIN_ACT"},
    "Effect of Foreign Exchange": {"source": "BDH", "field": "CF_FX_EFFECT"},
    "Net Changes in Cash": {"source": "BDH", "field": "CF_NET_CHNG_CASH"},
    # Capital Structure
    "Market Capitalization": {"source": "BDH", "field": "CUR_MKT_CAP"},
    "Total Debt": {"source": "BDH", "field": "TOT_DEBT"},
    "Preferred Stock": {"source": "BDH", "field": "PREFERRED_EQUITY"},
    "Non-Controlling Interest": {"source": "BDH", "field": "MINORITY_NONCONT_INT"},
    "Enterprise Value": {"source": "BDH", "field": "ENTERPRISE_VALUE"},
    # Additional
    "Total Borrowings": {"source": "BDH", "field": "TOT_BORROWINGS"},
    "Total Leases": {"source": "BDH", "field": "TOT_LEASE_LIAB"},
    "Net Debt": {"source": "BDH", "field": "NET_DEBT"},
    "Effective Tax Rate": {"source": "BDH", "field": "EFF_TAX_RATE"},
    "NOPAT": {"source": "BDH", "field": "NOPAT"},
    # Derived Metrics
    "Changes in Net Working Capital": {"source": "derived", "field": "Changes in Net Working Capital"},
    "DSO": {"source": "derived", "field": "DSO"},
    "DIH": {"source": "derived", "field": "DIH"},
    "DPO": {"source": "derived", "field": "DPO"},
    "Net Cash from Investments & Acquisitions": {"source": "derived", "field": "Net Cash from Investments & Acquisitions"}
}

def populate_valuation_model(template_path, ticker):
    """Populate the Inputs sheet with Bloomberg data for the given ticker."""
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template file {template_path} not found.")
    
    session = setup_bloomberg_session(ticker)
    if not session:
        return
    
    try:
        bdh_fields = [v["field"] for k, v in field_map.items() if v["source"] == "BDH"]
        bdh_data = fetch_bloomberg_data(session, ticker, bdh_fields)
        
        derived_data = calculate_derived_metrics(bdh_data)
        
        output_path = f"{ticker}_valuation_model.xlsx"
        shutil.copy(template_path, output_path)
        
        wb = openpyxl.load_workbook(output_path)
        if "Inputs" not in wb.sheetnames:
            raise ValueError("Inputs sheet not found in the template file.")
        ws = wb["Inputs"]
        
        row_map = {}
        for row in range(1, ws.max_row + 1):
            cell_value = ws[f"A{row}"].value
            if cell_value and cell_value in field_map:
                row_map[cell_value] = row
        
        year_columns = {2014: "B", 2015: "C", 2016: "D", 2017: "E", 2018: "F",
                        2019: "G", 2020: "H", 2021: "I", 2022: "J", 2023: "K", 2024: "L"}
        cagr_column = "M"
        
        for field, config in field_map.items():
            if field not in row_map:
                print(f"[WARNING] Field '{field}' not found in Inputs sheet.")
                continue
            row = row_map[field]
            
            if config["source"] == "BDH":
                values = bdh_data.get(config["field"], {})
                for year, col in year_columns.items():
                    if year in values:
                        ws[f"{col}{row}"] = values[year] / 1000  # Convert to millions
                start_value = values.get(2014, 0)
                end_value = values.get(2024, 0)
            elif config["source"] == "derived":
                values = derived_data[config["field"]]
                for year, col in year_columns.items():
                    if year in values:
                        ws[f"{col}{row}"] = values[year]
                start_value = values.get(2014, 0)
                end_value = values.get(2024, 0)
            
            if start_value and end_value:
                cagr = calculate_cagr(start_value, end_value, 10)
                ws[f"{cagr_column}{row}"] = cagr / 100
        
        wb.save(output_path)
        print(f"[INFO] Valuation model saved as {output_path}")
    
    except Exception as e:
        print(f"[ERROR] Error during Bloomberg API interaction for {ticker}: {e}")
    
    finally:
        try:
            session.stop()
            print("[INFO] Bloomberg session stopped.")
        except Exception as e:
            print(f"[WARNING] Error stopping Bloomberg session: {e}")

if __name__ == "__main__":
    template_path = "LIS_Valuation_Empty.xlsx"
    
    ticker = input("Enter the ticker symbol (e.g., AAPL): ").strip().upper()
    if not ticker:
        print("[ERROR] Ticker symbol cannot be empty.")
    elif not ticker.isalnum():
        print("[ERROR] Ticker symbol must contain only letters and numbers.")
    else:
        try:
            populate_valuation_model(template_path, ticker)
        except Exception as e:
            print(f"[ERROR] An error occurred: {e}")
