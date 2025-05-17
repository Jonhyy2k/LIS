# You need to login to the Bloomberg Terminal for the script to work!
# Run it using the arrow on the top right.
# Enter the stock ticker while specifying the country in the end,
# For example AAPL US or 000660 KS (the script will automatically add [Equity]

import blpapi
import openpyxl
import shutil
import os
import time
from datetime import datetime
import matplotlib.pyplot as plt
from io import BytesIO
from openpyxl.drawing.image import Image

def setup_bloomberg_session(ticker_symbol):
    """
    Initializes and starts a Bloomberg API session.
    Returns the session object or None if it fails.
    """
    options = blpapi.SessionOptions()
    options.setServerHost("localhost")
    options.setServerPort(8194)

    session = blpapi.Session(options)
    try:
        if not session.start():
            print(f"❌ Failed to start Bloomberg session for {ticker_symbol}. Are you sure the Bloomberg Terminal is running and you're logged in?")
            return None
        if not session.openService("//blp/refdata"):
            print(f"❌ Failed to open Bloomberg reference data service for {ticker_symbol}. Something's wrong with the Bloomberg API setup.")
            session.stop()
            return None
        return session
    except Exception as e:
        print(f"❌ Exception while setting up Bloomberg session for {ticker_symbol}: {e}")
        if session:
            session.stop()
        return None

def batch_fields(fields, batch_size):
    """
    Splits a list of fields into batches of specified size.
    Returns a list of lists (batches).
    """
    return [fields[i:i + batch_size] for i in range(0, len(fields), batch_size)]

def fetch_bloomberg_data(session, ticker, fields, field_to_name_map, start_year=2014, end_year=2024, timeout=30):
    """
    Fetches data from Bloomberg for the given ticker and fields.
    Returns a dictionary with field codes as keys and yearly data as values.
    """
    ref_data_service = session.getService("//blp/refdata")
    request = ref_data_service.createRequest("HistoricalDataRequest")
    request.getElement("securities").appendValue(f"{ticker} Equity")
    for field in fields:
        request.getElement("fields").appendValue(field)
    request.set("periodicityAdjustment", "ACTUAL")
    request.set("periodicitySelection", "YEARLY")
    request.set("startDate", f"{start_year}0101")
    request.set("endDate", f"{end_year}1231")

    session.sendRequest(request)
    start_time = time.time()

    data = {field: {} for field in fields}

    while True:
        if time.time() - start_time > timeout:
            print(f"⏰ Timeout after {timeout} seconds while fetching data for {ticker}. Bloomberg might be slow or unresponsive.")
            return None

        event = session.nextEvent(500)
        for msg in event:
            if event.eventType() in [blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE]:
                if msg.hasElement("securityData"):
                    security_data = msg.getElement("securityData")
                    field_data = security_data.getElement("fieldData")
                    for i in range(field_data.numValues()):
                        datum = field_data.getValue(i)
                        date_str = datum.getElement("date").getValueAsString()
                        year = int(date_str[:4])
                        if start_year <= year <= end_year:
                            for field in fields:
                                if datum.hasElement(field):
                                    value = datum.getElement(field).getValue()
                                    data[field][year] = value
                                else:
                                    data[field][year] = None
        if event.eventType() == blpapi.Event.RESPONSE:
            break
        elif event.eventType() == blpapi.Event.SESSION_STATUS:
            if msg.messageType() == "SessionTerminated":
                print(f"❌ Bloomberg session terminated unexpectedly for {ticker}.")
                return None

    fetched_data = {}
    for field, yearly_data in data.items():
        excel_name = field_to_name_map.get(field, field)
        fetched_data[field] = yearly_data

    return fetched_data

def get_target_cells_for_years(base_cell_ref, num_years):
    """
    Generates a list of cell references for a given base cell and number of years.
    Assumes data is laid out horizontally (e.g., G6, H6, I6, ...).
    """
    import re
    match = re.match(r"([A-Z]+)(\d+)", base_cell_ref)
    if not match:
        raise ValueError(f"Invalid cell reference: {base_cell_ref}")
    col, row = match.groups()
    col_num = sum((ord(c) - ord('A') + 1) * (26 ** i) for i, c in enumerate(reversed(col)))
    cells = []
    for i in range(num_years):
        new_col_num = col_num + i
        new_col = ""
        while new_col_num > 0:
            new_col_num, remainder = divmod(new_col_num - 1, 26)
            new_col = chr(65 + remainder) + new_col
        cells.append(f"{new_col}{row}")
    return cells

def calculate_derived_metrics(fetched_data, start_year, end_year):
    """
    Calculates derived metrics based on fetched data (e.g., Total Other Operating).
    Returns a dictionary of derived metrics.
    """
    derived_data = {}
    
    other_operating_components = [
        "IS_OTHER_OPER_INC",
        "IS_OTHER_OPERATING_EXPENSES"
    ]

    derived_data["Total_Other_Operating"] = {}
    
    for year in range(start_year, end_year + 1):
        total_other = 0
        has_value = False
        
        for field in other_operating_components:
            if field in fetched_data and year in fetched_data[field]:
                value = fetched_data[field].get(year)
                if isinstance(value, (int, float)):
                    total_other += value
                    has_value = True
        
        if has_value:
            derived_data["Total_Other_Operating"][year] = total_other
        else:
            derived_data["Total_Other_Operating"][year] = "N/A (Missing Components)"

    return derived_data

# Define the mapping of Excel field names to Bloomberg fields and their sources
field_map = {
    # Income Statement (IS)
    "Revenue (Sales)": {"source": "BDH", "field": "SALES_REV_TURN", "statement": "IS"},
    "Cost of Goods Sold": {"source": "BDH", "field": "IS_COGS", "statement": "IS"},
    "Gross Profit": {"source": "BDH", "field": "IS_GROSS_PROFIT", "statement": "IS"},
    "R&D Expenses": {"source": "BDH", "field": "IS_RD_EXPEND", "statement": "IS"},
    "SG&A Expenses": {"source": "BDH", "field": "IS_SGA_EXP", "statement": "IS"},
    "Other Operating Income": {"source": "BDH", "field": "IS_OTHER_OPER_INC", "statement": "IS"},
    "Other Operating Expenses": {"source": "BDH", "field": "IS_OTHER_OPERATING_EXPENSES", "statement": "IS"},
    "Total Other Operating": {"source": "derived", "field": "Total_Other_Operating", "statement": "IS"},
    "Operating Income": {"source": "BDH", "field": "IS_OPER_INC", "statement": "IS"},
    "Interest Expense": {"source": "BDH", "field": "IS_INT_EXPENSE", "statement": "IS"},
    "Interest Income": {"source": "BDH", "field": "IS_INT_INCOME", "statement": "IS"},
    "Other Non-Operating Income": {"source": "BDH", "field": "IS_NON_OPER_OTHER", "statement": "IS"},
    "Pre-Tax Income": {"source": "BDH", "field": "IS_PRETAX_INCOME", "statement": "IS"},
    "Income Taxes": {"source": "BDH", "field": "IS_INC_TAX", "statement": "IS"},
    "Net Income": {"source": "BDH", "field": "IS_NET_INCOME", "statement": "IS"},
    "Shares Outstanding": {"source": "BDH", "field": "BS_SHRS_OUTSTANDING", "statement": "IS"},
    "EPS (Diluted)": {"source": "BDH", "field": "IS_EPS_DILUTED", "statement": "IS"},
    
    # Balance Sheet (BS)
    "Cash & Cash Equivalents": {"source": "BDH", "field": "BS_CASH_NEAR_CASH_ITEM", "statement": "BS"},
    "Accounts Receivable": {"source": "BDH", "field": "BS_ACCOUNTS_RECEIVABLE", "statement": "BS"},
    "Inventory": {"source": "BDH", "field": "BS_INVENTORIES", "statement": "BS"},
    "Other Current Assets": {"source": "BDH", "field": "BS_OTHER_CURRENT_ASSETS", "statement": "BS"},
    "Total Current Assets": {"source": "BDH", "field": "BS_TOT_CUR_ASSET", "statement": "BS"},
    "Property, Plant & Equipment": {"source": "BDH", "field": "BS_TOT_FIX_ASSET", "statement": "BS"},
    "Goodwill": {"source": "BDH", "field": "BS_GOODWILL", "statement": "BS"},
    "Intangible Assets": {"source": "BDH", "field": "BS_INTANGIBLE_ASSETS", "statement": "BS"},
    "Other Long-Term Assets": {"source": "BDH", "field": "BS_OTHER_LT_ASSETS", "statement": "BS"},
    "Total Assets": {"source": "BDH", "field": "BS_TOT_ASSET", "statement": "BS"},
    "Accounts Payable": {"source": "BDH", "field": "BS_ACCTS_PAYABLE", "statement": "BS"},
    "Short-Term Debt": {"source": "BDH", "field": "BS_ST_BORROWINGS", "statement": "BS"},
    "Other Current Liabilities": {"source": "BDH", "field": "BS_OTHER_CURRENT_LIAB", "statement": "BS"},
    "Total Current Liabilities": {"source": "BDH", "field": "BS_TOT_CUR_LIAB", "statement": "BS"},
    "Long-Term Debt": {"source": "BDH", "field": "BS_LT_BORROWINGS", "statement": "BS"},
    "Other Long-Term Liabilities": {"source": "BDH", "field": "BS_OTHER_LT_LIAB", "statement": "BS"},
    "Total Liabilities": {"source": "BDH", "field": "BS_TOT_LIAB", "statement": "BS"},
    "Common Equity": {"source": "BDH", "field": "BS_COMMON_EQUITY", "statement": "BS"},
    "Retained Earnings": {"source": "BDH", "field": "BS_RETAINED_EARNINGS", "statement": "BS"},
    "Total Equity": {"source": "BDH", "field": "BS_TOT_EQUITY", "statement": "BS"},
    
    # Cash Flow Statement (CF)
    "Operating Cash Flow": {"source": "BDH", "field": "CF_CASH_FROM_OPER", "statement": "CF"},
    "Capital Expenditures": {"source": "BDH", "field": "CF_CAP_EXpend", "statement": "CF"},
    "Acquisitions": {"source": "BDH", "field": "CF_ACQUISITIONS", "statement": "CF"},
    "Dividends Paid": {"source": "BDH", "field": "CF_CASH_DIV_PAID", "statement": "CF"},
    "Issuance/Repayment of Debt": {"source": "BDH", "field": "CF_NET_DEBT_ISSUED", "statement": "CF"},
    "Issuance/Repurchase of Equity": {"source": "BDH", "field": "CF_NET_EQUITY_ISSUED", "statement": "CF"},
    "Free Cash Flow": {"source": "BDH", "field": "CF_FREE_CASH_FLOW", "statement": "CF"},
    
    # New Fields for Requested Data
    "Historical Stock Price": {"source": "BDH", "field": "PX_LAST", "statement": "Other"},
    "Insider Ownership Percent": {"source": "BDH", "field": "PCT_INSIDER_SHARES_OWNED", "statement": "Other"},
    "Institutional Ownership Percent": {"source": "BDH", "field": "PCT_INSTITUTIONAL_OWNED", "statement": "Other"},
    "Short Interest Percent": {"source": "BDH", "field": "SHORT_INT_RATIO", "statement": "Other"},
    "Top Holders": {"source": "BDS", "field": "OWN_TOP_HOLDERS", "statement": "Other"},
    "Company Description": {"source": "BDS", "field": "COMPANY_DESCRIPTION", "statement": "Other"},
}

# Define the mapping of Excel field names to their sheet and cell locations
field_cell_map = {
    # Income Statement (IS)
    "Revenue (Sales)": {"sheet": "Inputs", "cell": "G6"},
    "Cost of Goods Sold": {"sheet": "Inputs", "cell": "G7"},
    "Gross Profit": {"sheet": "Inputs", "cell": "G8"},
    "R&D Expenses": {"sheet": "Inputs", "cell": "G9"},
    "SG&A Expenses": {"sheet": "Inputs", "cell": "G10"},
    "Other Operating Income": {"sheet": "Inputs", "cell": "G11"},
    "Other Operating Expenses": {"sheet": "Inputs", "cell": "G12"},
    "Total Other Operating": {"sheet": "Inputs", "cell": "G13"},
    "Operating Income": {"sheet": "Inputs", "cell": "G14"},
    "Interest Expense": {"sheet": "Inputs", "cell": "G15"},
    "Interest Income": {"sheet": "Inputs", "cell": "G16"},
    "Other Non-Operating Income": {"sheet": "Inputs", "cell": "G17"},
    "Pre-Tax Income": {"sheet": "Inputs", "cell": "G18"},
    "Income Taxes": {"sheet": "Inputs", "cell": "G19"},
    "Net Income": {"sheet": "Inputs", "cell": "G20"},
    "Shares Outstanding": {"sheet": "Inputs", "cell": "G21"},
    "EPS (Diluted)": {"sheet": "Inputs", "cell": "G22"},
    
    # Balance Sheet (BS)
    "Cash & Cash Equivalents": {"sheet": "Inputs", "cell": "G33"},
    "Accounts Receivable": {"sheet": "Inputs", "cell": "G34"},
    "Inventory": {"sheet": "Inputs", "cell": "G35"},
    "Other Current Assets": {"sheet": "Inputs", "cell": "G36"},
    "Total Current Assets": {"sheet": "Inputs", "cell": "G37"},
    "Property, Plant & Equipment": {"sheet": "Inputs", "cell": "G38"},
    "Goodwill": {"sheet": "Inputs", "cell": "G39"},
    "Intangible Assets": {"sheet": "Inputs", "cell": "G40"},
    "Other Long-Term Assets": {"sheet": "Inputs", "cell": "G41"},
    "Total Assets": {"sheet": "Inputs", "cell": "G42"},
    "Accounts Payable": {"sheet": "Inputs", "cell": "G43"},
    "Short-Term Debt": {"sheet": "Inputs", "cell": "G44"},
    "Other Current Liabilities": {"sheet": "Inputs", "cell": "G45"},
    "Total Current Liabilities": {"sheet": "Inputs", "cell": "G46"},
    "Long-Term Debt": {"sheet": "Inputs", "cell": "G47"},
    "Other Long-Term Liabilities": {"sheet": "Inputs", "cell": "G48"},
    "Total Liabilities": {"sheet": "Inputs", "cell": "G49"},
    "Common Equity": {"sheet": "Inputs", "cell": "G50"},
    "Retained Earnings": {"sheet": "Inputs", "cell": "G51"},
    "Total Equity": {"sheet": "Inputs", "cell": "G52"},
    
    # Cash Flow Statement (CF)
    "Operating Cash Flow": {"sheet": "Inputs", "cell": "G76"},
    "Capital Expenditures": {"sheet": "Inputs", "cell": "G77"},
    "Acquisitions": {"sheet": "Inputs", "cell": "G78"},
    "Dividends Paid": {"sheet": "Inputs", "cell": "G79"},
    "Issuance/Repayment of Debt": {"sheet": "Inputs", "cell": "G80"},
    "Issuance/Repurchase of Equity": {"sheet": "Inputs", "cell": "G81"},
    "Free Cash Flow": {"sheet": "Inputs", "cell": "G82"},
    
    # New Mappings for Requested Data
    "Insider Ownership Percent": {"sheet": "Peers", "cell": "D76"},
    "Institutional Ownership Percent": {"sheet": "Peers", "cell": "D77"},
    "Short Interest Percent": {"sheet": "Peers", "cell": "D78"},
}

def generate_chart(historical_data, ticker_symbol):
    """
    Generate a line chart for the last 5 years of stock prices and save as an image.
    Returns a BytesIO buffer containing the chart image or None if no data.
    """
    if "PX_LAST" not in historical_data or not historical_data["PX_LAST"]:
        print(f"⚠️ No historical stock price data available for {ticker_symbol}. Skipping chart generation.")
        return None
    
    years = sorted([year for year in historical_data["PX_LAST"].keys() if 2020 <= year <= 2024])
    prices = [historical_data["PX_LAST"].get(year, 0) for year in years]
    
    if not prices or all(p is None or p == "N/A (Missing)" for p in prices):
        print(f"⚠️ Insufficient valid price data for {ticker_symbol} to generate chart.")
        return None
    
    plt.figure(figsize=(8, 4))
    plt.plot(years, prices, marker='o')
    plt.title(f"{ticker_symbol} Stock Price (2020-2024)")
    plt.xlabel("Year")
    plt.ylabel("Price (USD)")
    plt.grid(True)
    
    # Save chart to a BytesIO buffer
    buffer = BytesIO()
    plt.savefig(buffer, format='png', bbox_inches='tight')
    buffer.seek(0)
    plt.close()
    
    return buffer

def populate_valuation_model(template_path, output_path, ticker_symbol, current_field_map, current_field_cell_map):
    """
    Populates the Excel valuation model with fetched data.
    """
    if not os.path.exists(template_path):
        print(f"❌ Oh no! The Excel template file '{template_path}' wasn't found. Please make sure it's in the same directory as the script or you've provided the full path.")
        raise FileNotFoundError(f"Template file {template_path} not found.")

    try:
        shutil.copy(template_path, output_path)
        print(f"📄 Copied the template '{template_path}' to your new output file '{output_path}'.")
    except Exception as e_copy:
        print(f"❌ Failed to copy the template to '{output_path}'. Error: {e_copy}")
        raise

    try:
        wb = openpyxl.load_workbook(output_path)
    except Exception as e_load:
        print(f"❌ Trouble opening the new Excel file '{output_path}'. Error: {e_load}")
        raise

    # Verify required sheets
    required_sheets = ["Inputs", "Summary", "Peers"]
    for sheet in required_sheets:
        if sheet not in wb.sheetnames:
            print(f"❌ The Excel template is missing the '{sheet}' sheet. I need that sheet to put the data in! Please check the template.")
            raise ValueError(f"'{sheet}' sheet not found in the template file.")

    ws_inputs = wb["Inputs"]
    ws_summary = wb["Summary"]
    ws_peers = wb["Peers"]

    data_years = list(range(2014, 2024 + 1))
    num_data_years = len(data_years)

    all_fetched_bdh_data = {}  # Stores BDH (Historical Data Request) data
    all_fetched_bds_data = {}  # Stores BDS (Bulk Data Service) data

    global_bberg_code_to_excel_name_map = {
        config["field"]: name
        for name, config in current_field_map.items()
        if config.get("source") in ["BDH", "BDS"] and "field" in config
    }

    # Separate BDH and BDS fields
    bdh_fields_to_fetch_codes = set()
    bds_fields_to_fetch_codes = set()
    for excel_name, config in current_field_map.items():
        if config.get("source") == "BDH" and "field" in config:
            bdh_fields_to_fetch_codes.add(config["field"])
        elif config.get("source") == "BDS" and "field" in config:
            bds_fields_to_fetch_codes.add(config["field"])
        elif config.get("source") == "derived":
            # Derived fields handled separately
            pass

    if not (bdh_fields_to_fetch_codes or bds_fields_to_fetch_codes):
        print("🤔 It seems no Bloomberg data fields are listed in the configuration. I can't fetch anything without them. Please check the 'field_map'.")
        wb.save(output_path)
        return

    print(f"\n🚀 Phase 1: Starting data hunt for ticker: {ticker_symbol}")
    print(f"📊 I need to find {len(bdh_fields_to_fetch_codes)} BDH and {len(bds_fields_to_fetch_codes)} BDS pieces of data from Bloomberg.")

    # Batch BDH fields
    bdh_field_batches = batch_fields(list(bdh_fields_to_fetch_codes), batch_size=25)
    print(f"📦 I've split BDH fields into {len(bdh_field_batches)} smaller batches to ask Bloomberg.")

    session = None
    try:
        session = setup_bloomberg_session(ticker_symbol)
        if not session:
            print(f"❌ Major setback: Failed to start the Bloomberg session for {ticker_symbol}. I can't fetch any data. Please check your Bloomberg Terminal connection.")
            raise ConnectionError("Failed to establish Bloomberg session.")

        # Fetch BDH Data
        for batch_idx, current_batch_bberg_codes in enumerate(bdh_field_batches):
            print(f"    🔎 BDH Batch {batch_idx + 1} of {len(bdh_field_batches)}: Asking for {len(current_batch_bberg_codes)} specific items.")

            batch_data_fetched = fetch_bloomberg_data(
                session,
                ticker_symbol,
                current_batch_bberg_codes,
                global_bberg_code_to_excel_name_map,
                start_year=data_years[0],
                end_year=data_years[-1]
            )

            if batch_data_fetched is None:
                print(f"    ❗ Critical Error: Something went wrong with the Bloomberg connection during BDH batch {batch_idx + 1}. Stopping further data fetching.")
                raise ConnectionAbortedError("Bloomberg session terminated or critical fetch error during a BDH batch.")

            elif batch_data_fetched:
                for field_code, yearly_data in batch_data_fetched.items():
                    if field_code not in all_fetched_bdh_data:
                        all_fetched_bdh_data[field_code] = {}
                    for year, value in yearly_data.items():
                        if value is not None:
                            all_fetched_bdh_data[field_code][year] = value
                        elif year not in all_fetched_bdh_data[field_code]:
                            all_fetched_bdh_data[field_code][year] = value

                print(f"    👍 Success! Got data for BDH batch {batch_idx + 1}. Processed {len(batch_data_fetched)} field types from this batch.")
            else:
                print(f"    ℹ️ BDH Batch {batch_idx + 1} didn't return any data. This could be because all fields in it were invalid or no data was available.")

        # Fetch BDS Data (Top Holders, Company Description)
        ref_data_service = session.getService("//blp/refdata")
        for bds_field in bds_fields_to_fetch_codes:
            request = ref_data_service.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(f"{ticker_symbol} Equity")
            request.getElement("fields").appendValue(bds_field)
            session.sendRequest(request)

            print(f"📡 Sending BDS request for {ticker_symbol}: {bds_field}")
            all_fetched_bds_data[bds_field] = []

            while True:
                event = session.nextEvent(500)
                for msg in event:
                    if msg.hasElement("securityData"):
                        security_data = msg.getElement("securityData").getValue(0)
                        if security_data.hasElement("fieldData"):
                            field_data = security_data.getElement("fieldData")
                            if field_data.hasElement(bds_field):
                                bulk_data = field_data.getElement(bds_field)
                                for i in range(bulk_data.numValues()):
                                    row = {}
                                    for element in bulk_data.getValue(i).elements():
                                        row[element.name().__str__()] = element.getValueAsString()
                                    all_fetched_bds_data[bds_field].append(row)
                if event.eventType() == blpapi.Event.RESPONSE:
                    print(f"📬 Received BDS response for {bds_field}")
                    break

    except ConnectionError as e_conn_err:
        print(f"❌ Connection Error: {e_conn_err}")
    except ConnectionAbortedError as e_conn_abort:
        print(f"❌ Connection Aborted: {e_conn_abort}")
    except Exception as e_fetch:
        print(f"❌ An unexpected error occurred while trying to get data from Bloomberg: {e_fetch}")

    finally:
        if session:
            try:
                session.stop()
                print("🔌 Bloomberg session stopped. All done with data fetching (or tried our best!).")
            except Exception as e_stop:
                print(f"⚠️ Minor issue while trying to stop the Bloomberg session: {e_stop}")

    print(f"\n🏁 Phase 1 Complete: Finished all attempts to fetch data from Bloomberg.")

    print(f"\n🧮 Phase 2: Calculating any extra metrics...")
    all_derived_data = calculate_derived_metrics(all_fetched_bdh_data, start_year=data_years[0], end_year=data_years[-1])
    print("✅ Derived metrics calculated (if any were defined).")

    print(f"\n✍️ Phase 3: Writing all the gathered data into your Excel sheets...")

    # Write BDH and Derived Data
    for excel_name, config in current_field_map.items():
        if excel_name.startswith("__dep_"):
            continue

        cell_info = current_field_cell_map.get(excel_name)
        if not cell_info:
            print(f"🤔 Couldn't find where to put '{excel_name}' in the Excel sheet (no cell mapping). Skipping this item.")
            continue

        sheet_name = cell_info["sheet"]
        base_cell_ref = cell_info["cell"]
        ws = wb[sheet_name]

        try:
            target_cells_for_item = get_target_cells_for_years(base_cell_ref, num_data_years)
        except Exception as e_cell_calc:
            print(f"❌ Error figuring out the cells for '{excel_name}' in sheet '{sheet_name}' (starting from '{base_cell_ref}'): {e_cell_calc}. Skipping this one.")
            continue

        data_source_for_item = {}
        source_type = config.get("source", "unknown").upper()

        if source_type == "BDH":
            bberg_field_code = config.get("field")
            if not bberg_field_code:
                print(f"🤔 The item '{excel_name}' is marked as Bloomberg data (BDH), but has no Bloomberg field code. Skipping.")
                continue
            data_source_for_item = all_fetched_bdh_data.get(bberg_field_code, {})
            if not data_source_for_item:
                print(f"💨 No data was fetched for '{excel_name}' (Bloomberg code: {bberg_field_code}). It will be marked N/A.")

        elif source_type == "DERIVED":
            derived_field_key = config.get("field")
            if not derived_field_key:
                print(f"🤔 The item '{excel_name}' is marked as 'derived', but I don't know which calculation it refers to. Skipping.")
                continue
            data_source_for_item = all_derived_data.get(derived_field_key, {})
            if not data_source_for_item:
                print(f"💨 No data was calculated for the derived metric '{excel_name}'. It will be marked N/A (likely due to missing inputs).")

        else:
            print(f"❓ Item '{excel_name}' has an unknown data source type: '{config.get('source', 'Not Specified')}'. Skipping.")
            continue

        for i, year in enumerate(data_years):
            cell_ref = target_cells_for_item[i]
            raw_value = data_source_for_item.get(year)

            display_value = raw_value

            if raw_value is None:
                display_value = "N/A (Missing)"

            try:
                if isinstance(raw_value, (int, float)):
                    ws[cell_ref] = raw_value
                    ws[cell_ref].number_format = "#,##0.000"
                    if excel_name in ["Insider Ownership Percent", "Institutional Ownership Percent", "Short Interest Percent"]:
                        ws[cell_ref].number_format = "0.00%"
                    elif "EPS" in excel_name:
                        ws[cell_ref].number_format = "0.00"
                elif isinstance(raw_value, str):
                    ws[cell_ref] = raw_value
                else:
                    ws[cell_ref] = "0" if raw_value is None else str(raw_value)
            except Exception as e_write_cell:
                print(f"⚠️ Problem writing to cell {cell_ref} in sheet '{sheet_name}' for '{excel_name}': {e_write_cell}")
                ws[cell_ref] = "Error writing"

    # Write BDS Data (Top Holders, Company Description)
    # Major Holders (Summary, U38:V38 to U46:V46, merged cells)
    if "OWN_TOP_HOLDERS" in all_fetched_bds_data and all_fetched_bds_data["OWN_TOP_HOLDERS"]:
        holders = all_fetched_bds_data["OWN_TOP_HOLDERS"]
        for i, holder in enumerate(holders[:9]):  # Take up to 9 holders
            cell = f"U{38+i}"
            ws_summary[cell] = f"{holder.get('Holder Name', 'N/A')} ({holder.get('Percent of Shares Outstanding', 'N/A')}%)"
            # Ensure merged cell formatting
            ws_summary.merged_cells.add(f"U{38+i}:V{38+i}")
        for i in range(len(holders), 9):  # Fill remaining cells with N/A
            cell = f"U{38+i}"
            ws_summary[cell] = "N/A (No holder data)"
            ws_summary.merged_cells.add(f"U{38+i}:V{38+i}")
    else:
        for i in range(9):
            cell = f"U{38+i}"
            ws_summary[cell] = "N/A (No holder data)"
            ws_summary.merged_cells.add(f"U{38+i}:V{38+i}")
        print("⚠️ No major holders data available. Marked as N/A in cells U38:V46 in 'Summary'.")

    # Company Description (Summary, D11)
    if "COMPANY_DESCRIPTION" in all_fetched_bds_data and all_fetched_bds_data["COMPANY_DESCRIPTION"]:
        description = all_fetched_bds_data["COMPANY_DESCRIPTION"][0].get("Description", "N/A")
        ws_summary["D11"] = description
    else:
        ws_summary["D11"] = "N/A (No description)"
        print("⚠️ No company description available. Marked as N/A in cell D11 in 'Summary'.")

    # Generate and Embed Stock Price Chart (Summary, O65)
    chart_buffer = generate_chart(all_fetched_bdh_data, ticker_symbol)
    if chart_buffer:
        img = Image(chart_buffer)
        ws_summary.add_image(img, "O65")
        print("📊 Stock price chart embedded in cell O65 in 'Summary'.")
    else:
        ws_summary["O65"] = "N/A (No chart generated)"
        print("⚠️ Failed to generate stock price chart. Marked as N/A in cell O65 in 'Summary'.")

    try:
        wb.save(output_path)
        print(f"\n🎉 All Done! Your valuation model has been populated and saved to: '{output_path}'")
    except Exception as e_save:
        print(f"❌ Critical Error: Failed to save the final Excel workbook to '{output_path}'. Error: {e_save}")
        print("   Possible reasons: The file might be open in Excel, or there might be a permissions issue with the folder.")

if __name__ == "__main__":
    print("-" * 70)
    print(" ✨ Bloomberg Data to Excel Valuation Model Populator ✨ ")
    print("-" * 70)

    excel_template_path = "LIS_Valuation_Empty.xlsx"

    ticker_input = input("\n🔍 Please enter the stock ticker (e.g., 'AAPL US' or '000660 KS'): ").strip().upper()
    if not ticker_input:
        print("❌ You didn't enter a ticker. I need one to proceed. Exiting.")
        exit(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.expanduser("~/Desktop/Bloomberg_Valuation_Reports")
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"{ticker_input.replace(' ', '_')}_Valuation_Model_{timestamp}.xlsx"
    output_path = os.path.join(output_dir, output_filename)

    print(f"\n📝 I'll save the populated valuation model to: {output_path}")
    print(f"📑 Using the Excel template: {excel_template_path}")

    try:
        populate_valuation_model(
            template_path=excel_template_path,
            output_path=output_path,
            ticker_symbol=ticker_input,
            current_field_map=field_map,
            current_field_cell_map=field_cell_map
        )
    except FileNotFoundError as e_fnf:
        print(f"❌ File Not Found Error: {e_fnf}")
    except ValueError as e_val:
        print(f"❌ Value Error: {e_val}")
    except ConnectionError as e_conn:
        print(f"❌ Connection Error: {e_conn}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        print("\n🏁 Script execution completed (successfully or with errors). Check the messages above for details.")
