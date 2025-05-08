import blpapi
import openpyxl
import shutil
import os
import numpy as np
import logging
import argparse
import pickle
import json
from datetime import datetime, timedelta
from tqdm import tqdm
from pathlib import Path

# Configure logging
def setup_logging():
    logger = logging.getLogger("bloomberg_valuation")
    logger.setLevel(logging.INFO)
    
    # Create directory for logs
    os.makedirs("logs", exist_ok=True)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # File handler with timestamp
    log_file = f"logs/bloomberg_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    
    # Format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    
    logger.addHandler(ch)
    logger.addHandler(fh)
    
    return logger

# Load configuration
def load_config(config_path=None):
    default_config = {
        "bloomberg_host": "localhost",
        "bloomberg_port": 8194,
        "start_year": 2014,
        "end_year": 2024,
        "template_path": "LIS_Valuation_Empty.xlsx",
        "cache_dir": "cache",
        "cache_max_age_days": 1
    }
    
    if config_path and os.path.exists(config_path):
        with open(config_path, 'r') as f:
            user_config = json.load(f)
            default_config.update(user_config)
    
    return default_config

# Bloomberg API setup with error handling
def setup_bloomberg_session(host, port, max_retries=3):
    logger = logging.getLogger("bloomberg_valuation")
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to Bloomberg API at {host}:{port} (attempt {attempt+1}/{max_retries})")
            options = blpapi.SessionOptions()
            options.setServerHost(host)
            options.setServerPort(port)
            session = blpapi.Session(options)
            
            if not session.start():
                logger.error("Failed to start Bloomberg session")
                continue
                
            if not session.openService("//blp/refdata"):
                logger.error("Failed to open Bloomberg reference data service")
                session.stop()
                continue
                
            logger.info("Successfully connected to Bloomberg API")
            return session
        except Exception as e:
            logger.error(f"Error connecting to Bloomberg: {e}")
            if attempt == max_retries - 1:
                raise
            logger.info(f"Retrying in 5 seconds...")
            import time
            time.sleep(5)
    
    return None

# Cache management
def get_cached_data(ticker, start_year, end_year, cache_dir, max_age_days=1):
    logger = logging.getLogger("bloomberg_valuation")
    cache_file = os.path.join(cache_dir, f"{ticker}_{start_year}_{end_year}_data.pkl")
    
    # Check if cache exists and is recent
    if os.path.exists(cache_file):
        modified_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if datetime.now() - modified_time < timedelta(days=max_age_days):
            logger.info(f"Using cached data for {ticker} from {modified_time}")
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
    
    logger.info(f"No valid cache found for {ticker}")
    return None

def cache_data(ticker, start_year, end_year, bdh_data, derived_data, cache_dir):
    logger = logging.getLogger("bloomberg_valuation")
    os.makedirs(cache_dir, exist_ok=True)
    
    data = {
        "bdh_data": bdh_data,
        "derived_data": derived_data,
        "cached_at": datetime.now().isoformat()
    }
    
    cache_file = os.path.join(cache_dir, f"{ticker}_{start_year}_{end_year}_data.pkl")
    with open(cache_file, 'wb') as f:
        pickle.dump(data, f)
    
    logger.info(f"Data cached for {ticker} at {cache_file}")

# Fetch historical data (BDH) with proper error handling
def fetch_bloomberg_data(session, ticker, fields, start_year, end_year, timeout=60000):
    logger = logging.getLogger("bloomberg_valuation")
    
    try:
        logger.info(f"Fetching Bloomberg data for {ticker} ({len(fields)} fields)")
        ref_data_service = session.getService("//blp/refdata")
        request = ref_data_service.createRequest("HistoricalDataRequest")
        security = f"{ticker} US Equity"
        
        request.getElement("securities").appendValue(security)
        for field in fields:
            request.getElement("fields").appendValue(field)
        
        request.set("periodicitySelection", "YEARLY")
        request.set("startDate", f"{start_year}0101")
        request.set("endDate", f"{end_year}1231")
        
        logger.debug(f"Sending request for {ticker}")
        session.sendRequest(request)
        
        data = {field: {} for field in fields}
        
        start_time = datetime.now()
        while True:
            # Check for timeout
            if (datetime.now() - start_time).total_seconds() * 1000 > timeout:
                logger.error(f"Request timed out after {timeout/1000} seconds")
                raise TimeoutError(f"Bloomberg API request timed out")
            
            event = session.nextEvent(500)  # 500ms timeout for next event
            
            if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in event:
                    if msg.hasElement("securityData"):
                        security_data = msg.getElement("securityData")
                        
                        # Check for security errors
                        if security_data.hasElement("securityError"):
                            security_error = security_data.getElement("securityError")
                            error_msg = security_error.getElementAsString("message")
                            logger.error(f"Security error for {ticker}: {error_msg}")
                            continue
                        
                        if security_data.hasElement("fieldData"):
                            field_data = security_data.getElement("fieldData")
                            
                            for i in range(field_data.numValues()):
                                datum = field_data.getValue(i)
                                
                                if datum.hasElement("date"):
                                    date = datum.getElement("date").getValue()
                                    year = date.year
                                    
                                    for field in fields:
                                        if datum.hasElement(field):
                                            try:
                                                value = datum.getElement(field).getValue()
                                                data[field][year] = value
                                            except Exception as e:
                                                logger.warning(f"Error extracting {field} for {year}: {e}")
                                                data[field][year] = None
                                else:
                                    logger.warning("Missing date element in response")
            
            if event.eventType() == blpapi.Event.RESPONSE:
                break
        
        logger.info(f"Successfully fetched data for {ticker}")
        return data
    
    except Exception as e:
        logger.error(f"Error fetching Bloomberg data: {e}")
        raise

# Validate and clean the data
def validate_and_clean_data(data):
    logger = logging.getLogger("bloomberg_valuation")
    logger.info("Validating and cleaning data")
    
    cleaned_data = {}
    for field, values in data.items():
        cleaned_data[field] = {}
        for year, value in values.items():
            # Handle null/None values
            if value is None:
                logger.debug(f"Null value found for {field} in {year}, replacing with 0")
                cleaned_data[field][year] = 0
                continue
            
            # Handle unexpected data types
            try:
                cleaned_data[field][year] = float(value)
            except (ValueError, TypeError):
                logger.warning(f"Invalid value for {field} in {year}: {value}, replacing with 0")
                cleaned_data[field][year] = 0
    
    logger.info("Data validation complete")
    return cleaned_data

# Calculate derived metrics with robustness
def calculate_derived_metrics(data, start_year, end_year):
    logger = logging.getLogger("bloomberg_valuation")
    logger.info("Calculating derived metrics")
    
    derived = {
        "Changes in Net Working Capital": {},
        "DSO": {},
        "DIH": {},
        "DPO": {},
        "Net Cash from Investments & Acquisitions": {}
    }
    
    for year in range(start_year + 1, end_year + 1):  # Start from second year for NWC changes
        try:
            # Changes in Net Working Capital
            if all(k in data and year in data[k] and year-1 in data[k] 
                   for k in ["TOT_CUR_ASSETS", "TOT_CUR_LIAB"]):
                nwc_t = data["TOT_CUR_ASSETS"][year] - data["TOT_CUR_LIAB"][year]
                nwc_t1 = data["TOT_CUR_ASSETS"][year - 1] - data["TOT_CUR_LIAB"][year - 1]
                derived["Changes in Net Working Capital"][year] = nwc_t - nwc_t1
            else:
                logger.debug(f"Missing data for NWC calculation in {year}")
            
            # DSO, DIH, DPO
            if all(k in data and year in data[k] for k in ["ACCT_RCV", "SALES_REV_TURN", 
                                                          "INVENTORIES", "COGS", "ACCT_PAYABLE"]):
                revenue = data["SALES_REV_TURN"][year]
                cogs = data["COGS"][year]
                ar = data["ACCT_RCV"][year]
                inv = data["INVENTORIES"][year]
                ap = data["ACCT_PAYABLE"][year]
                
                # Avoid division by zero
                derived["DSO"][year] = (ar / revenue * 365) if revenue and revenue != 0 else 0
                derived["DIH"][year] = (inv / cogs * 365) if cogs and cogs != 0 else 0
                derived["DPO"][year] = (ap / cogs * 365) if cogs and cogs != 0 else 0
            else:
                logger.debug(f"Missing data for DSO/DIH/DPO calculation in {year}")
            
            # Net Cash from Investments & Acquisitions
            if all(k in data and year in data[k] for k in ["CF_ACQUISITIONS", "CF_DISPOSALS", "CF_OTHER_INVEST_ACT"]):
                derived["Net Cash from Investments & Acquisitions"][year] = (
                    data["CF_ACQUISITIONS"][year] +
                    data["CF_DISPOSALS"][year] +
                    data["CF_OTHER_INVEST_ACT"][year]
                )
            else:
                logger.debug(f"Missing data for Net Cash from Investments & Acquisitions in {year}")
        
        except Exception as e:
            logger.error(f"Error calculating derived metrics for {year}: {e}")
    
    logger.info("Derived metrics calculation complete")
    return derived

# Calculate CAGR with error handling
def calculate_cagr(start_value, end_value, years):
    if years <= 0:
        return 0
    
    try:
        if start_value == 0 or end_value == 0:
            return 0
        return ((end_value / start_value) ** (1 / years) - 1) * 100
    except Exception:
        return 0

# Populate Excel with data
def populate_excel(template_path, output_path, ticker, field_map, bdh_data, derived_data, start_year, end_year):
    logger = logging.getLogger("bloomberg_valuation")
    
    try:
        # Create a copy of the template
        logger.info(f"Creating copy of template: {template_path}")
        shutil.copy(template_path, output_path)
        
        # Load the workbook
        logger.info(f"Loading workbook: {output_path}")
        wb = openpyxl.load_workbook(output_path)
        
        # Check if Inputs sheet exists
        if "Inputs" not in wb.sheetnames:
            logger.error("Inputs sheet not found in template")
            raise ValueError("Template does not contain 'Inputs' sheet")
        
        ws = wb["Inputs"]
        
        # Map of row labels to row numbers
        logger.info("Mapping row labels to row numbers")
        row_map = {}
        for row in range(1, ws.max_row + 1):
            cell_value = ws[f"A{row}"].value
            if cell_value in field_map:
                row_map[cell_value] = row
        
        # Generate year columns
        year_columns = {year: chr(ord('A') + (year - start_year + 1)) for year in range(start_year, end_year + 1)}
        cagr_column = chr(ord('A') + (end_year - start_year + 2))
        
        # Populate data
        logger.info("Populating data into Excel")
        for field, config in tqdm(field_map.items(), desc="Processing fields"):
            if field not in row_map:
                logger.warning(f"Field {field} not found in Inputs sheet")
                continue
                
            row = row_map[field]
            
            # Select data source
            if config["source"] == "BDH":
                values = bdh_data.get(config["field"], {})
                for year, col in year_columns.items():
                    if year in values:
                        try:
                            # Convert to millions and handle potential errors
                            value = values[year]
                            if value is not None:
                                ws[f"{col}{row}"] = value / 1000  # Convert to millions
                        except Exception as e:
                            logger.warning(f"Error setting value for {field} in {year}: {e}")
                
                # Calculate CAGR
                start_value = values.get(start_year, 0)
                end_value = values.get(end_year, 0)
                if start_value and end_value:
                    cagr = calculate_cagr(start_value, end_value, end_year - start_year)
                    ws[f"{cagr_column}{row}"] = cagr / 100
            
            elif config["source"] == "derived":
                values = derived_data[config["field"]]
                for year, col in year_columns.items():
                    if year in values:
                        try:
                            ws[f"{col}{row}"] = values[year]
                        except Exception as e:
                            logger.warning(f"Error setting derived value for {field} in {year}: {e}")
                
                # For CAGR of derived metrics, use different approach
                years_with_data = [year for year in range(start_year, end_year + 1) if year in values]
                if len(years_with_data) >= 2:
                    first_year = min(years_with_data)
                    last_year = max(years_with_data)
                    start_value = values.get(first_year, 0)
                    end_value = values.get(last_year, 0)
                    if start_value and end_value:
                        cagr = calculate_cagr(start_value, end_value, last_year - first_year)
                        ws[f"{cagr_column}{row}"] = cagr / 100
        
        # Add metadata
        metadata_row = ws.max_row + 2
        ws[f"A{metadata_row}"] = "Metadata"
        ws[f"A{metadata_row+1}"] = "Ticker"
        ws[f"B{metadata_row+1}"] = ticker
        ws[f"A{metadata_row+2}"] = "Generated On"
        ws[f"B{metadata_row+2}"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws[f"A{metadata_row+3}"] = "Data Range"
        ws[f"B{metadata_row+3}"] = f"{start_year}-{end_year}"
        
        # Save the workbook
        logger.info(f"Saving workbook to {output_path}")
        wb.save(output_path)
        
        return True
    
    except Exception as e:
        logger.error(f"Error populating Excel: {e}")
        return False

# Main function
def populate_valuation_model(config, ticker):
    logger = logging.getLogger("bloomberg_valuation")
    logger.info(f"Starting valuation model population for {ticker}")
    
    start_year = config["start_year"]
    end_year = config["end_year"]
    
    # Create output directory
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create cache directory
    os.makedirs(config["cache_dir"], exist_ok=True)
    
    # Define output path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"{ticker}_valuation_model_{timestamp}.xlsx")
    
    # Import field map here to avoid defining it multiple times
    from field_map import field_map
    
    # Check for cached data
    cached_data = get_cached_data(ticker, start_year, end_year, 
                                 config["cache_dir"], config["cache_max_age_days"])
    
    if cached_data:
        logger.info("Using cached data")
        bdh_data = cached_data["bdh_data"]
        derived_data = cached_data["derived_data"]
    else:
        # Setup Bloomberg session
        session = setup_bloomberg_session(config["bloomberg_host"], config["bloomberg_port"])
        if not session:
            logger.error("Failed to create Bloomberg session")
            return False
        
        try:
            # Get fields to fetch from field_map
            bdh_fields = [v["field"] for k, v in field_map.items() if v["source"] == "BDH"]
            
            # Fetch BDH data
            bdh_data = fetch_bloomberg_data(session, ticker, bdh_fields, start_year, end_year)
            
            # Validate and clean data
            bdh_data = validate_and_clean_data(bdh_data)
            
            # Calculate derived metrics
            derived_data = calculate_derived_metrics(bdh_data, start_year, end_year)
            
            # Cache the data
            cache_data(ticker, start_year, end_year, bdh_data, derived_data, config["cache_dir"])
        
        except Exception as e:
            logger.error(f"Error processing Bloomberg data: {e}")
            return False
        finally:
            # Close Bloomberg session
            logger.info("Closing Bloomberg session")
            session.stop()
    
    # Populate Excel with data
    success = populate_excel(
        config["template_path"], 
        output_path, 
        ticker, 
        field_map, 
        bdh_data, 
        derived_data,
        start_year,
        end_year
    )
    
    if success:
        logger.info(f"Valuation model successfully saved as {output_path}")
        return True
    else:
        logger.error("Failed to populate valuation model")
        return False

# Command line interface
def parse_arguments():
    parser = argparse.ArgumentParser(description='Bloomberg Valuation Model Data Extractor')
    parser.add_argument('ticker', help='Ticker symbol (e.g., AAPL)')
    parser.add_argument('--template', help='Path to template Excel file')
    parser.add_argument('--config', help='Path to configuration JSON file')
    parser.add_argument('--start-year', type=int, help='Start year for historical data')
    parser.add_argument('--end-year', type=int, help='End year for historical data')
    parser.add_argument('--no-cache', action='store_true', help='Ignore cached data')
    return parser.parse_args()

# Entry point
if __name__ == "__main__":
    # Setup logging
    logger = setup_logging()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config(args.config)
    
    # Override config with command line arguments
    if args.template:
        config["template_path"] = args.template
    if args.start_year:
        config["start_year"] = args.start_year
    if args.end_year:
        config["end_year"] = args.end_year
    if args.no_cache:
        config["cache_max_age_days"] = -1  # Disable cache
    
    # Run the program
    success = populate_valuation_model(config, args.ticker)
    
    if success:
        logger.info("Program completed successfully")
    else:
        logger.error("Program failed")
        import sys
        sys.exit(1)
