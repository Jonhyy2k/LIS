import os
import sys
import json
import numpy as np
import pandas as pd
import  yfinance as yf
import requests
from datetime import datetime
from scipy.stats import linregress
import matplotlib.pyplot as plt


class DCFModel:
    def __init__(self, ticker):
        """
        Initialize the DCF model with the company ticker
        """
        self.ticker = ticker.upper()
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.data = {}
        self.projections = {}
        self.dcf_inputs = {}
        self.dcf_results = {}
        self.company_info = {}

        # Default DCF parameters - these can be modified
        self.dcf_params = {
            'projection_years': 5,
            'terminal_growth_rate': 0.02,
            'risk_free_rate': 0.04,  # 10-year Treasury yield
            'market_risk_premium': 0.055,  # Equity risk premium
            'capex_to_revenue': 0.02,  # CAPEX as % of revenue
            'working_capital_to_revenue_change': 0.26,  # WC change as % of revenue change
            'acquisition_to_revenue': 0.02,  # Acquisition as % of revenue
            'dividend_payout': 0.30,  # Dividend payout ratio
            'share_repurchases': 0.25,  # Share repurchase as % of net income
        }

        # Initialize with data
        self.download_data()

    def download_data(self):
        """
        Download stock data, financial statements, and company information
        """
        print(f"Retrieving data for {self.ticker}...")

        # Get stock data
        try:
            self.stock = yf.Ticker(self.ticker)

            # Get company info
            self.company_info = {
                'name': self.stock.info.get('shortName', self.ticker),
                'sector': self.stock.info.get('sector', 'Unknown'),
                'industry': self.stock.info.get('industry', 'Unknown'),
                'description': self.stock.info.get('longBusinessSummary', 'No description available'),
                'country': self.stock.info.get('country', 'Unknown'),
                'employees': self.stock.info.get('fullTimeEmployees', 0),
                'market_cap': self.stock.info.get('marketCap', 0),
                'shares_outstanding': self.stock.info.get('sharesOutstanding', 0),
                'current_price': self.stock.info.get('currentPrice', 0),
                'currency': self.stock.info.get('currency', 'USD')
            }

            # Get financial statements
            self.income_stmt = self.stock.income_stmt
            self.balance_sheet = self.stock.balance_sheet
            self.cash_flow = self.stock.cashflow

            # Convert to pandas DataFrames
            if len(self.income_stmt) > 0:
                self.income_stmt = self.income_stmt.fillna(0)
            else:
                print("Warning: Income statement data not available.")

            if len(self.balance_sheet) > 0:
                self.balance_sheet = self.balance_sheet.fillna(0)
            else:
                print("Warning: Balance sheet data not available.")

            if len(self.cash_flow) > 0:
                self.cash_flow = self.cash_flow.fillna(0)
            else:
                print("Warning: Cash flow data not available.")

            # Store historical price data
            self.price_history = self.stock.history(period="5y")

            # Get Beta from Yahoo Finance
            self.beta = self.stock.info.get('beta', 1.0)
            if self.beta is None or self.beta == 0:
                self.beta = 1.0

            print(f"Data retrieved successfully for {self.ticker}")

        except Exception as e:
            print(f"Error downloading data for {self.ticker}: {str(e)}")
            raise

    def calculate_historical_metrics(self):
        """
        Calculate historical financial metrics from the financial statements
        """
        print(f"Calculating historical metrics for {self.ticker}...")

        # Historical years (most recent 3 years)
        self.historical_years = []
        if len(self.income_stmt.columns) >= 3:
            self.historical_years = self.income_stmt.columns[:3]
        else:
            self.historical_years = self.income_stmt.columns

        # Initialize historical metrics dictionary
        self.historical = {year.strftime('%Y'): {} for year in self.historical_years}

        for year in self.historical_years:
            year_str = year.strftime('%Y')

            # Revenue
            if 'Total Revenue' in self.income_stmt.index:
                self.historical[year_str]['revenue'] = float(self.income_stmt.loc['Total Revenue', year])
            else:
                # Try alternative revenue field names
                revenue_fields = ['Revenue', 'Total Revenue', 'Sales', 'Total Sales']
                for field in revenue_fields:
                    if field in self.income_stmt.index:
                        self.historical[year_str]['revenue'] = float(self.income_stmt.loc[field, year])
                        break
                else:
                    self.historical[year_str]['revenue'] = 0
                    print(f"Warning: Revenue not found for {year_str}")

            # EBIT (Operating Income)
            if 'Operating Income' in self.income_stmt.index:
                self.historical[year_str]['ebit'] = float(self.income_stmt.loc['Operating Income', year])
            elif 'EBIT' in self.income_stmt.index:
                self.historical[year_str]['ebit'] = float(self.income_stmt.loc['EBIT', year])
            else:
                # Calculate EBIT from Gross Profit - SG&A and R&D
                if 'Gross Profit' in self.income_stmt.index:
                    gross_profit = float(self.income_stmt.loc['Gross Profit', year])
                    sga = 0
                    rd = 0

                    if 'Selling General Administrative' in self.income_stmt.index:
                        sga = float(self.income_stmt.loc['Selling General Administrative', year])

                    if 'Research Development' in self.income_stmt.index:
                        rd = float(self.income_stmt.loc['Research Development', year])

                    self.historical[year_str]['ebit'] = gross_profit - sga - rd
                else:
                    self.historical[year_str]['ebit'] = 0
                    print(f"Warning: EBIT not found for {year_str}")

            # Net Income
            if 'Net Income' in self.income_stmt.index:
                self.historical[year_str]['net_income'] = float(self.income_stmt.loc['Net Income', year])
            else:
                self.historical[year_str]['net_income'] = 0
                print(f"Warning: Net Income not found for {year_str}")

            # Tax Rate
            if self.historical[year_str]['ebit'] != 0:
                if 'Tax Provision' in self.income_stmt.index:
                    tax_expense = float(self.income_stmt.loc['Tax Provision', year])
                    pretax_income = float(self.income_stmt.loc[
                                              'Income Before Tax', year]) if 'Income Before Tax' in self.income_stmt.index else \
                    self.historical[year_str]['ebit']

                    # Avoid division by zero
                    if pretax_income != 0:
                        self.historical[year_str]['tax_rate'] = tax_expense / pretax_income
                    else:
                        self.historical[year_str]['tax_rate'] = 0.25  # Default to 25% if pretax income is zero
                else:
                    self.historical[year_str]['tax_rate'] = 0.25  # Default to 25%
            else:
                self.historical[year_str]['tax_rate'] = 0.25  # Default rate if EBIT is zero

            # Capital Expenditures
            if 'Capital Expenditure' in self.cash_flow.index:
                # Capex is usually negative in cash flow, so take absolute value
                self.historical[year_str]['capex'] = abs(float(self.cash_flow.loc['Capital Expenditure', year]))
            else:
                self.historical[year_str]['capex'] = 0
                print(f"Warning: Capital Expenditure not found for {year_str}")

            # Depreciation & Amortization
            if 'Depreciation' in self.cash_flow.index:
                self.historical[year_str]['depreciation'] = float(self.cash_flow.loc['Depreciation', year])
            elif 'Depreciation And Amortization' in self.cash_flow.index:
                self.historical[year_str]['depreciation'] = float(
                    self.cash_flow.loc['Depreciation And Amortization', year])
            else:
                self.historical[year_str]['depreciation'] = 0
                print(f"Warning: Depreciation not found for {year_str}")

            # Working Capital
            # Calculate working capital: Current Assets - Current Liabilities
            if 'Total Current Assets' in self.balance_sheet.index and 'Total Current Liabilities' in self.balance_sheet.index:
                self.historical[year_str]['working_capital'] = float(
                    self.balance_sheet.loc['Total Current Assets', year]) - float(
                    self.balance_sheet.loc['Total Current Liabilities', year])
            else:
                self.historical[year_str]['working_capital'] = 0
                print(f"Warning: Working Capital components not found for {year_str}")

            # Total Debt
            debt = 0
            if 'Long Term Debt' in self.balance_sheet.index:
                debt += float(self.balance_sheet.loc['Long Term Debt', year])

            if 'Short Long Term Debt' in self.balance_sheet.index:
                debt += float(self.balance_sheet.loc['Short Long Term Debt', year])

            self.historical[year_str]['total_debt'] = debt

            # Cash and Cash Equivalents
            if 'Cash And Cash Equivalents' in self.balance_sheet.index:
                self.historical[year_str]['cash'] = float(self.balance_sheet.loc['Cash And Cash Equivalents', year])
            elif 'Cash' in self.balance_sheet.index:
                self.historical[year_str]['cash'] = float(self.balance_sheet.loc['Cash', year])
            else:
                self.historical[year_str]['cash'] = 0
                print(f"Warning: Cash not found for {year_str}")

            # Calculate ratios
            if self.historical[year_str]['revenue'] != 0:
                self.historical[year_str]['ebit_margin'] = self.historical[year_str]['ebit'] / \
                                                           self.historical[year_str]['revenue']
                self.historical[year_str]['capex_to_revenue'] = self.historical[year_str]['capex'] / \
                                                                self.historical[year_str]['revenue']
                self.historical[year_str]['depreciation_to_revenue'] = self.historical[year_str]['depreciation'] / \
                                                                       self.historical[year_str]['revenue']
            else:
                self.historical[year_str]['ebit_margin'] = 0
                self.historical[year_str]['capex_to_revenue'] = 0
                self.historical[year_str]['depreciation_to_revenue'] = 0

        # Calculate year-over-year growth rates
        years_list = list(self.historical.keys())
        for i in range(1, len(years_list)):
            current_year = years_list[i]
            prev_year = years_list[i - 1]

            # Revenue Growth
            if self.historical[prev_year]['revenue'] != 0:
                self.historical[current_year]['revenue_growth'] = (
                        self.historical[current_year]['revenue'] / self.historical[prev_year]['revenue'] - 1
                )
            else:
                self.historical[current_year]['revenue_growth'] = 0

            # Working Capital Change
            wc_change = self.historical[current_year]['working_capital'] - self.historical[prev_year]['working_capital']
            revenue_change = self.historical[current_year]['revenue'] - self.historical[prev_year]['revenue']

            if revenue_change != 0:
                self.historical[current_year]['wc_to_revenue_change'] = wc_change / revenue_change
            else:
                self.historical[current_year]['wc_to_revenue_change'] = 0

        print(f"Historical metrics calculated for {self.ticker}")

    def generate_projections(self):
        """
        Generate financial projections for the DCF model
        """
        print(f"Generating financial projections for {self.ticker}...")

        # Calculate average historical metrics
        avg_metrics = {}

        years_list = list(self.historical.keys())

        # Revenue growth rate (use median to avoid outliers)
        growth_rates = [self.historical[year].get('revenue_growth', 0) for year in years_list[1:]]
        avg_metrics['revenue_growth'] = np.median(
            [rate for rate in growth_rates if rate != 0]) if growth_rates else 0.03

        # Clamp revenue growth to reasonable range
        avg_metrics['revenue_growth'] = max(0.01, min(0.15, avg_metrics['revenue_growth']))

        # EBIT margin (use median to avoid outliers)
        ebit_margins = [self.historical[year].get('ebit_margin', 0) for year in years_list]
        avg_metrics['ebit_margin'] = np.median(
            [margin for margin in ebit_margins if margin != 0]) if ebit_margins else 0.1

        # Capital expenditure ratio
        capex_ratios = [self.historical[year].get('capex_to_revenue', 0) for year in years_list]
        avg_metrics['capex_to_revenue'] = np.median(
            [ratio for ratio in capex_ratios if ratio != 0]) if capex_ratios else self.dcf_params['capex_to_revenue']

        # Depreciation ratio
        depreciation_ratios = [self.historical[year].get('depreciation_to_revenue', 0) for year in years_list]
        avg_metrics['depreciation_to_revenue'] = np.median(
            [ratio for ratio in depreciation_ratios if ratio != 0]) if depreciation_ratios else 0.03

        # Working capital to revenue change
        wc_ratios = [self.historical[year].get('wc_to_revenue_change', 0) for year in years_list[1:]]
        avg_metrics['wc_to_revenue_change'] = np.median(
            [ratio for ratio in wc_ratios if ratio != 0 and abs(ratio) < 1]) if wc_ratios else self.dcf_params[
            'working_capital_to_revenue_change']

        # Tax rate (use average of last 3 years)
        tax_rates = [self.historical[year].get('tax_rate', 0) for year in years_list]
        avg_metrics['tax_rate'] = np.mean([rate for rate in tax_rates if 0 < rate < 0.5]) if tax_rates else 0.25

        # Project financials for future years
        current_year = int(max(years_list))
        self.projection_years = [str(current_year + i + 1) for i in range(self.dcf_params['projection_years'])]

        # Initialize projections dictionary
        self.projections = {year: {} for year in self.projection_years}

        # Get the most recent historical values
        latest_year = max(years_list)
        latest_revenue = self.historical[latest_year]['revenue']
        latest_working_capital = self.historical[latest_year]['working_capital']

        # Project financials year by year
        for i, year in enumerate(self.projection_years):
            # Revenue growth gradually trends toward terminal growth rate
            if i == 0:
                # First year growth based on historical average
                growth_rate = avg_metrics['revenue_growth']
            else:
                # Blend historical growth with terminal growth (linear interpolation)
                weight = i / (len(self.projection_years) - 1) if len(self.projection_years) > 1 else 1
                growth_rate = avg_metrics['revenue_growth'] * (1 - weight) + self.dcf_params[
                    'terminal_growth_rate'] * weight

            # Project revenue
            if i == 0:
                self.projections[year]['revenue'] = latest_revenue * (1 + growth_rate)
            else:
                prev_year = self.projection_years[i - 1]
                self.projections[year]['revenue'] = self.projections[prev_year]['revenue'] * (1 + growth_rate)

            # Store growth rate
            self.projections[year]['revenue_growth'] = growth_rate

            # Project EBIT
            self.projections[year]['ebit_margin'] = avg_metrics['ebit_margin']
            self.projections[year]['ebit'] = self.projections[year]['revenue'] * self.projections[year]['ebit_margin']

            # Project depreciation
            self.projections[year]['depreciation'] = self.projections[year]['revenue'] * avg_metrics[
                'depreciation_to_revenue']

            # Project CAPEX
            self.projections[year]['capex'] = self.projections[year]['revenue'] * avg_metrics['capex_to_revenue']

            # Project working capital change
            if i == 0:
                revenue_change = self.projections[year]['revenue'] - latest_revenue
            else:
                prev_year = self.projection_years[i - 1]
                revenue_change = self.projections[year]['revenue'] - self.projections[prev_year]['revenue']

            self.projections[year]['working_capital_change'] = revenue_change * avg_metrics['wc_to_revenue_change']

            # Project taxes
            self.projections[year]['tax_rate'] = avg_metrics['tax_rate']
            self.projections[year]['taxes'] = self.projections[year]['ebit'] * self.projections[year]['tax_rate']

            # Calculate NOPAT (Net Operating Profit After Tax)
            self.projections[year]['nopat'] = self.projections[year]['ebit'] - self.projections[year]['taxes']

            # Calculate Free Cash Flow
            self.projections[year]['fcf'] = (
                    self.projections[year]['nopat'] +
                    self.projections[year]['depreciation'] -
                    self.projections[year]['capex'] -
                    self.projections[year]['working_capital_change']
            )

        print(f"Financial projections completed for {self.ticker}")

    def calculate_wacc(self):
        """
        Calculate the Weighted Average Cost of Capital (WACC)
        """
        print(f"Calculating WACC for {self.ticker}...")

        # Calculate cost of equity using CAPM
        cost_of_equity = self.dcf_params['risk_free_rate'] + self.beta * self.dcf_params['market_risk_premium']

        # Get latest financial data
        years_list = list(self.historical.keys())
        latest_year = max(years_list)

        total_debt = self.historical[latest_year]['total_debt']

        # Calculate market value of equity
        market_value_equity = self.company_info['market_cap']

        # Calculate total capital
        total_capital = market_value_equity + total_debt

        # Calculate weights
        if total_capital > 0:
            weight_equity = market_value_equity / total_capital
            weight_debt = total_debt / total_capital
        else:
            weight_equity = 1.0
            weight_debt = 0.0

        # Estimate cost of debt
        # Use a simple model: risk_free_rate + credit spread based on company's profitability
        latest_ebit_margin = self.historical[latest_year]['ebit_margin']

        # Credit spread estimation based on EBIT margin
        if latest_ebit_margin > 0.2:
            credit_spread = 0.01  # 100 bps for highly profitable companies
        elif latest_ebit_margin > 0.1:
            credit_spread = 0.02  # 200 bps for moderately profitable companies
        elif latest_ebit_margin > 0:
            credit_spread = 0.03  # 300 bps for marginally profitable companies
        else:
            credit_spread = 0.05  # 500 bps for unprofitable companies

        pre_tax_cost_of_debt = self.dcf_params['risk_free_rate'] + credit_spread

        # After-tax cost of debt
        cost_of_debt = pre_tax_cost_of_debt * (1 - self.historical[latest_year]['tax_rate'])

        # Calculate WACC
        wacc = weight_equity * cost_of_equity + weight_debt * cost_of_debt

        # Store the components
        self.wacc_components = {
            'risk_free_rate': self.dcf_params['risk_free_rate'],
            'beta': self.beta,
            'market_risk_premium': self.dcf_params['market_risk_premium'],
            'cost_of_equity': cost_of_equity,
            'pre_tax_cost_of_debt': pre_tax_cost_of_debt,
            'tax_rate': self.historical[latest_year]['tax_rate'],
            'after_tax_cost_of_debt': cost_of_debt,
            'weight_equity': weight_equity,
            'weight_debt': weight_debt,
            'wacc': wacc
        }

        print(f"WACC calculated: {wacc:.2%}")
        return wacc

    def calculate_dcf(self):
        """
        Calculate the Discounted Cash Flow (DCF) valuation
        """
        print(f"Calculating DCF valuation for {self.ticker}...")

        # Calculate WACC if not already done
        if not hasattr(self, 'wacc_components'):
            self.wacc = self.calculate_wacc()
        else:
            self.wacc = self.wacc_components['wacc']

        # Present value factors for each year
        pv_factors = [(1 + self.wacc) ** -(i + 1) for i in range(len(self.projection_years))]

        # Calculate present value of projected free cash flows
        fcf_values = [self.projections[year]['fcf'] for year in self.projection_years]
        pv_fcf = [fcf * pv_factor for fcf, pv_factor in zip(fcf_values, pv_factors)]
        sum_pv_fcf = sum(pv_fcf)

        # Calculate terminal value
        last_year = self.projection_years[-1]
        terminal_fcf = self.projections[last_year]['fcf'] * (1 + self.dcf_params['terminal_growth_rate'])
        terminal_value = terminal_fcf / (self.wacc - self.dcf_params['terminal_growth_rate'])
        pv_terminal_value = terminal_value * pv_factors[-1]

        # Calculate enterprise value
        enterprise_value = sum_pv_fcf + pv_terminal_value

        # Get latest financial data for adjustments
        years_list = list(self.historical.keys())
        latest_year = max(years_list)

        # Adjustments to get to equity value
        debt = self.historical[latest_year]['total_debt']
        cash = self.historical[latest_year]['cash']

        # Calculate equity value
        equity_value = enterprise_value - debt + cash

        # Calculate per share value
        shares_outstanding = self.company_info['shares_outstanding']
        if shares_outstanding > 0:
            per_share_value = equity_value / shares_outstanding
        else:
            per_share_value = 0
            print("Warning: Shares outstanding is zero or not available")

        # Current price from Yahoo Finance
        current_price = self.company_info['current_price']

        # Calculate upside/downside
        if current_price > 0:
            upside_percentage = (per_share_value / current_price - 1) * 100
        else:
            upside_percentage = 0
            print("Warning: Current price is zero or not available")

        # Determine recommendation
        if upside_percentage > 15:
            recommendation = "BUY"
        elif upside_percentage < -15:
            recommendation = "SELL"
        else:
            recommendation = "HOLD"

        # Store DCF results
        self.dcf_results = {
            'projection_years': self.projection_years,
            'wacc': self.wacc,
            'terminal_growth_rate': self.dcf_params['terminal_growth_rate'],
            'fcf_projections': {year: self.projections[year]['fcf'] for year in self.projection_years},
            'pv_fcf': {year: pv for year, pv in zip(self.projection_years, pv_fcf)},
            'sum_pv_fcf': sum_pv_fcf,
            'terminal_value': terminal_value,
            'pv_terminal_value': pv_terminal_value,
            'enterprise_value': enterprise_value,
            'debt': debt,
            'cash': cash,
            'equity_value': equity_value,
            'shares_outstanding': shares_outstanding,
            'per_share_value': per_share_value,
            'current_price': current_price,
            'upside_percentage': upside_percentage,
            'recommendation': recommendation
        }

        print(f"DCF valuation completed for {self.ticker}")
        print(f"Estimated value per share: {per_share_value:.2f} {self.company_info['currency']}")
        print(f"Current price: {current_price:.2f} {self.company_info['currency']}")
        print(f"Upside potential: {upside_percentage:.2f}%")
        print(f"Recommendation: {recommendation}")

        return self.dcf_results

    def sensitivity_analysis(self, wacc_range=0.02, growth_range=0.01):
        """
        Perform sensitivity analysis by varying WACC and terminal growth rate
        """
        print(f"Performing sensitivity analysis for {self.ticker}...")

        # Create ranges for WACC and terminal growth rate
        base_wacc = self.wacc
        base_growth = self.dcf_params['terminal_growth_rate']

        wacc_values = [base_wacc - wacc_range, base_wacc - wacc_range / 2, base_wacc,
                       base_wacc + wacc_range / 2, base_wacc + wacc_range]
        growth_values = [base_growth - growth_range, base_growth - growth_range / 2, base_growth,
                         base_growth + growth_range / 2, base_growth + growth_range]

        # Ensure terminal growth is less than WACC
        growth_values = [min(g, w - 0.005) for g in growth_values for w in [wacc_values[2]]]

        # Initialize sensitivity matrix
        sensitivity_matrix = []

        # Calculate per share value for each combination
        for wacc in wacc_values:
            row = []
            for growth in growth_values:
                # Skip invalid combinations (growth >= wacc)
                if growth >= wacc:
                    row.append(None)
                    continue

                # Calculate last FCF
                last_year = self.projection_years[-1]
                last_fcf = self.projections[last_year]['fcf']

                # Calculate terminal value with new parameters
                terminal_fcf = last_fcf * (1 + growth)
                terminal_value = terminal_fcf / (wacc - growth)

                # Calculate present value of terminal value
                pv_factor = (1 + wacc) ** -len(self.projection_years)
                pv_terminal_value = terminal_value * pv_factor

                # Calculate present value of FCFs with new WACC
                pv_factors = [(1 + wacc) ** -(i + 1) for i in range(len(self.projection_years))]
                fcf_values = [self.projections[year]['fcf'] for year in self.projection_years]
                pv_fcf = [fcf * pv_factor for fcf, pv_factor in zip(fcf_values, pv_factors)]
                sum_pv_fcf = sum(pv_fcf)

                # Calculate new enterprise value
                enterprise_value = sum_pv_fcf + pv_terminal_value

                # Get latest financial data for adjustments
                years_list = list(self.historical.keys())
                latest_year = max(years_list)

                # Adjustments to get to equity value
                debt = self.historical[latest_year]['total_debt']
                cash = self.historical[latest_year]['cash']

                # Calculate equity value
                equity_value = enterprise_value - debt + cash

                # Calculate per share value
                shares_outstanding = self.company_info['shares_outstanding']
                if shares_outstanding > 0:
                    per_share_value = equity_value / shares_outstanding
                else:
                    per_share_value = 0

                row.append(per_share_value)

            sensitivity_matrix.append(row)

        # Store sensitivity analysis results
        self.sensitivity_results = {
            'wacc_values': wacc_values,
            'growth_values': growth_values,
            'sensitivity_matrix': sensitivity_matrix
        }

        print(f"Sensitivity analysis completed for {self.ticker}")
        return self.sensitivity_results

    def generate_report(self, output_dir="./"):
        """
        Generate a comprehensive DCF analysis report in text format
        """
        print(f"Generating DCF analysis report for {self.ticker}...")

        # Ensure we have run all necessary calculations
        if not hasattr(self, 'historical'):
            self.calculate_historical_metrics()

        if not hasattr(self, 'projections') or not self.projections:
            self.generate_projections()

        if not hasattr(self, 'dcf_results') or not self.dcf_results:
            self.calculate_dcf()

        if not hasattr(self, 'sensitivity_results') or not self.sensitivity_results:
            self.sensitivity_analysis()

        # Create a text report
        report = []

        # Title
        report.append(f"DCF VALUATION REPORT: {self.company_info['name']} ({self.ticker})")
        report.append(f"Date: {self.today}")
        report.append(f"{'-' * 80}")

        # Company information
        report.append("COMPANY DESCRIPTION")
        report.append(f"{'-' * 80}")
        report.append(f"Sector: {self.company_info['sector']}")
        report.append(f"Industry: {self.company_info['industry']}")
        report.append(f"Country: {self.company_info['country']}")
        report.append(f"Employees: {self.company_info['employees']}")
        report.append("")
        report.append(f"{self.company_info['description']}")
        report.append("")

        # Summary of results
        report.append("VALUATION SUMMARY")
        report.append(f"{'-' * 80}")
        report.append(f"Current Price: {self.dcf_results['current_price']:.2f} {self.company_info['currency']}")
        report.append(f"DCF Value: {self.dcf_results['per_share_value']:.2f} {self.company_info['currency']}")
        report.append(f"Upside Potential: {self.dcf_results['upside_percentage']:.2f}%")
        report.append(f"Recommendation: {self.dcf_results['recommendation']}")
        report.append("")

        # Key assumptions
        report.append("KEY ASSUMPTIONS")
        report.append(f"{'-' * 80}")
        report.append(f"WACC: {self.wacc:.2%}")
        report.append(f"Terminal Growth Rate: {self.dcf_params['terminal_growth_rate']:.2%}")
        report.append(f"Projection Period: {len(self.projection_years)} years")
        report.append("")

        # Historical financials
        report.append("HISTORICAL FINANCIALS")
        report.append(f"{'-' * 80}")
        report.append(f"{'Year':<10} {'Revenue':<15} {'EBIT':<15} {'EBIT Margin':<15} {'Net Income':<15} {'FCF':<15}")
        report.append(f"{'-' * 80}")

        years_list = list(self.historical.keys())
        for year in years_list:
            revenue = self.historical[year]['revenue'] / 1e6  # Convert to millions
            ebit = self.historical[year]['ebit'] / 1e6  # Convert to millions
            ebit_margin = self.historical[year].get('ebit_margin', 0)
            net_income = self.historical[year]['net_income'] / 1e6  # Convert to millions

            # Approximate FCF for historical years
            if 'depreciation' in self.historical[year] and 'capex' in self.historical[year]:
                fcf = (self.historical[year]['net_income'] + self.historical[year]['depreciation'] -
                       self.historical[year]['capex']) / 1e6
            else:
                fcf = 0

            report.append(
                f"{year:<10} {revenue:,.2f} M {ebit:,.2f} M {ebit_margin:.2%} {net_income:,.2f} M {fcf:,.2f} M")

        report.append("")

        # Financial projections
        report.append("FINANCIAL PROJECTIONS")
        report.append(f"{'-' * 80}")
        report.append(f"{'Year':<10} {'Revenue':<15} {'Growth':<10} {'EBIT':<15} {'EBIT Margin':<15} {'FCF':<15}")
        report.append(f"{'-' * 80}")

        for year in self.projection_years:
            revenue = self.projections[year]['revenue'] / 1e6  # Convert to millions
            growth = self.projections[year]['revenue_growth']
            ebit = self.projections[year]['ebit'] / 1e6  # Convert to millions
            ebit_margin = self.projections[year]['ebit_margin']
            fcf = self.projections[year]['fcf'] / 1e6  # Convert to millions

            report.append(f"{year:<10} {revenue:,.2f} M {growth:.2%} {ebit:,.2f} M {ebit_margin:.2%} {fcf:,.2f} M")

        report.append("")

        # DCF calculation
        report.append("DCF CALCULATION")
        report.append(f"{'-' * 80}")
        report.append(f"{'Year':<10} {'FCF':<15} {'PV Factor':<15} {'PV of FCF':<15}")
        report.append(f"{'-' * 80}")

        for i, year in enumerate(self.projection_years):
            fcf = self.dcf_results['fcf_projections'][year] / 1e6  # Convert to millions
            pv_factor = (1 + self.wacc) ** -(i + 1)
            pv_fcf = pv_factor * fcf

            report.append(f"{year:<10} {fcf:,.2f} M {pv_factor:.4f} {pv_fcf:,.2f} M")

        report.append(f"{'-' * 80}")
        report.append(f"Sum of PV of FCF: {self.dcf_results['sum_pv_fcf'] / 1e6:,.2f} M")
        report.append("")
        report.append(f"Terminal Value: {self.dcf_results['terminal_value'] / 1e6:,.2f} M")
        report.append(f"PV of Terminal Value: {self.dcf_results['pv_terminal_value'] / 1e6:,.2f} M")
        report.append("")
        report.append(f"Enterprise Value: {self.dcf_results['enterprise_value'] / 1e6:,.2f} M")
        report.append(f"- Debt: {self.dcf_results['debt'] / 1e6:,.2f} M")
        report.append(f"+ Cash: {self.dcf_results['cash'] / 1e6:,.2f} M")
        report.append(f"= Equity Value: {self.dcf_results['equity_value'] / 1e6:,.2f} M")
        report.append("")
        report.append(f"Shares Outstanding: {self.dcf_results['shares_outstanding'] / 1e6:,.2f} M")
        report.append(f"Value per Share: {self.dcf_results['per_share_value']:.2f} {self.company_info['currency']}")
        report.append("")

        # WACC calculation
        report.append("WACC CALCULATION")
        report.append(f"{'-' * 80}")
        report.append(f"Risk-free Rate: {self.wacc_components['risk_free_rate']:.2%}")
        report.append(f"Beta: {self.wacc_components['beta']:.2f}")
        report.append(f"Market Risk Premium: {self.wacc_components['market_risk_premium']:.2%}")
        report.append(f"Cost of Equity: {self.wacc_components['cost_of_equity']:.2%}")
        report.append("")
        report.append(f"Pre-tax Cost of Debt: {self.wacc_components['pre_tax_cost_of_debt']:.2%}")
        report.append(f"Tax Rate: {self.wacc_components['tax_rate']:.2%}")
        report.append(f"After-tax Cost of Debt: {self.wacc_components['after_tax_cost_of_debt']:.2%}")
        report.append("")
        report.append(f"Weight of Equity: {self.wacc_components['weight_equity']:.2%}")
        report.append(f"Weight of Debt: {self.wacc_components['weight_debt']:.2%}")
        report.append("")
        report.append(f"WACC: {self.wacc_components['wacc']:.2%}")
        report.append("")

        # Sensitivity analysis
        report.append("SENSITIVITY ANALYSIS")
        report.append(f"{'-' * 80}")
        report.append("Share Price at different WACC and Terminal Growth Rate combinations:")
        report.append("")

        # Header row for terminal growth rates
        header = "{:<10}".format("WACC \\ g")
        for growth in self.sensitivity_results['growth_values']:
            header += f"{growth:.2%}        "
        report.append(header)

        # Data rows
        for i, wacc in enumerate(self.sensitivity_results['wacc_values']):
            row = f"{wacc:.2%}     "
            for j, value in enumerate(self.sensitivity_results['sensitivity_matrix'][i]):
                if value is None:
                    row += f"N/A           "
                else:
                    row += f"{value:.2f}        "
            report.append(row)

        report.append("")

        # DCF Model sheet representation
        report.append("DCF MODEL SHEET")
        report.append(f"{'-' * 80}")

        # Header with years
        header = f"{'Item':<30}"
        for year in [max(list(self.historical.keys()))] + self.projection_years:
            header += f"{year:<15}"
        report.append(header)
        report.append(f"{'-' * 80}")

        # Revenue row
        row = f"{'Revenue':<30}"
        latest_year = max(list(self.historical.keys()))
        row += f"{self.historical[latest_year]['revenue'] / 1e6:,.2f} M    "

        for year in self.projection_years:
            row += f"{self.projections[year]['revenue'] / 1e6:,.2f} M    "
        report.append(row)

        # Revenue growth row
        row = f"{'Revenue Growth':<30}"
        if len(list(self.historical.keys())) > 1:
            sorted_years = sorted(list(self.historical.keys()))
            prev_year = sorted_years[-2]
            latest_year = sorted_years[-1]
            growth = (self.historical[latest_year]['revenue'] / self.historical[prev_year]['revenue']) - 1
            row += f"{growth:.2%}         "
        else:
            row += f"N/A          "

        for year in self.projection_years:
            row += f"{self.projections[year]['revenue_growth']:.2%}         "
        report.append(row)

        # EBIT row
        row = f"{'EBIT':<30}"
        row += f"{self.historical[latest_year]['ebit'] / 1e6:,.2f} M    "

        for year in self.projection_years:
            row += f"{self.projections[year]['ebit'] / 1e6:,.2f} M    "
        report.append(row)

        # EBIT margin row
        row = f"{'EBIT Margin':<30}"
        row += f"{self.historical[latest_year]['ebit_margin']:.2%}         "

        for year in self.projection_years:
            row += f"{self.projections[year]['ebit_margin']:.2%}         "
        report.append(row)

        # Taxes row
        row = f"{'Taxes':<30}"
        if 'taxes' in self.historical[latest_year]:
            row += f"{self.historical[latest_year]['taxes'] / 1e6:,.2f} M    "
        else:
            tax_rate = self.historical[latest_year]['tax_rate']
            taxes = self.historical[latest_year]['ebit'] * tax_rate
            row += f"{taxes / 1e6:,.2f} M    "

        for year in self.projection_years:
            row += f"{self.projections[year]['taxes'] / 1e6:,.2f} M    "
        report.append(row)

        # NOPAT row
        row = f"{'NOPAT':<30}"
        if 'taxes' in self.historical[latest_year]:
            nopat = self.historical[latest_year]['ebit'] - self.historical[latest_year]['taxes']
        else:
            tax_rate = self.historical[latest_year]['tax_rate']
            nopat = self.historical[latest_year]['ebit'] * (1 - tax_rate)
        row += f"{nopat / 1e6:,.2f} M    "

        for year in self.projection_years:
            row += f"{self.projections[year]['nopat'] / 1e6:,.2f} M    "
        report.append(row)

        # Depreciation row
        row = f"{'+ Depreciation':<30}"
        row += f"{self.historical[latest_year]['depreciation'] / 1e6:,.2f} M    "

        for year in self.projection_years:
            row += f"{self.projections[year]['depreciation'] / 1e6:,.2f} M    "
        report.append(row)

        # CAPEX row
        row = f"{'- CAPEX':<30}"
        row += f"{self.historical[latest_year]['capex'] / 1e6:,.2f} M    "

        for year in self.projection_years:
            row += f"{self.projections[year]['capex'] / 1e6:,.2f} M    "
        report.append(row)

        # Working capital change row
        row = f"{'- Working Capital Change':<30}"
        if len(list(self.historical.keys())) > 1:
            sorted_years = sorted(list(self.historical.keys()))
            prev_year = sorted_years[-2]
            latest_year = sorted_years[-1]
            wc_change = (
                        self.historical[latest_year]['working_capital'] - self.historical[prev_year]['working_capital'])
            row += f"{wc_change / 1e6:,.2f} M    "
        else:
            row += f"N/A          "

        for year in self.projection_years:
            row += f"{self.projections[year]['working_capital_change'] / 1e6:,.2f} M    "
        report.append(row)

        # Free Cash Flow row
        row = f"{'= Free Cash Flow':<30}"
        if 'taxes' in self.historical[latest_year] and len(list(self.historical.keys())) > 1:
            nopat = self.historical[latest_year]['ebit'] - self.historical[latest_year]['taxes']
            sorted_years = sorted(list(self.historical.keys()))
            prev_year = sorted_years[-2]
            wc_change = (
                        self.historical[latest_year]['working_capital'] - self.historical[prev_year]['working_capital'])
            fcf = nopat + self.historical[latest_year]['depreciation'] - self.historical[latest_year][
                'capex'] - wc_change
            row += f"{fcf / 1e6:,.2f} M    "
        else:
            row += f"N/A          "

        for year in self.projection_years:
            row += f"{self.projections[year]['fcf'] / 1e6:,.2f} M    "
        report.append(row)

        # Discount factor row
        row = f"{'Discount Factor':<30}"
        row += f"1.0000        "

        for i, year in enumerate(self.projection_years):
            discount_factor = (1 + self.wacc) ** -(i + 1)
            row += f"{discount_factor:.4f}       "
        report.append(row)

        # PV of FCF row
        row = f"{'PV of FCF':<30}"
        row += f"N/A          "

        for i, year in enumerate(self.projection_years):
            discount_factor = (1 + self.wacc) ** -(i + 1)
            pv_fcf = self.projections[year]['fcf'] * discount_factor
            row += f"{pv_fcf / 1e6:,.2f} M    "
        report.append(row)

        report.append(f"{'-' * 80}")
        report.append("")

        # Save the report to a file
        filename = f"{self.ticker}_DCF_Analysis_{self.today}.txt"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))

        print(f"DCF analysis report generated and saved to {filepath}")

        return report

    def plot_charts(self, output_dir="./"):
        """
        Generate charts for the DCF analysis
        """
        print(f"Generating charts for {self.ticker}...")

        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Historical vs. Projected Revenue
        plt.figure(figsize=(12, 6))

        # Historical data
        years_list = sorted(list(self.historical.keys()))
        hist_revenue = [self.historical[year]['revenue'] / 1e6 for year in years_list]  # Convert to millions

        # Projected data
        proj_years = self.projection_years
        proj_revenue = [self.projections[year]['revenue'] / 1e6 for year in proj_years]  # Convert to millions

        # Plot
        plt.plot(years_list, hist_revenue, 'b-o', label='Historical')
        plt.plot(proj_years, proj_revenue, 'r--o', label='Projected')

        # Add a vertical line separating historical and projected data
        plt.axvline(x=years_list[-1], color='gray', linestyle='--')

        plt.title(f'{self.ticker} Revenue - Historical vs. Projected', fontsize=16)
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('Revenue (in millions)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()

        # Save the chart
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{self.ticker}_Revenue_Chart.png"))
        plt.close()

        # EBIT Margin - Historical vs. Projected
        plt.figure(figsize=(12, 6))

        # Historical data
        hist_margin = [self.historical[year]['ebit_margin'] for year in years_list]

        # Projected data
        proj_margin = [self.projections[year]['ebit_margin'] for year in proj_years]

        # Plot
        plt.plot(years_list, hist_margin, 'b-o', label='Historical')
        plt.plot(proj_years, proj_margin, 'r--o', label='Projected')

        # Add a vertical line separating historical and projected data
        plt.axvline(x=years_list[-1], color='gray', linestyle='--')

        plt.title(f'{self.ticker} EBIT Margin - Historical vs. Projected', fontsize=16)
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('EBIT Margin (%)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()

        # Format y-axis as percentage
        plt.gca().yaxis.set_major_formatter(plt.matplotlib.ticker.PercentFormatter(1.0))

        # Save the chart
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{self.ticker}_EBIT_Margin_Chart.png"))
        plt.close()

        # Free Cash Flow - Historical vs. Projected
        plt.figure(figsize=(12, 6))

        # Historical data (approximate FCF calculation)
        hist_fcf = []
        for i, year in enumerate(years_list):
            if i > 0:
                prev_year = years_list[i - 1]
                nopat = self.historical[year]['ebit'] * (1 - self.historical[year]['tax_rate'])
                wc_change = (self.historical[year]['working_capital'] - self.historical[prev_year]['working_capital'])
                fcf = nopat + self.historical[year]['depreciation'] - self.historical[year]['capex'] - wc_change
                hist_fcf.append(fcf / 1e6)  # Convert to millions
            else:
                hist_fcf.append(None)  # First year's FCF is not calculated

        # Projected data
        proj_fcf = [self.projections[year]['fcf'] / 1e6 for year in proj_years]  # Convert to millions

        # Plot (skip first year of historical data)
        plt.plot(years_list[1:], hist_fcf[1:], 'b-o', label='Historical')
        plt.plot(proj_years, proj_fcf, 'r--o', label='Projected')

        # Add a vertical line separating historical and projected data
        plt.axvline(x=years_list[-1], color='gray', linestyle='--')

        plt.title(f'{self.ticker} Free Cash Flow - Historical vs. Projected', fontsize=16)
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('Free Cash Flow (in millions)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()

        # Save the chart
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{self.ticker}_FCF_Chart.png"))
        plt.close()

        # DCF Valuation Breakdown chart (pie chart)
        plt.figure(figsize=(10, 8))

        # Data for the pie chart
        pv_fcf_sum = self.dcf_results['sum_pv_fcf']
        pv_terminal = self.dcf_results['pv_terminal_value']
        total_enterprise_value = pv_fcf_sum + pv_terminal

        # Calculate percentages
        pv_fcf_pct = pv_fcf_sum / total_enterprise_value * 100
        pv_terminal_pct = pv_terminal / total_enterprise_value * 100

        # Create pie chart
        labels = [f'PV of Projected FCF: {pv_fcf_pct:.1f}%', f'PV of Terminal Value: {pv_terminal_pct:.1f}%']
        sizes = [pv_fcf_sum, pv_terminal]
        colors = ['#4CAF50', '#2196F3']
        explode = (0.1, 0)  # explode the 1st slice (PV of FCF)

        plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
                shadow=True, startangle=140, textprops={'fontsize': 12})
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle

        plt.title(f'{self.ticker} DCF Valuation Breakdown', fontsize=16)

        # Save the chart
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{self.ticker}_DCF_Breakdown_Chart.png"))
        plt.close()

        # Sensitivity Analysis Heatmap
        plt.figure(figsize=(12, 8))

        # Prepare data for heatmap
        wacc_values = [f"{w:.2%}" for w in self.sensitivity_results['wacc_values']]
        growth_values = [f"{g:.2%}" for g in self.sensitivity_results['growth_values']]

        # Convert sensitivity matrix to numpy array and handle None values
        matrix = np.array(self.sensitivity_results['sensitivity_matrix'], dtype=float)
        matrix[matrix == None] = np.nan

        # Create heatmap
        cmap = plt.cm.RdYlGn  # Red-Yellow-Green colormap
        heatmap = plt.pcolormesh(matrix, cmap=cmap)

        # Add a color bar
        plt.colorbar(heatmap, label='Share Price')

        # Set the ticks and labels
        plt.yticks(np.arange(0.5, len(wacc_values)), wacc_values)
        plt.xticks(np.arange(0.5, len(growth_values)), growth_values)

        plt.title(f'{self.ticker} Sensitivity Analysis: Share Price', fontsize=16)
        plt.xlabel('Terminal Growth Rate', fontsize=12)
        plt.ylabel('WACC', fontsize=12)

        # Save the chart
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{self.ticker}_Sensitivity_Analysis.png"))
        plt.close()

        print(f"Charts generated and saved to {output_dir}")

    def run_full_analysis(self, output_dir="./"):
        """
        Run the complete DCF analysis pipeline
        """
        try:
            # Calculate historical metrics
            self.calculate_historical_metrics()

            # Generate projections
            self.generate_projections()

            # Calculate WACC
            self.calculate_wacc()

            # Calculate DCF
            self.calculate_dcf()

            # Perform sensitivity analysis
            self.sensitivity_analysis()

            # Generate report
            self.generate_report(output_dir)

            # Generate charts
            self.plot_charts(output_dir)

            print(f"Full DCF analysis completed successfully for {self.ticker}")
            return True

        except Exception as e:
            print(f"Error during DCF analysis: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """
    Main function to run the DCF model from command line
    """
    if len(sys.argv) < 2:
        print("Usage: python dcf_model.py <ticker_symbol>")
        return

    ticker = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "./"

    try:
        # Create DCF model
        dcf_model = DCFModel(ticker)

        # Run full analysis
        success = dcf_model.run_full_analysis(output_dir)

        if success:
            print(f"DCF analysis for {ticker} completed successfully")
            print(f"Results saved to {output_dir}")
        else:
            print(f"DCF analysis for {ticker} failed")

    except Exception as e:
        print(f"Error analyzing {ticker}: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
