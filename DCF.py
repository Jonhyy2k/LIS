#!/usr/bin/env python3
"""
DCF Analysis Runner - A simple script to run DCF analysis for stocks

This script provides a user-friendly interface to run the DCF model
on any publicly traded stock by simply entering a ticker symbol.
"""

import os
import sys
import argparse
from datetime import datetime
from dcf_automator import DCFModel


def run_dcf_analysis(ticker, output_dir="./outputs", custom_params=None):
    """
    Run a complete DCF analysis for the given ticker

    Args:
        ticker (str): Stock ticker symbol
        output_dir (str): Directory to save output files
        custom_params (dict): Optional custom parameters for the DCF model

    Returns:
        bool: True if analysis was successful, False otherwise
    """
    print(f"\n{'=' * 80}")
    print(f"Starting DCF Analysis for {ticker}")
    print(f"{'=' * 80}\n")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Create DCF model
        dcf_model = DCFModel(ticker)

        # Update parameters if provided
        if custom_params:
            for key, value in custom_params.items():
                if key in dcf_model.dcf_params:
                    dcf_model.dcf_params[key] = value
                    print(f"Updated {key} to {value}")

        # Run full analysis
        success = dcf_model.run_full_analysis(output_dir)

        if success:
            print(f"\n{'=' * 80}")
            print(f"DCF analysis for {ticker} completed successfully")
            print(f"Results saved to {output_dir}/{ticker}_DCF_Analysis_{datetime.now().strftime('%Y-%m-%d')}.txt")
            print(f"{'=' * 80}\n")

            # Display key results
            print(f"Key Results for {ticker}:")
            print(f"Current Price: {dcf_model.dcf_results['current_price']:.2f}")
            print(f"DCF Value: {dcf_model.dcf_results['per_share_value']:.2f}")
            print(f"Upside Potential: {dcf_model.dcf_results['upside_percentage']:.2f}%")
            print(f"Recommendation: {dcf_model.dcf_results['recommendation']}")
            return True
        else:
            print(f"\nDCF analysis for {ticker} failed")
            return False

    except Exception as e:
        print(f"\nError analyzing {ticker}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """
    Main function - get ticker and parameters via user input
    """
    print("\n===== Automated DCF Analysis Tool =====\n")

    # Get ticker from user input
    ticker = input("Enter stock ticker symbol (e.g., AAPL, MSFT): ").strip().upper()

    if not ticker:
        print("Error: Ticker symbol is required")
        return False

    # Get optional parameters with defaults
    output_dir = input("Output directory (press Enter for './outputs'): ").strip()
    if not output_dir:
        output_dir = "./outputs"

    # Get terminal growth rate
    growth_input = input("Terminal growth rate (press Enter for default 2%): ").strip()
    growth_rate = None
    if growth_input:
        try:
            growth_rate = float(growth_input)
            if growth_rate > 1:  # User likely entered percentage like 2 instead of 0.02
                growth_rate = growth_rate / 100
        except ValueError:
            print("Invalid growth rate. Using default.")

    # Get projection years
    years_input = input("Projection years (press Enter for default 5): ").strip()
    projection_years = None
    if years_input:
        try:
            projection_years = int(years_input)
        except ValueError:
            print("Invalid number of years. Using default.")

    # Build custom parameters if provided
    custom_params = {}
    if growth_rate is not None:
        custom_params['terminal_growth_rate'] = growth_rate
    if projection_years is not None:
        custom_params['projection_years'] = projection_years

    # Run analysis
    print(f"\nStarting DCF analysis for {ticker}...\n")
    return run_dcf_analysis(ticker, output_dir, custom_params)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
