import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date, datetime, timedelta
from scipy import stats
from requests_html import HTMLSession
import json
import time
import re

ASSET_MAP = {
    "gold": "GC=F",  # Gold futures
    "natural gas": "NG=F",  # Natural gas futures
    "crude oil": "CL=F",  # Crude oil futures
    # Add more mappings as needed
}


def get_yahoo_finance_data(ticker, start_date, end_date):
    """
    Fetch historical data from Yahoo Finance using requests-html.
    This approach directly scrapes the data from Yahoo Finance's website.

    Args:
        ticker (str): The ticker symbol
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        pd.DataFrame: DataFrame with historical price data
    """
    try:
        # Convert dates to Unix timestamp (seconds since epoch)
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_timestamp = int(
            datetime.strptime(end_date, "%Y-%m-%d").timestamp() + 86400)  # Add a day to include end date

        # Create URL for the API call
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start_timestamp}&period2={end_timestamp}&interval=1d"

        # Create a session
        session = HTMLSession()

        # Add headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        # Get the data
        response = session.get(url, headers=headers)

        # Check if response is valid
        if response.status_code != 200:
            print(f"Failed to get data for {ticker}. Status code: {response.status_code}")
            return pd.DataFrame()

        # Parse JSON response
        data = response.json()

        # Check if we have data
        if 'chart' not in data or 'result' not in data['chart'] or not data['chart']['result']:
            print(f"No data returned for {ticker}")
            return pd.DataFrame()

        # Extract price data
        chart_data = data['chart']['result'][0]
        timestamps = chart_data['timestamp']
        quote = chart_data['indicators']['quote'][0]

        # Check if we have adjusted close prices
        adjclose = None
        if 'adjclose' in chart_data['indicators']:
            adjclose = chart_data['indicators']['adjclose'][0]['adjclose']

        # Create DataFrame
        df = pd.DataFrame({
            'open': quote.get('open', []),
            'high': quote.get('high', []),
            'low': quote.get('low', []),
            'close': quote.get('close', []),
            'volume': quote.get('volume', [])
        })

        # Add adjusted close if available
        if adjclose is not None:
            df['adjclose'] = adjclose
        else:
            df['adjclose'] = df['close']

        # Add date index
        df.index = pd.to_datetime([datetime.fromtimestamp(x) for x in timestamps])
        df.index.name = 'date'

        # Fill any missing values
        df = df.ffill()  # Using ffill() instead of fillna(method='ffill')

        return df

    except Exception as e:
        print(f"Error retrieving data for {ticker}: {str(e)}")
        return pd.DataFrame()


def calculate_r_squared(x, y):
    """Calculate R² correlation coefficient between two series."""
    # Remove any NaN values
    mask = ~np.isnan(x) & ~np.isnan(y)
    x = x[mask]
    y = y[mask]

    if len(x) < 2 or len(y) < 2:  # Need at least 2 points for correlation
        return np.nan

    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    return r_value ** 2


def plot_assets_with_highlights(target_asset, related_assets, start_date, end_date, events=None, average_related=False,
                                ma_window=20):
    """Plots asset prices with highlights, including moving average and R² correlations."""
    all_tickers = [target_asset] + related_assets

    try:
        # Download data for each ticker individually
        print(f"Downloading data for {len(all_tickers)} assets...")
        data_dict = {}

        # Get target asset data
        print(f"Fetching data for {target_asset}...")
        target_data = get_yahoo_finance_data(target_asset, start_date, end_date)
        if not target_data.empty:
            data_dict[target_asset] = target_data['adjclose']
        else:
            raise ValueError(f"Failed to retrieve data for target asset {target_asset}")

        # Give a small delay to avoid rate limiting
        time.sleep(1)

        # Get related assets data
        valid_related_assets = []
        for i, asset in enumerate(related_assets):
            print(f"Fetching data for {asset} ({i + 1}/{len(related_assets)})...")
            asset_data = get_yahoo_finance_data(asset, start_date, end_date)
            if not asset_data.empty:
                data_dict[asset] = asset_data['adjclose']
                valid_related_assets.append(asset)
            else:
                print(f"Warning: No data for {asset}, excluding from analysis")

            # Add delay between requests to avoid rate limiting
            if i < len(related_assets) - 1:
                time.sleep(1)

        # Create a DataFrame with aligned dates
        data = pd.DataFrame(data_dict)

        # Check if we have enough data
        if data.empty or len(data.columns) < 1:
            raise ValueError("No data available for the specified assets and date range.")

        # Forward fill any missing values
        data = data.ffill()  # Using ffill() instead of fillna(method='ffill')

        # Normalize prices to starting value = 100
        normalized_prices = (data / data.iloc[0]) * 100

        # Calculate moving averages
        moving_averages = normalized_prices.rolling(window=ma_window).mean()

        # Calculate R² correlations only if we have related assets
        correlations = {}
        if valid_related_assets:
            target_returns = normalized_prices[target_asset].pct_change(fill_method=None)
            for asset in valid_related_assets:
                asset_returns = normalized_prices[asset].pct_change(fill_method=None)
                r_squared = calculate_r_squared(target_returns, asset_returns)
                correlations[asset] = r_squared

        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot target asset and its moving average
        ax.plot(normalized_prices[target_asset], label=target_asset)
        ax.plot(moving_averages[target_asset],
                label=f'{target_asset} {ma_window}-day MA',
                linestyle='--',
                alpha=0.7)

        if average_related and valid_related_assets:
            # Plot average of related assets and its moving average
            average_related_price = normalized_prices[valid_related_assets].mean(axis=1)
            average_ma = moving_averages[valid_related_assets].mean(axis=1)

            ax.plot(average_related_price,
                    label=f"Average of Related Assets (R²={np.mean(list(correlations.values())):.2f})",
                    linestyle='-',
                    linewidth=2)
            ax.plot(average_ma,
                    label=f'Related Assets {ma_window}-day MA',
                    linestyle='--',
                    alpha=0.7)
        else:
            # Plot individual related assets and their moving averages
            for asset in valid_related_assets:
                ax.plot(normalized_prices[asset],
                        label=f"{asset} (R²={correlations[asset]:.2f})")
                ax.plot(moving_averages[asset],
                        label=f'{asset} {ma_window}-day MA',
                        linestyle='--',
                        alpha=0.7)

        if events:
            for date_str, event in events.items():
                event_date = pd.to_datetime(date_str)
                if event_date >= pd.to_datetime(start_date) and event_date <= pd.to_datetime(end_date):
                    ax.axvline(event_date, color='red', linestyle='--',
                               alpha=0.6, label=event)

        ax.set_title(f"Asset Price Performance ({start_date} - {end_date})")
        ax.set_xlabel("Date")
        ax.set_ylabel("Normalized Price (%)")
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True)

        # Adjust layout to prevent label cutoff
        plt.tight_layout()

        # Save to a file instead of showing directly (to avoid PyCharm display issues)
        output_file = "asset_performance_chart.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()

        print(f"\nChart saved to '{output_file}'")

        # Still try to show it, but wrapped in a try-except to handle PyCharm issues
        try:
            plt.figure()
            img = plt.imread(output_file)
            plt.imshow(img)
            plt.axis('off')
            plt.show()
        except Exception as e:
            print(f"Could not display the chart directly, but it was saved to '{output_file}'")
            print(f"Display error: {e}")

    except ValueError as e:
        print(f"Error: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
        return


if __name__ == "__main__":
    print("Asset Price Analysis Tool (Yahoo Finance)")
    print("========================================")

    target_asset = input("Enter the target asset ticker (e.g., SPY): ")
    # Use the mapped ticker if available, otherwise assume it's a ticker
    target_asset = ASSET_MAP.get(target_asset.lower(), target_asset)

    average_related = input("Do you want to average the related assets' performance? (yes/no): ").lower() == "yes"
    ma_window = int(input("Enter the moving average window (e.g., 20 for 20-day MA): "))
    num_related = int(input("Enter the number of related assets: "))

    related_assets = []
    for i in range(num_related):
        related_asset = input(f"Enter related asset ticker {i + 1} (or common name like 'gold'): ")
        # Use the mapped ticker if available, otherwise assume it's a ticker
        related_assets.append(ASSET_MAP.get(related_asset.lower(), related_asset))

    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date_input = input("Enter end date (YYYY-MM-DD) or press Enter for today: ")
    end_date = end_date_input if end_date_input else date.today().strftime("%Y-%m-%d")

    use_events = input("Do you want to highlight events? (yes/no): ")
    events = {}
    if use_events.lower() == "yes":
        num_events = int(input("Enter the number of events: "))
        for i in range(num_events):
            date_input = input(f"Enter event date {i + 1} (YYYY-MM-DD): ")
            description = input(f"Enter event description {i + 1}: ")
            events[date_input] = description

    print(f"\nAnalyzing {target_asset} compared to {len(related_assets)} related assets...")
    plot_assets_with_highlights(target_asset, related_assets, start_date, end_date,
                                events, average_related=average_related, ma_window=ma_window)
