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
    """Plots asset prices with highlights, including moving average and R² correlations in a Bloomberg-like dark theme."""
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

        # Set dark Bloomberg-like style
        plt.style.use('dark_background')

        # Create figure with specific dimensions for professional look
        fig, ax = plt.subplots(figsize=(14, 8), dpi=100)
        fig.patch.set_facecolor('#121212')  # Very dark gray background
        ax.set_facecolor('#1e1e1e')  # Slightly lighter dark background for plot area

        # Define professional color palette - bright colors that stand out on dark background
        colors = ['#00a5ff', '#ff9500', '#00c853', '#ff3d71', '#7b61ff', '#ffce3e', '#4ecdc4', '#ff6e57']

        # Grid styling
        ax.grid(True, linestyle='--', alpha=0.3, color='#555555')

        # Plot target asset with thicker line
        ax.plot(normalized_prices[target_asset], label=target_asset,
                color=colors[0], linewidth=2.5)

        # Plot target moving average
        ax.plot(moving_averages[target_asset],
                label=f'{target_asset} {ma_window}D MA',
                color=colors[0], linestyle='--',
                alpha=0.7, linewidth=1.5)

        if average_related and valid_related_assets:
            # Plot average of related assets and its moving average
            average_related_price = normalized_prices[valid_related_assets].mean(axis=1)
            average_ma = moving_averages[valid_related_assets].mean(axis=1)

            ax.plot(average_related_price,
                    label=f"Avg Related (R²={np.mean(list(correlations.values())):.2f})",
                    color=colors[1], linewidth=2.5)
            ax.plot(average_ma,
                    label=f'Avg Related {ma_window}D MA',
                    color=colors[1], linestyle='--',
                    alpha=0.7, linewidth=1.5)
        else:
            # Plot individual related assets and their moving averages
            for i, asset in enumerate(valid_related_assets):
                color_idx = (i + 1) % len(colors)  # Cycle through colors
                ax.plot(normalized_prices[asset],
                        label=f"{asset} (R²={correlations[asset]:.2f})",
                        color=colors[color_idx], linewidth=1.8)
                ax.plot(moving_averages[asset],
                        label=f'{asset} {ma_window}D MA',
                        color=colors[color_idx], linestyle='--',
                        alpha=0.7, linewidth=1.2)

        # Add event markers if specified
        if events:
            event_handles = []
            event_labels = []
            for date_str, event in events.items():
                event_date = pd.to_datetime(date_str)
                if event_date >= pd.to_datetime(start_date) and event_date <= pd.to_datetime(end_date):
                    line = ax.axvline(event_date, color='#ff3d71', linestyle='-',
                                      alpha=0.5, linewidth=1.5)
                    # Add annotation above the line
                    y_pos = normalized_prices.max().max() * 1.05
                    ax.annotate(event, xy=(event_date, y_pos),
                                xytext=(0, 10), textcoords='offset points',
                                ha='center', va='bottom', color='#ff3d71',
                                fontsize=9, fontweight='bold')
                    event_handles.append(line)
                    event_labels.append(event)

        # Customize spines (borders)
        for spine in ax.spines.values():
            spine.set_color('#555555')
            spine.set_linewidth(0.5)

        # Format axes
        ax.xaxis.set_tick_params(colors='white', labelsize=10)
        ax.yaxis.set_tick_params(colors='white', labelsize=10)

        # Title and labels with professional formatting
        current_date = date.today().strftime("%Y-%m-%d")
        ax.set_title(f"Normalized Asset Performance\n{start_date} - {end_date}",
                     color='white', fontsize=16, fontweight='bold', pad=15)

        # Add a subtitle with additional information
        ax.text(0.5, 0.97, f"{target_asset} vs. {', '.join(valid_related_assets)}",
                transform=ax.transAxes, ha='center', color='#bbbbbb',
                fontsize=11, fontweight='normal')

        ax.set_xlabel("Date", color='white', fontsize=12, labelpad=10)
        ax.set_ylabel("Normalized Price (Base = 100)", color='white', fontsize=12, labelpad=10)

        # Improve x-axis date formatting
        locator = plt.MaxNLocator(nbins=10)
        ax.xaxis.set_major_locator(locator)
        date_format = plt.matplotlib.dates.DateFormatter('%b %Y')  # e.g., Jan 2023
        ax.xaxis.set_major_formatter(date_format)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # Add price range annotation
        min_price = normalized_prices.min().min()
        max_price = normalized_prices.max().max()
        range_text = f"Range: {min_price:.1f} - {max_price:.1f} ({max_price - min_price:.1f} pts)"
        ax.text(0.01, 0.01, range_text, transform=ax.transAxes,
                color='#bbbbbb', fontsize=9, alpha=0.8)

        # Add current date stamp to the plot
        ax.text(0.99, 0.01, f"Generated: {current_date}", transform=ax.transAxes,
                color='#bbbbbb', fontsize=9, ha='right', alpha=0.8)

        # Custom legend with cleaner formatting
        legend = ax.legend(loc='upper left', bbox_to_anchor=(1.01, 1), frameon=True,
                           facecolor='#1e1e1e', edgecolor='#555555', fontsize=10)
        for text in legend.get_texts():
            text.set_color('white')

        # Adjust layout
        plt.tight_layout()
        plt.subplots_adjust(right=0.8)  # Make room for legend

        # Save to a file with high resolution
        output_file = "bloomberg_style_asset_performance.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor=fig.get_facecolor())
        plt.close()

        print(f"\nProfessional chart saved to '{output_file}'")

        # Still try to show it, but wrapped in a try-except to handle PyCharm issues
        try:
            plt.figure(figsize=(14, 8))
            img = plt.imread(output_file)
            plt.imshow(img)
            plt.axis('off')
            plt.tight_layout(pad=0)
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
