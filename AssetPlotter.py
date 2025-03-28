import yfinance as yf
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import date
from scipy import stats

ASSET_MAP = {
    "gold": "GC=F",  # Gold futures
    "natural gas": "NG=F",  # Natural gas futures
    "crude oil": "CL=F",  # Crude oil futures
    # Add more mappings as needed
}


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
    tickers = [target_asset] + related_assets

    try:
        data = yf.download(tickers, start=start_date, end=end_date)["Adj Close"]

        if data.empty or data.isnull().all().any():
            raise ValueError("No data available for the specified assets and date range.")

        normalized_prices = (data / data.iloc[0]) * 100

        # Calculate moving averages
        moving_averages = normalized_prices.rolling(window=ma_window).mean()

        # Calculate R² correlations
        correlations = {}
        target_returns = normalized_prices[target_asset].pct_change()
        for asset in related_assets:
            asset_returns = normalized_prices[asset].pct_change()
            r_squared = calculate_r_squared(target_returns, asset_returns)
            correlations[asset] = r_squared

        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot target asset and its moving average
        ax.plot(normalized_prices[target_asset], label=target_asset)
        ax.plot(moving_averages[target_asset],
                label=f'{target_asset} {ma_window}-day MA',
                linestyle='--',
                alpha=0.7)

        if average_related:
            # Plot average of related assets and its moving average
            average_related_price = normalized_prices[related_assets].mean(axis=1)
            average_ma = moving_averages[related_assets].mean(axis=1)

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
            for asset in related_assets:
                ax.plot(normalized_prices[asset],
                        label=f"{asset} (R²={correlations[asset]:.2f})")
                ax.plot(moving_averages[asset],
                        label=f'{asset} {ma_window}-day MA',
                        linestyle='--',
                        alpha=0.7)

        if events:
            for date, event in events.items():
                event_date = pd.to_datetime(date)
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
        plt.show()

    except ValueError as e:
        print(f"Error: {e}")
        return
    except Exception as e:
        print(f"An error occurred during data download: {e}")
        return


if __name__ == "__main__":
    target_asset = input("Enter the target asset ticker (e.g., SPY): ")
    average_related = input("Do you want to average the related assets' performance? (yes/no): ").lower() == "yes"
    ma_window = int(input("Enter the moving average window (e.g., 20 for 20-day MA): "))
    num_related = int(input("Enter the number of related assets: "))

    related_assets = []
    for i in range(num_related):
        related_asset = input(f"Enter related asset ticker {i + 1} (or common name like 'gold'): ")
        # Use the mapped ticker if available, otherwise assume it's a ticker
        related_assets.append(ASSET_MAP.get(related_asset.lower(), related_asset))

    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = date.today().strftime("%Y-%m-%d")

    use_events = input("Do you want to highlight events? (yes/no): ")
    events = {}
    if use_events.lower() == "yes":
        num_events = int(input("Enter the number of events: "))
        for i in range(num_events):
            date = input(f"Enter event date {i + 1} (YYYY-MM-DD): ")
            description = input(f"Enter event description {i + 1}: ")
            events[date] = description

    plot_assets_with_highlights(target_asset, related_assets, start_date, end_date,
                                events, average_related=average_related, ma_window=ma_window)
