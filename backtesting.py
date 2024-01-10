import os

print("Credentials Path:", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/marinaschmid/Library/CloudStorage/GoogleDrive-schmid.squad@gmail.com/My Drive/Trading/profit-cycles-185eecd0c62f.json"


import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from prettytable import PrettyTable


# Constants
MIN_DAYS = 20
MAX_DAYS = 60
LOOK_AHEAD_DAYS = 100
YEARS_BACK = 10

# Helper function definitions
# Function to safely adjust date for cross-year patterns
def adjust_cross_year_date(date, year):
    try:
        new_date = date.replace(year=year)
    except ValueError:
        # Handle February 29th in leap years by moving to the last day of February
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            # Leap year
            new_date = date.replace(year=year, day=29)
        else:
            # Non-leap year
            new_date = date.replace(year=year, day=28)
    return new_date

# Function to analyze a pattern and determine if it's bullish or bearish
def analyze_pattern(data, start_date, end_date):
    bullish_years = 0
    bearish_years = 0

    for year in range(start_date.year - YEARS_BACK, start_date.year):
        yearly_start_date = adjust_cross_year_date(start_date, year)
        yearly_end_date = adjust_cross_year_date(end_date, year)

        if yearly_end_date < yearly_start_date:
            yearly_end_date = adjust_cross_year_date(end_date, year + 1)

        if yearly_start_date not in data.index or yearly_end_date not in data.index:
            continue  # Skip if dates are not in the dataset

        start_price = data.at[yearly_start_date, 'Adj Close']
        end_price = data.at[yearly_end_date, 'Adj Close']

        if end_price > start_price:
            bullish_years += 1
        elif end_price < start_price:
            bearish_years += 1

    # Determine if the pattern is consistently bullish or bearish
    if bullish_years == YEARS_BACK or bearish_years == YEARS_BACK:
        return ['10/10', 'bullish' if bullish_years == YEARS_BACK else 'bearish']
    #elif bullish_years == YEARS_BACK - 1 or bearish_years == YEARS_BACK - 1:
        #return ['9/10', 'bullish' if bullish_years == YEARS_BACK - 1 else 'bearish']
    else:
        return 'None'


# Function to get yearly details of a pattern
def get_yearly_details(data, start_date, end_date):
    details = []
    for year in range(start_date.year - YEARS_BACK, start_date.year):
        yearly_start_date = adjust_cross_year_date(start_date, year)
        yearly_end_date = adjust_cross_year_date(end_date, year)

        if yearly_end_date < yearly_start_date:
            yearly_end_date = adjust_cross_year_date(end_date, year + 1)

        if yearly_start_date in data.index and yearly_end_date in data.index:
            start_price = data.at[yearly_start_date, 'Adj Close']
            end_price = data.at[yearly_end_date, 'Adj Close']
            profit = end_price - start_price
            profit_percent = (profit / start_price) * 100
            max_price = data.loc[yearly_start_date:yearly_end_date, 'Adj Close'].max()
            min_price = data.loc[yearly_start_date:yearly_end_date, 'Adj Close'].min()
            max_rise_percent = ((max_price - start_price) / start_price) * 100
            max_drop_percent = ((start_price - min_price) / start_price) * 100

            details.append({
                'year': year,
                'start_date': yearly_start_date,
                'end_date': yearly_end_date,
                'start_price': start_price,
                'end_price': end_price,
                'profit': profit,
                'profit_percent': profit_percent,
                'max_rise_percent': max_rise_percent,
                'max_drop_percent': max_drop_percent
            })
    return details

# Modified function to backtest patterns for the entire previous year
def backtest_patterns(patterns, data_for_year, year):
    backtest_results = []
    for pattern in patterns:
        ticker = pattern['ticker']
        pattern_type = pattern['pattern_type']
        stock_data = data_for_year.get(ticker)

        if stock_data is None or stock_data.empty:
            continue  # Skip if no data available for this ticker

        # Convert pattern dates to the backtest year
        start_date = pattern['start_date'].replace(year=year)
        end_date = pattern['end_date'].replace(year=year)

        # Adjust for leap year
        start_date = adjust_cross_year_date(start_date, year)
        end_date = adjust_cross_year_date(end_date, year)

        if end_date < start_date:
            end_date = adjust_cross_year_date(end_date, year + 1)

        if start_date not in stock_data.index or end_date not in stock_data.index:
            continue  # Skip if dates are not in the dataset

        start_price = stock_data.at[start_date, 'Adj Close']
        end_price = stock_data.at[end_date, 'Adj Close']

        if pattern_type == 'bullish':
            profit = end_price - start_price
        elif pattern_type == 'bearish':
            # For bearish patterns, profit is calculated differently
            profit = start_price - end_price
        else:
            profit = 0  # No profit for undefined pattern types

        profit_percent = (profit / start_price) * 100 if start_price != 0 else 0

        backtest_results.append({
            'ticker': ticker,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'start_price': start_price,
            'end_price': end_price,
            'return_dollar': profit,
            'return_percent': profit_percent
        })

    return backtest_results




def serialize_pattern(pattern):
    # Convert pattern dates to string format
    serialized_pattern = pattern.copy()
    serialized_pattern['start_date'] = pattern['start_date'].strftime('%Y-%m-%d')
    serialized_pattern['end_date'] = pattern['end_date'].strftime('%Y-%m-%d')
    for detail in serialized_pattern['yearly_details']:
        detail['start_date'] = detail['start_date'].strftime('%Y-%m-%d')
        detail['end_date'] = detail['end_date'].strftime('%Y-%m-%d')
    return serialized_pattern


def download_and_process_data(year):
    tickers = ['GC=F', 'SI=F', 'PL=F', 'HG=F', 'CL=F', 'HO=F', 'RB=F', 'NG=F', 'ZC=F', 'ZW=F', 'ZS=F', 'ZM=F', 'ZL=F']
    patterns = []

    for ticker in tickers:
        try:
            print(f"Processing {ticker}...")
            stock_data = yf.download(ticker, period="max")
            stock_name = yf.Ticker(ticker).info["shortName"]

            if stock_data.empty:
                print(f"No data found for {ticker}, skipping.")
                continue

            stock_data = stock_data.asfreq('D', method='bfill')

            # Adjust start and end dates to cover the entire year
            start_year_date = pd.Timestamp(datetime(year, 1, 1))
            end_year_date = pd.Timestamp(datetime(year, 12, 31))

            # Iterate over each day of the year
            for start_date in pd.date_range(start_year_date, end_year_date):
                for duration in range(MIN_DAYS, MAX_DAYS + 1):
                    end_date = start_date + timedelta(days=duration)
                    if end_date > end_year_date:
                        break
                    pattern_type = analyze_pattern(stock_data, start_date, end_date)
                    if pattern_type not in ['None']:
                        yearly_details = get_yearly_details(stock_data, start_date, end_date)
                        average_return = sum([d['profit_percent'] if pattern_type[1] == 'bullish' else -d['profit_percent'] for d in yearly_details]) / len(yearly_details)
                        patterns.append({
                            'ticker': ticker,
                            'name': stock_name,
                            'start_date': start_date,
                            'end_date': end_date,
                            'pattern_type': pattern_type[1],
                            'ratio': pattern_type[0],
                            'average_return_percent': average_return,
                            'yearly_details': yearly_details
                        })
        except Exception as e:
            print(f"Error processing {ticker}: {e}, skipping.")
            continue

    patterns.sort(key=lambda x: x['start_date'])
    # Process all patterns to get the best one for each start date
    best_patterns = {}

    for pattern in patterns:
        key = f"{pattern['ticker']}_{pattern['start_date'].strftime('%Y-%m-%d')}"
        if key in best_patterns:
            if pattern['average_return_percent'] > best_patterns[key]['average_return_percent']:
                best_patterns[key] = pattern
        else:
            best_patterns[key] = pattern
    return best_patterns

def download_data_for_year(tickers, year):
    data_for_year = {}
    for ticker in tickers:
        try:
            data = yf.download(ticker, start=f"{year}-01-01", end=f"{year}-12-31")
            data_for_year[ticker] = data
        except Exception as e:
            print(f"Error downloading data for {ticker}: {e}")
    return data_for_year


# Print results using PrettyTable
def print_results(backtest_results):
    table = PrettyTable()
    table.field_names = ["Ticker", "Start Date", "End Date", "Start Price", "End Price", "Return ($)", "Return (%)"]

    for result in backtest_results:
        table.add_row([
            result['ticker'],
            result['start_date'],
            result['end_date'],
            f"${result['start_price']:.2f}",
            f"${result['end_price']:.2f}",
            f"${result['return_dollar']:.2f}",
            f"{result['return_percent']:.2f}%"
        ])

    print(table)


# Local testing harness
if __name__ == "__main__":
    # Main function and testing harness
    year_to_process = 2022  # Example year
    best_patterns = download_and_process_data(year_to_process)

    # Correctly extract tickers from the values of the best_patterns dictionary
    tickers = [pattern['ticker'] for pattern in best_patterns.values()]
    tickers = list(set(tickers))  # Remove duplicates

    # Download data for backtesting year
    backtest_year = year_to_process + 1
    data_for_backtest_year = download_data_for_year(tickers, backtest_year)

    # Backtest patterns
    backtest_results = backtest_patterns(best_patterns.values(), data_for_backtest_year, backtest_year)

    # Print results using the new function
    print_results(backtest_results)


