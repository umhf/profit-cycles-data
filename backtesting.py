import os

print("Credentials Path:", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/marinaschmid/Library/CloudStorage/GoogleDrive-schmid.squad@gmail.com/My Drive/Trading/profit-cycles-185eecd0c62f.json"


import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from prettytable import PrettyTable

from utils.summary import calculate_max_drawdown
from utils.processing import readFromFile, saveToFile, adjust_cross_year_date, get_yearly_details, filter_patterns, filter_30_day_best_patterns, saveToLocalCSVBacktesting


# Constants
MIN_DAYS = 14
MAX_DAYS = 30
YEARS_BACK = 10

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
        #elif end_price < start_price:
         #   bearish_years += 1

    if bullish_years == YEARS_BACK:
        return ['10/10', 'bullish']
    # Determine if the pattern is consistently bullish or bearish
    #if bullish_years == YEARS_BACK or bearish_years == YEARS_BACK:
        #return ['10/10', 'bullish' if bullish_years == YEARS_BACK else 'bearish']
    #elif bullish_years == YEARS_BACK - 1 or bearish_years == YEARS_BACK - 1:
        #return ['9/10', 'bullish' if bullish_years == YEARS_BACK - 1 else 'bearish']
    else:
        return 'None'

def backtest_patterns(patterns, data_for_year, year, trade_amount, initial_capital):
    backtest_results = []
    total_return_dollar = 0
    capital = initial_capital
    num_trades = 0  # Counter for number of trades

    for pattern in patterns:
        ticker = pattern['ticker']
        pattern_type = pattern['pattern_type']
        stock_data = data_for_year.get(ticker)

        if stock_data is None or stock_data.empty or capital < trade_amount:
            continue  # Skip if no data or not enough capital

        # Adjust dates for leap year
        start_date = adjust_cross_year_date(pattern['start_date'], year)
        end_date = adjust_cross_year_date(pattern['end_date'], year)

        if end_date < start_date:
            end_date = adjust_cross_year_date(end_date, year + 1)

        if start_date not in stock_data.index or end_date not in stock_data.index:
            continue  # Skip if dates are not in the dataset

        # Calculate the max rise and max drop
        period_data = stock_data.loc[start_date:end_date, 'Adj Close']
        max_price = period_data.max()
        min_price = period_data.min()
        start_price = period_data.iloc[0]
        end_price = period_data.iloc[-1]

        max_rise_percent = ((max_price - start_price) / start_price) * 100 if start_price != 0 else 0
        max_drop_percent = ((start_price - min_price) / start_price) * 100 if start_price != 0 else 0

        # Calculate profit and profit percent
        profit = end_price - start_price if pattern_type == 'bullish' else start_price - end_price
        profit_percent = (profit / start_price) * 100 if start_price != 0 else 0

        trade_return = trade_amount * (profit_percent / 100)
        if trade_return != 0:  # Check to avoid counting non-trades
            num_trades += 1
            capital += trade_return
            total_return_dollar += trade_return

        backtest_results.append({
            'ticker': ticker,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'start_price': start_price,
            'end_price': end_price,
            'return_dollar': trade_return,
            'return_percent': profit_percent,
            'max_rise_percent': max_rise_percent,
            'max_drop_percent': max_drop_percent,
            'yearly_details': pattern['yearly_details']
        })

    return backtest_results, total_return_dollar, capital, num_trades




def download_and_process_data(tickers, year): 
    patterns = []
    for ticker in tickers:
        try:
            print(f"Processing {ticker}...")
            stock_data = yf.download(ticker, period="max")
            stock_name = yf.Ticker(ticker).info["shortName"]

            # Check if the data is empty
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
                        yearly_details = get_yearly_details(stock_data, start_date, end_date, YEARS_BACK)
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
def print_results_and_summary(backtest_results, total_return, final_capital, num_trades, initial_capital):
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
    overall_profit_percent = (total_return / initial_capital) * 100 if initial_capital != 0 else 0
    average_return_per_trade = total_return / num_trades if num_trades > 0 else 0
    win_rate = sum(1 for result in backtest_results if result['return_dollar'] > 0) / num_trades * 100 if num_trades > 0 else 0
    capital_over_time = [initial_capital + sum(result['return_dollar'] for result in backtest_results[:i+1]) for i in range(len(backtest_results))]
    max_drawdown = calculate_max_drawdown(capital_over_time)


    # Print summary
    print(f"\nSummary:")
    print(f"Total Return for the Year: ${total_return:.2f}")
    print(f"Final Capital: ${final_capital:.2f}")
    print(f"Number of Trades: {num_trades}")
    print(f"Overall Profit (%): {overall_profit_percent:.2f}%")
    print(f"Average Return per Trade: ${average_return_per_trade:.2f}")
    print(f"Win Rate: {win_rate:.2f}%")
    print(f"Maximum Drawdown: {max_drawdown:.2f}%")







# Local testing harness
if __name__ == "__main__":
    # Main function and testing harness
    # tickers = ['GC=F', 'SI=F', 'PL=F', 'HG=F', 'CL=F', 'HO=F', 'RB=F', 'NG=F', 'ZC=F', 'ZW=F', 'ZS=F', 'ZM=F', 'ZL=F']
    #tickers = ['GC=F', 'SI=F', 'PL=F', 'HG=F', 'CL=F', 'HO=F', 'RB=F', 'NG=F', 'ZC=F', 'ZW=F', 'ZS=F', 'ZM=F', 'ZL=F', 'CC=F', 'CT=F', 'KC=F', 'SB=F', 'LE=F', 'HE=F', '6A=F', '6B=F', '6C=F', '6E=F', '6J=F', '6S=F']
    # GET S&P500
    # Fetch S&P 500 tickers
    import os
    import subprocess
    #export SSL_CERT_FILE=$(python -m certifi) to a python script
    # Get the path to the certificate file using Python's subprocess module
    cert_file = subprocess.check_output(["python3", "-m", "certifi"]).strip().decode()
    # Set the SSL_CERT_FILE environment variable
    os.environ["SSL_CERT_FILE"] = cert_file


    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    sp500_table = pd.read_html(sp500_url, header=0)[0]
    sp500_tickers = sp500_table['Symbol'].tolist()


    year_to_process = 2023  # Example year



    for year_to_process in range(2016, 2024):  # 2023 is exclusive, so it will loop until 2022

        tickers = [ticker.replace('.', '-') for ticker in sp500_tickers]
        useFile = False

        if useFile:
            best_patterns_dict = readFromFile(year_to_process)
        else:
            best_patterns_dict = download_and_process_data(tickers, year_to_process)

            saveToFile(best_patterns_dict, year_to_process)



        # Convert the dictionary values to a list for filtering
        best_patterns_list = list(best_patterns_dict.values())

        # Apply the filter to the list of patterns
        filtered_patterns = filter_patterns(best_patterns_list)
        filtered_patterns = filter_30_day_best_patterns(filtered_patterns)

        # Extract tickers from the filtered patterns
        tickers = [pattern['ticker'] for pattern in filtered_patterns]
        tickers = list(set(tickers))  # Remove duplicates

        # Download data for backtesting year
        data_for_backtest_year = download_data_for_year(tickers, year_to_process)

        # Backtest patterns
        initial_capital = 25000
        trade_amount = 1000
        backtest_results, total_return, final_capital, num_trades = backtest_patterns(
        filtered_patterns, data_for_backtest_year, year_to_process, trade_amount, initial_capital)

        saveToLocalCSVBacktesting(backtest_results, year_to_process)

    #print_results_and_summary(backtest_results, total_return, final_capital, num_trades, initial_capital)
"""     from utils.processing import saveToGoogle
    saveToGoogle(backtest_results, str(backtest_year)) """
    



