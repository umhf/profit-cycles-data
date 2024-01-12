import os

print("Credentials Path:", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/marinaschmid/Library/CloudStorage/GoogleDrive-schmid.squad@gmail.com/My Drive/Trading/profit-cycles-185eecd0c62f.json"


import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from google.cloud import firestore
from utils.processing import readFromFile, saveToFile, adjust_cross_year_date, get_yearly_details, serialize_pattern, filter_patterns, saveToLocalCSV, filter_30_day_best_patterns

# Constants
MIN_DAYS = 20
MAX_DAYS = 60
LOOK_AHEAD_DAYS = 365
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
        elif end_price < start_price:
            bearish_years += 1

    # Determine if the pattern is consistently bullish or bearish
    if bullish_years == YEARS_BACK or bearish_years == YEARS_BACK:
        return ['10/10', 'bullish' if bullish_years == YEARS_BACK else 'bearish']
    else:
        return 'None'
    """ elif bullish_years == YEARS_BACK - 1 or bearish_years == YEARS_BACK - 1:
        return ['9/10', 'bullish' if bullish_years == YEARS_BACK - 1 else 'bearish'] """






import json


def download_and_process_data(tickers):
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

            today = pd.Timestamp(datetime.now().date())

            for day_offset in range(LOOK_AHEAD_DAYS):
                start_date = today + timedelta(days=day_offset)
                for duration in range(MIN_DAYS, MAX_DAYS + 1):
                    end_date = start_date + timedelta(days=duration)
                    if end_date > today + timedelta(days=LOOK_AHEAD_DAYS):
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

# Write to a file
""" with open('stock_patterns.json', 'w') as file:
    file.write(json_output)
 """

# Function to upload data to Firestore
def upload_to_firestore(json_data):
    db = firestore.Client()
    for item in json_data:
        # Create a document reference with a custom ID in a collection named 'stock_patterns'
        doc_ref = db.collection('stock_patterns').document(f"{item['ticker']}_{item['start_date']}")
        # Set the document with the data
        doc_ref.set(item)


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
    tickers = [ticker.replace('.', '-') for ticker in sp500_tickers]
    tickers = ['MAR','ATO','AON']



    best_patterns_dict = download_and_process_data(tickers)

        #saveToFile(best_patterns_dict, year_to_process)



    # Convert the dictionary values to a list for filtering
    best_patterns_list = list(best_patterns_dict.values())

    # Apply the filter to the list of patterns
    filtered_patterns = filter_patterns(best_patterns_list)
    filtered_patterns = filter_30_day_best_patterns(filtered_patterns)
    
    saveToLocalCSV(filtered_patterns)

     # Serialize and convert best patterns to JSON
    #json_data = [serialize_pattern(pattern) for pattern in filtered_patterns.values()]
    #json_output = json.dumps(json_data, indent=4)
    #upload_to_firestore(filtered_patterns)