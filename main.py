import os

print("Credentials Path:", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/marinaschmid/Library/CloudStorage/GoogleDrive-schmid.squad@gmail.com/My Drive/Trading/profit-cycles-185eecd0c62f.json"

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from google.cloud import firestore

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
    elif bullish_years == YEARS_BACK - 1 or bearish_years == YEARS_BACK - 1:
        return ['9/10', 'bullish' if bullish_years == YEARS_BACK - 1 else 'bearish']
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



import json

def serialize_pattern(pattern):
    # Convert pattern dates to string format
    serialized_pattern = pattern.copy()
    serialized_pattern['start_date'] = pattern['start_date'].strftime('%Y-%m-%d')
    serialized_pattern['end_date'] = pattern['end_date'].strftime('%Y-%m-%d')
    for detail in serialized_pattern['yearly_details']:
        detail['start_date'] = detail['start_date'].strftime('%Y-%m-%d')
        detail['end_date'] = detail['end_date'].strftime('%Y-%m-%d')
    return serialized_pattern
def download_and_process_data():
    tickers = ['GC=F', 'SI=F', 'PL=F', 'HG=F', 'CL=F', 'HO=F', 'RB=F', 'NG=F', 'ZC=F', 'ZW=F', 'ZS=F', 'ZM=F', 'ZL=F', 'CC=F', 'CT=F', 'KC=F', 'SB=F', 'LE=F', 'HE=F', '6A=F', '6B=F', '6C=F', '6E=F', '6J=F', '6S=F']
    #tickers = ['GC=F', 'SI=F', 'PL=F', 'HG=F', 'CL=F', 'HO=F', 'RB=F', 'NG=F', 'ZC=F', 'ZW=F', 'ZS=F', 'ZM=F', 'ZL=F', 'CC=F', 'CT=F', 'KC=F', 'SB=F', 'LE=F', 'HE=F']
    # tickers = ['RB=F']


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
                        yearly_details = get_yearly_details(stock_data, start_date, end_date)
                        average_return = sum([d['profit_percent'] if pattern_type[1] == 'bullish' else -d['profit_percent'] for d in yearly_details]) / len(yearly_details)
                        #average_return = sum([d['profit_percent'] for d in yearly_details]) / len(yearly_details)
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

    # Serialize and convert best patterns to JSON
    json_data = [serialize_pattern(pattern) for pattern in best_patterns.values()]
    #json_output = json.dumps(json_data, indent=4)
    return json_data

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

def main(event, context):
    json_data = download_and_process_data()
    # Call the upload function
    upload_to_firestore(json_data)

    print(f"This function was triggered by messageId {context.event_id} published at {context.timestamp}")

# Local testing harness
if __name__ == "__main__":
    json_data = download_and_process_data()
    upload_to_firestore(json_data)