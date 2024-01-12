def saveToFile(best_patterns, year):
    import pickle
    # Assuming best_patterns is a complex object
    with open(f'best_patterns_{year}.pkl', 'wb') as file:
        pickle.dump(best_patterns, file)

def readFromFile(year):
    import pickle

    with open(f'best_patterns_{year}.pkl', 'rb') as file:
        return pickle.load(file)

def saveToGoogle(patterns, year):
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import time

    # Use creds to create a client to interact with the Google Drive API
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials/seasonal-stock-patterns-6aaf5f253e1d.json', scope)
    client = gspread.authorize(creds)

    # Open the spreadsheet and select the first sheet
    spreadsheet_name = 'seasonal patterns'  # Replace with your spreadsheet name
    spreadsheet = client.open(spreadsheet_name)

    # Check if the tab exists, and if not, create it
    try:
        sheet = spreadsheet.worksheet(str(year))
    except gspread.exceptions.WorksheetNotFound:
        # Creating a new worksheet with 1000 rows and 26 columns (default)
        sheet = spreadsheet.add_worksheet(title=str(year), rows="1000", cols="26")

    # Example: Writing data to the first row
    header = ["Ticker", "Start Date", "End Date", "Start Price", "End Price", "Return ($)", "Return (%)"]
    sheet.append_row(header)

    # Write patterns and their details to the sheet
    row_count = 0

    for pattern in patterns:
        # Prepare the initial part of the row with general pattern information
        row = [
            pattern['ticker'],
            pattern['start_date'],
            pattern['end_date'],
            f"${pattern['start_price']:.2f}",
            f"${pattern['end_price']:.2f}",
            f"${pattern['return_dollar']:.2f}",
            f"{pattern['return_percent']:.2f}%"
        ]


        # Append the row to the sheet
        sheet.append_row(row)
        row_count += 1

        # Pause every 50 rows to comply with Google Sheets API rate limits
        if row_count % 50 == 0:
            print("Pausing for a minute to comply with rate limits...")
            time.sleep(60)

def adjust_cross_year_date(date, year):
    try:
        new_date = date.replace(year=year)
    except ValueError:
        # Handle February 29th in leap years by moving to the last day of February
        new_date = date.replace(year=year, day=28)
    return new_date

# Function to get yearly details of a pattern
def get_yearly_details(data, start_date, end_date, YEARS_BACK):
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
                'start_date': yearly_start_date.strftime('%Y-%m-%d'),
                'end_date': yearly_end_date.strftime('%Y-%m-%d'),
                'start_price': round(start_price, 2),
                'end_price': round(end_price, 2),
                'profit': round(profit, 2),
                'profit_percent': round(profit_percent, 2),
                'max_rise_percent': round(max_rise_percent, 2),
                'max_drop_percent': round(max_drop_percent, 2)
            })
    return details

def serialize_pattern(pattern):
    # Convert pattern dates to string format
    serialized_pattern = pattern.copy()
    serialized_pattern['start_date'] = pattern['start_date'].strftime('%Y-%m-%d')
    serialized_pattern['end_date'] = pattern['end_date'].strftime('%Y-%m-%d')
    for detail in serialized_pattern['yearly_details']:
        detail['start_date'] = detail['start_date'].strftime('%Y-%m-%d')
        detail['end_date'] = detail['end_date'].strftime('%Y-%m-%d')
    return serialized_pattern

def filter_patterns(patterns):
    # Sort patterns by ticker, start date, and end date
    patterns.sort(key=lambda x: (x['ticker'], x['start_date'], x['end_date']))

    filtered_patterns = []
    prev_pattern = None

    for pattern in patterns:
        if prev_pattern and pattern['ticker'] == prev_pattern['ticker']:
            # Check if the start dates are adjacent or end dates are the same
            start_dates_adjacent = (pattern['start_date'] - prev_pattern['start_date']).days <= 1
            end_dates_same = pattern['end_date'] == prev_pattern['end_date']

            if start_dates_adjacent or end_dates_same:
                # Keep the more profitable pattern
                if pattern['average_return_percent'] > prev_pattern['average_return_percent']:
                    prev_pattern = pattern
                # Skip adding this pattern
                continue

        filtered_patterns.append(prev_pattern) if prev_pattern else None
        prev_pattern = pattern

    # Add the last pattern if it wasn't added
    if prev_pattern and (not filtered_patterns or filtered_patterns[-1] != prev_pattern):
        filtered_patterns.append(prev_pattern)

    return filtered_patterns


import csv

def saveToLocalCSV(patterns, year=None):
    # Set the filename for the CSV file, omitting the year if it's not provided
    if year is None:
        csv_filename = 'seasonal_patterns.csv'
    else:
        csv_filename = f'seasonal_patterns_{year}.csv'

    # Define the header of the CSV file
    header = ["Ticker", "Start Date", "End Date", "Average Return (%)", "Yearly Details"]

    # Open the file in write mode and create a csv writer object
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)

        # Write the header
        writer.writerow(header)

        # Write patterns and their details to the CSV file
        for pattern in patterns:
            row = [
                pattern['ticker'],
                pattern['start_date'].strftime('%Y-%m-%d'),
                pattern['end_date'].strftime('%Y-%m-%d'),
                round(pattern['average_return_percent'], 2),
                pattern['yearly_details']
            ]
            writer.writerow(row)

    print(f"Data saved to {csv_filename}")



def filter_30_day_best_patterns(patterns):
    from collections import defaultdict
    from datetime import timedelta

    # Group patterns by ticker
    grouped_patterns = defaultdict(list)
    for pattern in patterns:
        grouped_patterns[pattern['ticker']].append(pattern)

    # Sort patterns within each group and apply filtering
    filtered_patterns = []
    for ticker, ticker_patterns in grouped_patterns.items():
        ticker_patterns.sort(key=lambda x: x['start_date'])

        window_start = None
        best_pattern_in_window = None

        for pattern in ticker_patterns:
            if window_start is None or pattern['start_date'] > window_start + timedelta(days=30):
                # New window starts
                if best_pattern_in_window:
                    filtered_patterns.append(best_pattern_in_window)
                window_start = pattern['start_date']
                best_pattern_in_window = pattern
            else:
                # Within the window, keep the best pattern
                if pattern['average_return_percent'] > best_pattern_in_window['average_return_percent']:
                    best_pattern_in_window = pattern

        # Add the best pattern of the last window for this ticker
        if best_pattern_in_window:
            filtered_patterns.append(best_pattern_in_window)

    return filtered_patterns

def saveToLocalCSVBacktesting(patterns, year=None):
    # Set the filename for the CSV file, omitting the year if it's not provided
    if year is None:
        csv_filename = 'backtesting.csv'
    else:
        csv_filename = f'backtesting_{year}.csv'

    # Define the header of the CSV file
    header = ["Ticker", "Start Date", "End Date", "Start Price", "End Price", "Return ($)", "Return (%)"]

    # Open the file in write mode and create a csv writer object
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)

        # Write the header
        writer.writerow(header)

        # Write patterns and their details to the CSV file
        for pattern in patterns:
            row = [
                pattern['ticker'],
                pattern['start_date'],
                pattern['end_date'],
                round(pattern['start_price'], 2),
                round(pattern['end_price'], 2),
                round(pattern['return_dollar'], 2),
                round(pattern['return_dollar'], 2),
                pattern['return_percent']
            ]
            writer.writerow(row)

    print(f"Data saved to {csv_filename}")