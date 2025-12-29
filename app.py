import pandas as pd
import requests
import os
import jdatetime
from datetime import datetime, timedelta
import math
from flask import Flask, render_template, jsonify
import threading
import time

app = Flask(__name__)

# Global variable to store the latest data
latest_df = pd.DataFrame()
last_update_time = None
update_interval = 60  # Update every 60 seconds

def fetch_data():
    """Your data fetching function"""
    global latest_df, last_update_time
    
    try:
        print(f"[{datetime.now()}] Fetching data...")
        
        # ================ START: YOUR ORIGINAL CODE ================
        # Download data
        url = "https://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx"
        response = requests.get(url, timeout=10)
        data = response.text

        ## Split data
        rows = data.split('@')
        all_stocks = rows[2].split(';')

        # Target list
        target_list = [
            'اخابر', 'آساس', 'اطلس', 'امين شهر', 'اهرم',
            'بساما', 'پادا', 'تاصيكو', 'توان', 'تيام', 'جهش',
            'خاور', 'خبهمن', 'خساپا', 'خگستر', 'خودران', 'خودرو', 'ذوب', 'رويين',
            'سامان', 'شپنا', 'شتاب', 'شستا', 'طعام', 'فارس', 'فرابورس', 'فزر', 'فصبا',
            'فملي', 'فولاد', 'كاريس', 'موج', 'نارنج اهرم', 'وبصادر', 'وبملت', 'وتجارت', 'وتعاون'
            , 'وغدير'
        ]

        # Allowed values for part[22]
        allowed_codes = {'311', '312', '320', '321'}

        # Filter stocks
        filtered_stocks = []
        for stock in all_stocks:
            if stock:
                parts = stock.split(',')
                if len(parts) > 22:  # Need at least 23 columns for parts[22]
                    col3 = parts[2]  # COLUMN 3
                    col22 = parts[22]  # COLUMN 23 (industry code)
                    
                    # Check conditions:
                    # 1. Starts with ض or ط AND col22 is 311/312/320/321
                    starts_with_condition = col3.startswith(('ض', 'ط')) and col22 in allowed_codes
                    
                    # 2. OR exactly matches a target in target_list
                    exact_match_condition = any(target == col3 for target in target_list)
                    
                    if starts_with_condition or exact_match_condition:
                        # Keep ONLY the columns we want: 2, 3, 6, 7
                        # parts[2], parts[3], parts[6], parts[7]
                        selected_columns = [parts[22], parts[2], parts[3], parts[6], parts[7]]
                        filtered_stocks.append(selected_columns)

        # Create DataFrame with column names
        df = pd.DataFrame(filtered_stocks, columns=[
            'Coding',          # Will be int64
            'Short_Symbol',    # String
            'Full_Name',       # String
            'Close_Price',     # Will be int64
            'Last_Price'       # Will be int64
        ])

        # ======== CONVERT TO NUMERIC (int64) ========
        # Step 1: Convert to numeric (float)
        df['Coding'] = pd.to_numeric(df['Coding'], errors='coerce')
        df['Close_Price'] = pd.to_numeric(df['Close_Price'], errors='coerce')
        df['Last_Price'] = pd.to_numeric(df['Last_Price'], errors='coerce')

        # Step 2: Fill NaN with 0 and convert to int64
        df['Coding'] = df['Coding'].fillna(0).astype('int64')
        df['Close_Price'] = df['Close_Price'].fillna(0).astype('int64')
        df['Last_Price'] = df['Last_Price'].fillna(0).astype('int64')

        types = []
        Strike_Price = []
        expiration_dates = []

        # Check EACH ROW
        for i in range(len(df)):
            symbol = df.loc[i, 'Full_Name']
            Coding = df.loc[i, 'Coding']
            # Now compare with integers
            if Coding in [311, 312, 320, 321]:
                parts = symbol.split('-')         # Split this row
                if parts[0].startswith("اختيارخ"):
                    types.append("Call")    
                else:
                    types.append("Put")
                Strike_Price.append(parts[1] if len(parts) > 1 else None)
                expiration_dates.append(parts[2] if len(parts) > 2 else 'NA')
            else:
                types.append('پایه')
                Strike_Price.append(None)
                expiration_dates.append('NA')

        # Add to DataFrame
        df['Type'] = types

        # ======== FIX: CONVERT STRIKE PRICE PROPERLY ========
        # Convert list to pandas Series first
        strike_series = pd.Series(Strike_Price)
        df['Strike_Price'] = pd.to_numeric(strike_series, errors='coerce').fillna(0).astype('int64')

        df['Expiration_date'] = expiration_dates

        # setting up underlying stock
        underlying = []
        
        for i in range(len(df)):
            Full_Name = df.loc[i, 'Full_Name']
            Coding = df.loc[i, 'Coding']
            if Coding in [311, 312, 320, 321]:
                found_match = None
                for target in target_list:
                    if  target in Full_Name:
                        found_match = target
                        break
            # If not found in target_list, check for "نارنج"
                if not found_match and "نارنج" in Full_Name:
                    found_match = "نارنج اهرم"
            else:
                found_match = None
            underlying.append(found_match if found_match else None)
        df['Underlying_Stock'] = underlying

        # finding underlying data and assigning to option
        Stock_prices = {}
        for i in range(len(df)):
            if df.loc[i,'Coding'] not in [311, 312, 320, 321]:
                name = df.loc[i,'Short_Symbol']
                # Store as int
                Stock_prices[name] = {
                    'Close' : int(df.loc[i,'Close_Price']) if pd.notna(df.loc[i,'Close_Price']) else 0,
                    'Last' : int(df.loc[i,'Last_Price']) if pd.notna(df.loc[i,'Last_Price']) else 0
                }

        #### now we go to data frame and for compare the underlying asset of the option
        ####to dict and put the values in the new columns
        Stock_last = []
        Stock_Close = []
        for i in range(len(df)):
            Coding = df.loc[i, 'Coding']
            stock_name = df.loc[i,'Underlying_Stock']
            if Coding in [311, 312, 320, 321] and stock_name in Stock_prices:
                Last = Stock_prices[stock_name]['Last']
                Close = Stock_prices[stock_name]['Close']
            else:
                Last = None
                Close = None
            Stock_last.append(Last)
            Stock_Close.append(Close)
        
        # ======== FIX: CONVERT STOCK PRICES PROPERLY ========
        # Convert lists to Series first
        stock_last_series = pd.Series(Stock_last)
        stock_close_series = pd.Series(Stock_Close)

        df['Stock_Last_Price'] = pd.to_numeric(stock_last_series, errors='coerce').fillna(0).astype('int64')
        df['Stock_Close_Price'] = pd.to_numeric(stock_close_series, errors='coerce').fillna(0).astype('int64')

        # standardizing the EXP date and converting to georgian and calculating DTE

        dte_list = []  # Days To Expiration

        for i in range(len(df)):
            date_str = str(df.loc[i,'Expiration_date'])
            Coding = df.loc[i, 'Coding']
            # If 'NA' or not an option
            if date_str == 'NA' or Coding not in [311, 312, 320, 321]:
                dte_list.append('NA')
                continue
            
            clean_date = date_str.replace("/", "")
            if len(clean_date) == 6:
                year = int("14" + clean_date[:2])
                month = int(clean_date[2:4])
                day = int(clean_date[4:6])
            elif len(clean_date) == 8:
                year = int(clean_date[:4])
                month = int(clean_date[4:6])
                day = int(clean_date[6:8])
            else:
                dte_list.append('NA')
                continue
                
            try:
                # Persian date → Georgian date
                persian_date = jdatetime.date(year, month, day)
                georgian_date = persian_date.togregorian()
                
                # Today's date
                today = datetime.now().date()
                
                # DTE calculation
                dte = (georgian_date - today).days
                
                # Only keep positive DTE
                dte_list.append(dte if dte >= 0 else 0)
                
            except:
                dte_list.append('NA')

        # Add DTE column
        df['DTE'] = dte_list
        
        # required margin calculations
        margin = []
        contract_size = 1000
        a_factor = 0.2
        b_factor = 0.1
        c_factor = 10000

        for i in range(len(df)):
            Coding = df.loc[i, 'Coding']
            if Coding in [311, 312, 320, 321]:
                # Get values (already int64, no NaN)
                stock_price = df.loc[i,'Stock_Close_Price']
                strike_price = df.loc[i,'Strike_Price']
                
                ASpot_price = a_factor * stock_price * contract_size
                
                if df.loc[i,'Type'] == "Call":
                    OTM_amount = (strike_price - stock_price) * contract_size
                    ####for call 
                    if OTM_amount > 0:
                        if OTM_amount<ASpot_price:
                            A = ASpot_price - OTM_amount+10000
                        else:
                            A = ASpot_price - OTM_amount
                    else:
                        A = ASpot_price
                        
                    BSpot_price = b_factor * stock_price * contract_size
                    ###for put 
                else:
                    OTM_amount = (stock_price - strike_price) * contract_size
                    if OTM_amount > 0:
                        if OTM_amount<ASpot_price:
                            A = ASpot_price - OTM_amount-10000
                        else:
                            A = ASpot_price - OTM_amount
                    else:
                        A = ASpot_price
                        
                    BSpot_price = (b_factor * strike_price * contract_size)+10000
                    
                lm = max(A, BSpot_price, 0)
                Fmargin = math.ceil(lm / c_factor) * c_factor
                margin.append(Fmargin)
            else:
                margin.append(0)  # 0 for non-options

        df['Margin'] = margin

        ## daily return calc
        Daily_return_list = []

        for i in range(len(df)):
            if df.loc[i, 'Coding'] in [311, 312, 320, 321]:
                margin = df.loc[i, 'Margin']
                opt_price = df.loc[i, 'Last_Price']
                dte = df.loc[i, 'DTE']
                
                if margin > 0 and opt_price > 0 and dte != 'NA' and dte > 0:
                    premium = opt_price * 1000
                    
                    if margin > premium:
                        daily_return_pct = (premium / (margin - premium)) / dte * 100
                    else:
                        daily_return_pct = (premium / margin) / dte * 100
                    
                    # Round to 2 decimals
                    daily_return_pct = round(daily_return_pct, 2)
                else:
                    daily_return_pct = 0.00
            else:
                daily_return_pct = None
            
            Daily_return_list.append(daily_return_pct)

        df['Daily_Return_%'] = Daily_return_list

        # OTM percentage
        OTM_percentag_list = []

        for i in range(len(df)):
            if df.loc[i, 'Coding'] in [311, 312, 320, 321]:
                stock_price = df.loc[i, 'Stock_Close_Price']  # FIXED: Use correct column name
                strike_price = df.loc[i, 'Strike_Price']
                
                # Avoid division by zero
                if stock_price > 0:
                    if df.loc[i,'Type'] == "Call":
                        otm_value = (strike_price - stock_price) / stock_price * 100
                    else:
                        otm_value = (stock_price - strike_price) / stock_price * 100
                    
                    otm_value = round(otm_value, 2)
                else:
                    otm_value = None
            else:
                otm_value = None  # FIXED: Append None for non-options
            
            OTM_percentag_list.append(otm_value)

        df['OTM%'] = OTM_percentag_list
        
        # ================ END: YOUR ORIGINAL CODE ================
        
        # Save to global variable
        latest_df = df.copy()
        last_update_time = datetime.now()
        
        print(f"[{last_update_time}] Data updated. Rows: {len(latest_df)}")
        
    except Exception as e:
        print(f"Error in fetch_data: {e}")
        import traceback
        traceback.print_exc()

def auto_update():
    """Background thread to auto-update data"""
    while True:
        fetch_data()
        time.sleep(update_interval)

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/data')
def get_data():
    """API endpoint to get JSON data"""
    if latest_df.empty:
        return jsonify({'error': 'No data available yet. First update in progress...'})
    
    # Convert to JSON
    data = latest_df.to_dict('records')
    
    return jsonify({
        'data': data,
        'last_update': last_update_time.strftime('%Y-%m-%d %H:%M:%S') if last_update_time else 'Never',
        'total_rows': len(latest_df)
    })

@app.route('/filter/<column>/<min_val>/<max_val>')
def filter_data(column, min_val, max_val):
    """Filter data by column range"""
    if latest_df.empty:
        return jsonify({'error': 'No data available'})
    
    try:
        min_val = float(min_val)
        max_val = float(max_val)
        
        # Filter the dataframe
        filtered_df = latest_df[
            (latest_df[column] >= min_val) & 
            (latest_df[column] <= max_val)
        ]
        
        return jsonify({
            'data': filtered_df.to_dict('records'),
            'filtered_rows': len(filtered_df),
            'total_rows': len(latest_df),
            'filter': f'{column}: {min_val} to {max_val}'
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/sort/<column>/<order>')
def sort_data(column, order):
    """Sort data by column"""
    if latest_df.empty:
        return jsonify({'error': 'No data available'})
    
    try:
        ascending = order.lower() == 'asc'
        sorted_df = latest_df.sort_values(by=column, ascending=ascending)
        
        return jsonify({
            'data': sorted_df.to_dict('records'),
            'sorted_by': f'{column} ({order})'
        })
    except Exception as e:
        return jsonify({'error': str(e)})

# Start background thread when module loads
print("Starting background update thread...")
update_thread = threading.Thread(target=auto_update, daemon=True)
update_thread.start()

# Initial fetch
print("Performing initial data fetch...")
fetch_data()