import requests
import json
import base64
import hmac
import hashlib
import datetime, time
import pandas as pd
import random
import ta
import configparser
from IPython.display import clear_output
import threading
import yfinance as yf
import numpy as np
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import json
import os
from pathlib import Path
import matplotlib.pyplot as plt
from io import BytesIO
import matplotlib.dates as mdates
import datetime
import threading



def GetGitHubPriceSetupFile():
    file_content=''
    url = 'https://raw.githubusercontent.com/jainkgaurav/MyRepo/main/PriceSetup.ini'
    response = requests.get(url)
    if response.status_code == 200:
        # Content of the file
        file_content = response.text
    return file_content

def ProcessPriceSetupFileToLocal():
    filename = 'SymbolSetup.ini'
    try:
        if(can_rewrite(filename)==1):
            content = GetGitHubPriceSetupFile()
            if len(content)>0:
                with open(filename, 'w') as file:
                     file.write(content)
                     #write_to_log(content)
    except Exception as e:
        write_to_log(f'Error: {e}')
        
def can_rewrite(file_path):
    AllowUpdate=0
    try:
        # Get the last modification timestamp of the file
        last_modified_timestamp = os.path.getmtime(file_path)
        # Get the current time
        current_time = time.time()
        # Calculate the time difference in seconds
        time_difference = current_time - last_modified_timestamp
        # Check if the time difference is more than 1 hour (3600 seconds)
        if time_difference > 60:
           AllowUpdate=1
    except Exception as e:
        write_to_log(f'Error: {e}')
    return AllowUpdate

def data_to_write(content,symbol,filename):
    filename = filename+symbol+'.ini'
    with open(filename, 'w') as file:
        file.write('['+symbol+']\n')
        file.write(symbol+'='+str(content))

def data_to_write(content,symbol):
    filename = symbol+'TrailPrice.ini'
    with open(filename, 'w') as file:
        file.write('['+symbol+']\n')
        file.write(symbol+'='+str(content))


def write_to_log(*args):
    log_file_path = 'gemini_log_ju.txt'
    max_file_size_kb = 10000
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f'{current_time} - {" ".join(map(str, args))}\n'
    # Check file size
    if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > max_file_size_kb * 1024:
        # If file size exceeds the threshold, create a new log file and delete the old one
        create_new_log(log_file_path)
   # Open the log file in append mode
    with open(log_file_path, 'a') as log_file:
        log_file.write(log_entry)


def create_new_log(old_log_path):
    # Create a new log file with a timestamp in the filename
    new_log_path = f'example_log_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.txt'
    os.rename(old_log_path, new_log_path)
    os.remove(new_log_path)
    
def remove_file(symbol):
    # Check if the file exists before attempting to remove it
    filename = symbol+'TrailPrice.ini'
    if os.path.exists(filename):
        os.remove(filename)
  
def read_config(filename='GeminiConfig.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config


def isSymbolPresent(df, symbol):
    return symbol.lower() in df['symbol'].str.lower().values

def read_price_setup_from_csv(symbol):
    df = pd.read_csv('SymbolPriceLvlSetup.txt', dtype={'symbol':'string','side':'string','UpperRange':float,'LowerRange':float})
    price_ranges  = df[df['symbol'] == symbol] 
    return price_ranges
 
def GetMAVal(ConfigKey, MAPerid=100,period='5m',PriceBand=.0015):
    print(period)
    base_url = 'https://api.gemini.com/v2'
    response = requests.get(base_url + '/candles/'+ConfigKey+'/'+period)
    data = response.json()
    df=pd.DataFrame(data)
    # Create a dictionary to map numerical column names to labels
    column_mapping = {0: 'timestamp', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume'}
    # Rename columns using the mapping
    df = df.rename(columns=column_mapping)
    #df['timestamp'] = pd.to_datetime(df['timestamp'])
    # Convert the timestamp column to datetime
    df['timestamp_ts'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.sort_values(by='timestamp', ascending=True)  # Sort by timestamp in descending order
    df.set_index('timestamp', inplace=True)
    pd.set_option('display.float_format', '{:.2f}'.format)
    df_cleaned = df.dropna()
    
    dfNew=CandleLogic(df_cleaned)
    return dfNew
    


def calculate_ha_ohlc(data_frame):
    """Calculate Heikin-Ashi OHLC."""
    data_frame['HA_Open'] = (data_frame['Open'].shift(1) + data_frame['Close'].shift(1)) / 2
    data_frame['HA_Close'] = (data_frame['Open'] + data_frame['Low'] + data_frame['Close'] + data_frame['High']) / 4
    data_frame['HA_High'] = data_frame[['High', 'Open', 'Close']].max(axis=1)
    data_frame['HA_Low'] = data_frame[['Low', 'Open', 'Close']].min(axis=1)
    return data_frame

def calculate_mas(data_frame):
    """Calculate moving averages."""
    for i in range(1, 5):
        data_frame[f'FastMA{i}'] = data_frame['HA_Close'].rolling(window=i * 3 ).mean()
        
    for i in range(1, 5):
        data_frame[f'SlowMA{i}'] = data_frame['HA_Close'].rolling(window=i * 10+10).mean()
    data_frame['IsGreen'] = np.where(data_frame['HA_Open'] < data_frame['HA_Close'], 'G', 'R') 

    data_frame['MA'] = data_frame['HA_Close'].rolling(window=50).mean()    
    data_frame['SlowHigh'] = data_frame['HA_High'].rolling(window=200).mean()
    data_frame['SlowLow'] = data_frame['HA_Low'].rolling(window=200).mean()
    data_frame['HighLowDiff'] = data_frame['SlowHigh']-data_frame['SlowLow']
    data_frame['UpperMA'] = data_frame['MA']+data_frame['HighLowDiff']
    data_frame['LowerMA'] = data_frame['MA']-data_frame['HighLowDiff']
    
    return data_frame
def calculate_mametrics(data_frame):
    """Calculate moving average metrics."""
    fast_ma_cols = [f'FastMA{i}' for i in range(1, 5)]
    slow_ma_cols = [f'SlowMA{i}' for i in range(1, 5)]
    data_frame['MAMaxFast'] = data_frame[fast_ma_cols].max(axis=1)
    data_frame['MAMinFast'] = data_frame[fast_ma_cols].min(axis=1)
    data_frame['MAMaxSlow'] = data_frame[slow_ma_cols].max(axis=1)
    data_frame['MAMinSlow'] = data_frame[slow_ma_cols].min(axis=1)
    all_ma_cols = fast_ma_cols + slow_ma_cols
    data_frame['MAMax'] = data_frame[all_ma_cols].max(axis=1)
    data_frame['MAMin'] = data_frame[all_ma_cols].min(axis=1)
    data_frame['MARatio'] = data_frame['MAMax'] / data_frame['MAMin']
    return data_frame

def check_incremental_order(values):
    return all(values[i] < values[i+1] for i in range(len(values) - 1))

def check_decremental_order(values):
    return all(values[i] > values[i+1] for i in range(len(values) - 1))

def check_incremental_condition_last_row(last_row, fast_ma_cols, slow_ma_cols):
    fast_incremental = check_incremental_order(last_row[fast_ma_cols].values)
    slow_incremental = check_incremental_order(last_row[slow_ma_cols].values)
    return fast_incremental and slow_incremental

def check_decremental_condition_last_row(last_row, fast_ma_cols, slow_ma_cols):
    fast_decremental = check_decremental_order(last_row[fast_ma_cols].values)
    slow_decremental = check_decremental_order(last_row[slow_ma_cols].values)
    return fast_decremental and slow_decremental

def calculate_signals(data_frame):
    """Calculate trading signals."""
    calculate_ha_ohlc(data_frame)
    calculate_mas(data_frame)
    calculate_mametrics(data_frame)
    
    fast_ma_cols = [f'FastMA{i}' for i in range(1, 5)]
    slow_ma_cols = [f'SlowMA{i}' for i in range(1, 5)]
    
    multi_ma_condition = (
        (data_frame['HA_Low'] <= data_frame[fast_ma_cols].min(axis=1)) &
        (data_frame['HA_High'] >= data_frame[fast_ma_cols].max(axis=1)) &
        (data_frame['HA_Low'] <= data_frame[slow_ma_cols].min(axis=1)) &
        (data_frame['HA_High'] >= data_frame[slow_ma_cols].max(axis=1))
    )
    data_frame['IsInBtwHighLow'] = np.where(multi_ma_condition, 'Y', 'N')

    data_frame['IsLTHighLowRation'] = np.where(data_frame['MARatio'] < (data_frame['SlowHigh'] / data_frame['SlowLow']), 'Y', 'N')
    last_row=data_frame.iloc[-1]
    is_incremental = check_incremental_condition_last_row(last_row, fast_ma_cols, slow_ma_cols)
    is_decremental = check_decremental_condition_last_row(last_row, fast_ma_cols, slow_ma_cols)
    last_row=data_frame.iloc[-2]
    is_incremental_prev = check_incremental_condition_last_row(last_row, fast_ma_cols, slow_ma_cols)
    is_decremental_prev = check_decremental_condition_last_row(last_row, fast_ma_cols, slow_ma_cols)

    
     # Combine conditions to determine Buy, Sell, or blank
    data_frame['CheckTrendSignal'] = np.where((is_incremental and is_incremental_prev==False), 'Buy', '')
    data_frame['CheckTrendSignal'] = np.where((is_decremental and is_decremental_prev==False), 'Sell', data_frame['CheckTrendSignal'])


    return data_frame
def calculate_signals_1(df,current_price):
    last_row=df.iloc[-1]
    last_row1=df.iloc[-2]
    last_row2=df.iloc[-3]
    isBuyCondMatch =( last_row['IsGreen']=="G" 
                              and last_row1['IsGreen']=="R" 
                              and last_row2['IsGreen']=="R" 
                              and ((
                                  current_price<last_row["UpperMA"]*(1+.002)
                                  and
                                  current_price>last_row["LowerMA"]*(1-.002)
                                  )
                                  or
                                   (
                                    current_price>last_row["LowerMA"]*(1-.035)
                                  )
                                  )
                              and current_price>last_row["HA_Close"]
                              and current_price<last_row["HA_Close"]*(1+.002)
                     
                    )

    isSellCondMatch = (last_row['IsGreen']=="R" 
                          and last_row1['IsGreen']=="G" 
                          and last_row2['IsGreen']=="G" 
                          and (
                              (current_price<last_row["UpperMA"]*(1+.002)
                                  and current_price>last_row["LowerMA"]*(1-.002)
                               )
                               or
                                   (
                                    current_price>last_row["UpperMA"]*(1+.035)
                                  ))
                          and current_price<last_row["HA_Close"]
                          and current_price>last_row["HA_Close"]*(1-.002) 
                        )
   

    signal=0
    if isBuyCondMatch:
       signal=1
    if isSellCondMatch:
       signal=2
    return signal
    
def CandleLogic(df):
    df=calculate_ha_ohlc(df)
    df=calculate_mas(df)
    df=calculate_mametrics(df)
    df=calculate_signals(df)

    return df

    
    
def get_symbol_data(symbol='BTC-USD', interval='15m', period='3d',MAPeriod=20 ,shift=1):
    heikin_ashi_df= getYahooData(symbol,interval,period,MAPeriod,shift)
    last_row = heikin_ashi_df.iloc[-1]
    #plotChar(heikin_ashi_df)
    write_to_log(last_row)
    return last_row

    

# Function to plot moving averages and price data
def plot_and_send_to_telegram(df, chat_id):
    # Plotting
    plt.figure(figsize=(10, 6))
    
    # Plot price data
    plt.plot(df.index, df['HA_Close'], label='Price', color='black')
    
    # Plot moving averages
    for col in df.columns:
        if 'MA' in col:
            plt.plot(df.index, df[col], label=col)
    
    # Formatting
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.title('Moving Averages and Price')
    plt.legend()
    plt.grid(True)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gcf().autofmt_xdate()
    
    # Save plot to bytes buffer
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    
    # Send plot to Telegram
    bot.send_photo(chat_id, photo=buffer)
    buffer.close()



def getYahooData(symbol, interval, period, MAPeriod=50,shift=10,BuyRange=.002):
    # Download historical data from Yahoo Finance
    df = yf.download(symbol, interval=interval, period=period)
    dfHA=CandleLogic(df,MAPeriod)
    return dfHA


def FormatNumber(val):
    return "{:.8f}".format(val)


def ends_with(string,len,val):
    return string[-len:].lower() == val

def getMidPrice(symbol):
    current_price = getScriptPrice(symbol)
    midPrice=current_price.values[0][1]
    return float(midPrice)
    
def getScriptPrice(symbol):
    base_url = 'https://api.gemini.com/v1'
    response = requests.get(base_url + '/pubticker/'+symbol)
    data = response.json()
    df = pd.DataFrame(data)
    return df


def get_total_valuation(symbol):
    total_valuation=0
    url = f'https://api.gemini.com/v1/pubticker/{symbol}'
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()

        last_trade_price = float(data['last'])
        
        
        dict_items = list(data['volume'].items())
        
        # Access the value at the first index
        btc_volume = dict_items[0][1]
        
        print("BTC volume:", btc_volume)
        return total_valuation
    else:
        print("Failed to retrieve data from Gemini API.")
        return 0

def fetch_symbols():
    base_url = "https://api.gemini.com/v1"
    response = requests.get(base_url + "/symbols")
    symbols = response.json()
    return symbols

def send_notification(symbol, intrvl, close_price, chat_id, bot_token, msg):
    message_text = (
        f"*{symbol}*:  \n"
        f"_Interval_: {intrvl}"
        f"{msg}"
    )
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {'chat_id': chat_id, 'text': message_text, 'parse_mode': 'Markdown'}
    response = requests.post(url, json=params)
    if response.status_code != 200:
        print("Failed to send message. Status code:", response.status_code)

def process_symbol(symbol, intrvl, bot_token, chat_id):
    price=getMidPrice(symbol)
    df = GetMAVal(symbol, MAPerid=50, period=intrvl, PriceBand=.002)
    signal=calculate_signals_1(df,price)
    last_row = df.iloc[-1]
    msg=''        
    
    if last_row['IsLTHighLowRation'] =='Y':
        msg += '\n- All MA in Very Narrow Range'
    if last_row['IsInBtwHighLow'] =='Y':
        msg += '\n- All MA in High Low Range'
    if last_row['CheckTrendSignal'] !='':
        msg += '\n- Signal : '+last_row['CheckTrendSignal']+' '
    if signal==1:    
        msg += '\n- Signal Buy Trend'
    if signal==2:      
        msg += '\n- Signal Sell Trend'
        
    if msg!='' and signal>0:
       
        msg += '\n- Market Mid Price : '+str(price)
        ration=last_row['SlowHigh']  / last_row['SlowLow']
        
        StopLoss = 0
        if signal == 1:
            StopLoss = price * (2 - ration)
        elif signal == 2:
            StopLoss = price * ration

        msg += f'\n- StopLoss: {StopLoss}'
            
        send_notification(
            symbol,
            intrvl,
            last_row["HA_Close"],
            chat_id,
            bot_token,
            msg
        )

     
def job(interval, period,symbols, bot_token, chat_id):
    while True:
        current_minute = datetime.datetime.now().minute
        print(current_minute)
        if current_minute % interval == 0:
            for symbol in symbols:
                process_symbol(symbol, period, bot_token, chat_id)
        time.sleep(60)  # Sleep for 60 seconds before running again

# Define your symbols and candle intervals
symbols = ['btcusd', 'ethusd', 'solusd', 'maticusd', 'pepeusd','ltcusd','linkusd','injusd','galausd','xrpusd','dogeusd','sushiusd','uniusd','renusd','umausd','aaveusd','avaxusd']
candle_intervals = {'15m': 15, '30m': 30, '1hr': 60, '6hr': 360}
#'1m': 1,'5m': 5,
# Read config
config = read_config('C:\\Users\\jaina\\Gaurav\\Gemini\\Config\\APIKey.ini')
bot_token = config.get('TGSIGNALBOT', 'TOKEN')
chat_id = config.get('TGSIGNALBOT', 'CHAT_ID')

# Start the job in separate threads for each candle interval
threads = []
for intrvl, interval_minutes in candle_intervals.items():
    thread = threading.Thread(target=job, args=(interval_minutes, intrvl, symbols, bot_token, chat_id))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Main thread can continue executing other tasks if needed
# Keep the main thread alive so that the job thread can continue running
while True:
    continue

