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
#%matplotlib qt
import requests
from bs4 import BeautifulSoup
import json
import os
from pathlib import Path
import logging
from loguru import logger

   
def GetGitHubPriceSetupFile():
    file_content=""
    url = "https://raw.githubusercontent.com/jainkgaurav/MyRepo/main/PriceSetup.ini"
    response = requests.get(url)
    if response.status_code == 200:
        # Content of the file
        file_content = response.text
    return file_content

def ProcessPriceSetupFileToLocal():
    filename = "Config/SymbolSetup.ini"
    try:
        if(can_rewrite(filename)==1):
            content = GetGitHubPriceSetupFile()
            if len(content)>0:
                with open(filename, "w") as file:
                     file.write(content)
                     write_to_log(content)
    except Exception as e:
        write_to_log(f"Error: {e}")
        
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
        if time_difference > 180:
           AllowUpdate=1
    except Exception as e:
        write_to_log(f"Error: {e}")
    return AllowUpdate



def data_to_write(content,symbol):
    filename = "Config/"+symbol+"TrailPrice.ini"
    with open(filename, "w") as file:
        file.write("["+symbol+"]\n")
        file.write(symbol+"="+str(content))

def write_to_log(*args):
    log_file_path = "log/gemini_log.txt"
    max_file_size_kb = 1000

    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{current_time} - {' '.join(map(str, args))}\n"

    # Check file size
    if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > max_file_size_kb * 1024:
        # If file size exceeds the threshold, create a new log file and delete the old one
        create_new_log(log_file_path)
    
    # Open the log file in append mode
    with open(log_file_path, "a") as log_file:
        log_file.write(log_entry)

def create_new_log(old_log_path):
    # Create a new log file with a timestamp in the filename
    new_log_path = f"example_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
    os.rename(old_log_path, new_log_path)
    os.remove(new_log_path)
  
def remove_file(symbol):
    # Check if the file exists before attempting to remove it
    filename = "Config/"+symbol+"TrailPrice.ini"
    if os.path.exists(filename):
        os.remove(filename)

def get_input_element_value(url, class_name, input_type):
    try:
        # Fetch HTML content from the specified URL
        response = requests.get(url)
        response.raise_for_status()

        # Parse HTML content with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the <input> element with the specified class and type
        input_element = soup.find('input', {'class': class_name, 'type': input_type})

        # Extract the value attribute from the <input> element
        if input_element:
            value_attribute = input_element.get('value')
            return value_attribute
        else:
            write_to_log(f"No <input> element with class '{class_name}' and type '{input_type}' found.")
            return None

    except requests.exceptions.RequestException as e:
        write_to_log(f"Error: {e}")
        return None

# Function to generate a new nonce
def generate_nonce():
    #nonce = f"{int(time.time()) * 1000001 + int(time.perf_counter() * 1000001)}"
    t = datetime.datetime.now()
    nonce = time.time()
    #write_to_log(nonce)
    return nonce  # Convert current time to milliseconds

def read_config(filename='Config/GeminiConfig.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config

# Read configuration file
config = read_config()

def MyPositions():
    payload_nounce = generate_nonce()
    payload = {"request": "/v1/balances", "nonce": payload_nounce}
    return payload


def MyTrades():
    payload_nounce = generate_nonce()
    payload = {"request": "/v1/mytrades", "nonce": payload_nounce}
    return payload

def NewOrder(SymbolScript="",Qty=0.00,ClientOrderID="", orderPrice=0, OrderType="LMT", BuyOrSell=""):
    payload_nounce = generate_nonce()
    #"client_order_id":ClientOrderID,
    match OrderType:
        case "MC":
                OptionType="maker-or-cancel"
        case "IC":
                OptionType="immediate-or-cancel"
        case "FC":    
                OptionType="fill-or-kill"
        
    if OrderType  == "LMT":
        payload = {
                "request": "/v1/order/new",
                "nonce": payload_nounce,
                "symbol": SymbolScript,
                "amount": str(Qty),
                "price": str(orderPrice),
                "side": BuyOrSell,
                "type": "exchange limit"
                  }
    elif OrderType  == "SL":
        payload = {
                "request": "/v1/order/new",
                "nonce": payload_nounce,
                "symbol": SymbolScript,
                "amount": str(Qty),
                "price": str(orderPrice),
                "side": BuyOrSell,
                "type": "exchange stop limit"
                  }   
    else:
        payload = {
                "request": "/v1/order/new",
                "client_order_id":ClientOrderID,
                "nonce": payload_nounce,
                "symbol": SymbolScript,
                "amount": str(Qty),
                "price": str(orderPrice),
                "side": BuyOrSell,
                "type": "exchange limit",
                "options" : [OptionType]
                  }
        
    return payload


def Auth(payload,isPrimary="N"):
    if isPrimary=="Y":
        gemini_api_key = config.get('GeminiAPI', 'PA_gemini_api_key')
        gemini_api_secret = config.get('GeminiAPI', 'PA_gemini_api_secret').encode()
    else:
        gemini_api_key = config.get('GeminiAPI', 'gemini_api_key')
        gemini_api_secret = config.get('GeminiAPI', 'gemini_api_secret').encode()
 
    payload["nonce"] = generate_nonce()
   
    encoded_payload = json.dumps(payload).encode()
    b64 = base64.b64encode(encoded_payload)
    # Ensure gemini_api_secret is represented as bytes
    signature = hmac.new(gemini_api_secret, b64, hashlib.sha384).hexdigest()
    request_headers = {
        'Content-Type': "text/plain",
        'Content-Length': "0",
        'X-GEMINI-APIKEY': gemini_api_key,
        'X-GEMINI-PAYLOAD': b64.decode(),
        'X-GEMINI-SIGNATURE': signature,
        'Cache-Control': "no-cache"
    }
    return request_headers

def getScriptPrice(symbol):
    base_url = "https://api.gemini.com/v1"
    response = requests.get(base_url + "/pubticker/"+symbol)
    data = response.json()
    df = pd.DataFrame(data)
    return df

def getMidPrice(symbol):
    current_price = getScriptPrice(symbol)
    midPrice=current_price.values[0][1]
    return float(midPrice)
    
def GetMarkPriceOfETH():
    #mark_price
    response = RequestType("OP")
    data= response.json()
    df = pd.DataFrame(data)
    return df


def getOpenOrders():
    payload_nounce = generate_nonce()
    payload = { "nonce": payload_nounce,"request": "/v1/orders"}
    return payload

def OpenPositions():
    payload_nounce = generate_nonce()
    payload = {  "request": "/v1/positions", "nonce": payload_nounce,      }
    return payload

def CancelAllOrder():
    payload_nounce = generate_nonce()
    payload = {  "request": "/v1/order/cancel/all", "nonce": payload_nounce    }
    return payload



def isSymbolPresent(df, symbol):
    return symbol.lower() in df['symbol'].str.lower().values
    
def hasOpenOrders(symbol):
    df = RequestType("OO")  # "OO" stands for Open Orders
    return int('symbol' in df.columns and df[df['symbol'] == symbol].shape[0] > 0)

def hasOpenPosition(symbol):
    df = RequestType("OP")  # "OP" stands for Open Positions
    return int('symbol' in df.columns and df[df['symbol'] == symbol].shape[0] > 0)
   

def CloseAllOrders():
    open_orders = hasOpenOrders()

    if open_orders:
        for order in open_orders:
            order_id = order.get("order_id", None)
            if order_id:
                # Cancel the open order
                cancel_response = RequestType("CO", order_id)
                write_to_log(f"Canceled order {order_id}: {cancel_response.json()}")
    else:
        write_to_log("No open orders to close.")




# Add other functions as needed
def RequestType(strType, Symbol="",Qty=0.00,ClientOrderID="", orderPrice=0, OpType="", BuyOrSell=""):
    match strType:
        case "Bal":
            url = "https://api.gemini.com/v1/balances"
            request_headers=Auth(MyPositions())
            
        case "MT":
            url = "https://api.gemini.com/v1/mytrades"
            request_headers=Auth(MyTrades())
            
        case "NO":
            url = "https://api.gemini.com/v1/order/new"
            request_headers=Auth(NewOrder(Symbol,Qty,ClientOrderID, orderPrice, OpType, BuyOrSell))    

        case "OO":
            write_to_log("Checking Open Orders...")
            url = "https://api.gemini.com/v1/orders"
            request_headers=Auth(getOpenOrders())               
            
        
        case "OP":
            url = "https://api.gemini.com/v1/positions"
            request_headers=Auth(OpenPositions())        
            
        case "CO":
            url = "https://api.gemini.com/v1/order/cancel/all"
            request_headers=Auth(CancelAllOrder())        
    
        case _:  write_to_log("Please provide correct input fuction")

    response = requests.post(url, headers=request_headers)
    data = response.json()
    write_to_log("data : ",data)
    data
   
    if isinstance(data, (list, dict)):
        # If the data is a list or dictionary, use DataFrame directly
        df = pd.DataFrame([data]) if isinstance(data, dict) else pd.DataFrame(data)
    else:
        # If the data is neither a list nor a dictionary, create a DataFrame with a single column
        df = pd.DataFrame({'data_column': [data]})

    write_to_log("Successfully loaded into a DataFrame:")
    write_to_log(df)
    return df
	

def read_price_setup_from_csv(symbol):
    df = pd.read_csv('config/SymbolPriceLvlSetup.txt', dtype={'symbol':'string','side':'string','UpperRange':float,'LowerRange':float})
    price_ranges  = df[df['symbol'] == symbol] 
    return price_ranges
 

def GetMAVal(ConfigKey, MAPerid=100,period='5m',PriceBand=.006):
    base_url = "https://api.gemini.com/v2"
    response = requests.get(base_url + "/candles/"+ConfigKey+"/"+period)
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
    dfHA=CandleLogic(df,MAPerid,PriceBand)
    last_row = df.iloc[-1]
    #write_to_log(last_row)
    #return last_row
    
    return dfHA

    
def CandleLogic(df,MAPeriod,PriceBand):
    shift=20
    # Download historical data from Yahoo Finance
    df['HA_Open']  = (df['Open'].shift(1) + df['Close'].shift(1)) / 2
    df['HA_Close']  = (df['Open'] + df['Low'] + df['Close'] + df['High']) / 4
    df['HA_High']  = df[['High', 'Open', 'Close']].max(axis=1)
    df['HA_Low']  = df[['Low', 'Open', 'Close']].min(axis=1)
 
    df['MA20'] = df["HA_Close"].rolling(window=20).mean()
    df['MA'] = df["HA_Close"].rolling(window=MAPeriod).mean()
    df['UpperMA'] = df['MA']*(1+PriceBand)
    df['LowerMA'] = df['MA']*(1-PriceBand)
    
    df['IsGreen'] = np.where(df['HA_Open'] < df['HA_Close'], 'G', 'R') 
    
    # Calculate lowest low for each rolling window
    df['LongEntry'] = df['HA_High'].shift(shift).rolling(window=MAPeriod).min()
    #df['LowerRange'] = df['MinHigh'].rolling(window=MAPeriod).max()
    #df['UpperRange'] =df['LowerRange'] * (1+BuyRange)
    df_cleaned = df.dropna()
    return df_cleaned
 
    
def get_symbol_data(symbol='BTC-USD', interval='15m', period='3d',MAPeriod=20 ,shift=1):
    heikin_ashi_df= getYahooData(symbol,interval,period,MAPeriod,shift)
    last_row = heikin_ashi_df.iloc[-1]
    #plotChar(heikin_ashi_df)
    write_to_log(last_row)
    return last_row

def plot(df):
    #df=getYahooData('ETH-USD', '5m', '3d',MAPeriod=50,shift=10)
    
    # Plotting OHLC data
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.plot(df['HA_Close'], label='HA_Close', marker='',linewidth=1)
    ax.plot(df['Close'], label='Close', marker='',linewidth=1)
    ax.plot(df['MA'], label='MA', marker='',linewidth=1)
    
    ax.set_title('OHLC Chart')
    ax.set_xlabel('Index')
    ax.set_ylabel('Price')
    ax.legend()
    
    plt.show()
    
def calculate_hma(data, period):
    
    half_period = int(period / 2)

    wma_half = data.rolling(window=half_period).mean()
    wma_full = data.rolling(window=period).mean()

    weighted_moving_average = (2 * wma_half - wma_full).rolling(window=int(np.sqrt(period))).mean()
    
    return weighted_moving_average

def getYahooData(symbol, interval, period, MAPeriod=50,shift=10,BuyRange=.002):
    # Download historical data from Yahoo Finance
    df = yf.download(symbol, interval=interval, period=period)
    dfHA=CandleLogic(df,MAPeriod)
    return dfHA


#dfYoo=getYahooData(YahooCode, '5m', '2d', MAPeriod=MAPeriod,shift=shift,BuyRange=BuyRange)

#dfPriceRange = read_price_setup_from_csv(symbol)
#df=getYahooData(YahooCode, '5m', '4d', MAPeriod=10)
#write_to_log(last_row)
#CandleColor=last_row['IsGreen']
 


def StartTrading(symbol):

    
    try:
         
        write_to_log("\n\n************ StartTrading *******************")
        #dfOO = RequestType("OO") 
        ProcessPriceSetupFileToLocal()
        #time.sleep(50)
        ConfigKey=symbol
        #config = read_config()
        config = read_config("Config/SymbolSetup.ini")
        correcion = float(config.get('InitParam', 'Correction'))
        
        MAPeriod = int(config.get('InitParam', 'MAPeriod'))
        shift = int(config.get('InitParam', 'shift'))
        BuyRange = float(config.get(ConfigKey, 'BuyRange'))
        
        StopLossPerc = float(config.get(ConfigKey, 'StopLossPerc'))
        TargetProftPerc = float(config.get(ConfigKey, 'TargetProftPerc'))
           
        Qty = float(config.get(ConfigKey, 'Qty'))
        InvestAmt = float(config.get(ConfigKey, 'InvestAmt'))
      
        ClientOrderID = config.get(ConfigKey, 'ClientOrderID')
        MaxQty =   float(config.get(ConfigKey, 'MaxQty'))
        YahooCode =   config.get(ConfigKey, 'YahooCode')
        Pair =   config.get(ConfigKey, 'Pair')
        AllowTrading =   config.get(ConfigKey, 'AllowTrading')
        AllowTrailing =   config.get(ConfigKey, 'AllowTrailing')
        TrailStartAfterPerc =   float(config.get(ConfigKey, 'TrailStartAfterPerc'))
        PriceFactor =   float(config.get(ConfigKey, 'PriceFactor'))
        
        
        LongEntryLB = float(config.get(ConfigKey, 'LongEntry'))
        LongEntryUB = LongEntryLB*(1+BuyRange)
        LongExit = float(config.get(ConfigKey, 'LongExit'))
        
        ShortEntryUB = float(config.get(ConfigKey, 'ShortEntry'))
        ShortEntryLB = ShortEntryUB*(1-BuyRange)
        ShortExit = float(config.get(ConfigKey, 'ShortExit'))
        
        write_to_log("LongEntryLB,LongEntryUB,ShortEntryUB,ShortEntryLB")
        write_to_log(LongEntryLB,LongEntryUB,ShortEntryUB,ShortEntryLB)
        
        Updown=correcion
        
        if 1==1:
            TrailPrice=0.0
            IsCloseCondMatch=False
            unrealised_pnl = 0 
            average_cost=0
            OpenTradeQuantity=0
            CloseSide=""
            CanClosePosition=False
            TrailPriceStopLoss=0
            markPrice=0
            current_price = getMidPrice(Pair)  
            #df=GetMAVal(Pair, MAPerid=90,period='5m',PriceBand=BuyRange)
            #last_row = df.iloc[-1]
            write_to_log("round(current_price,0)%PriceFactor : ",current_price,round(current_price,0)%PriceFactor)
            if round(current_price*1.001,0)%PriceFactor<PriceFactor*.1 :
                write_to_log("current_price,PriceFactor : = ",current_price,PriceFactor)
                LongEntryLB=current_price
            
            LongEntryUB = LongEntryLB*(1+BuyRange)
            
            isBuyCondMatch =  current_price>=LongEntryLB and current_price<LongEntryUB  #and last_row['IsGreen']=='G' 
            isSellCondMatch = current_price>ShortEntryLB and current_price<ShortEntryUB #and last_row['IsGreen']=='R' 
            write_to_log("Qty : = ",round(InvestAmt/current_price,3))
        
            write_to_log("\n\n current_price,isBuyCondMatch,isSellCondMatch",current_price,isBuyCondMatch,isSellCondMatch)
              
            dfOP = RequestType("OP")    
            if not dfOP.empty: 
                 
               filtered_dfOP = dfOP[dfOP['symbol'] == symbol]
               write_to_log("symbol : ",symbol)
               if len(filtered_dfOP)>0: #Close Existing Order
                    write_to_log("filtered_dfOP : /n",filtered_dfOP)
                    average_cost = round(float(filtered_dfOP["average_cost"].values[0]), 2)
                    OpenTradeQuantity = float(filtered_dfOP["quantity"].values[0])
                    unrealised_pnl=round(float(filtered_dfOP["unrealised_pnl"].values[0]),0)
                    current_price=round(float(filtered_dfOP["mark_price"].values[0]),0)
                     
                    write_to_log("\n\naverage_cost, OpenTradeQuantity",   average_cost ,OpenTradeQuantity)
               
                    try:
                        file_path = symbol+'TrailPrice.ini'
                        file_path_obj = Path(file_path)
                        if file_path_obj.exists():
                            GetTrailConfig=read_config(filename=file_path)  
                            TrailPrice= float(GetTrailConfig.get(symbol, symbol))
                    except (ValueError, KeyError, IndexError) as e:
                        write_to_log(f"Error in Getting Trail price: {e}")
                        TrailPrice=0
                       
                    if TrailPrice==0 or AllowTrailing=="N":
                        TrailPrice=average_cost
                    
                    if OpenTradeQuantity>0:# For Closing Buy Position
                       CloseSide="sell" 
                       Updown=correcion
                       if AllowTrailing=="Y" and current_price/average_cost>(1+TrailStartAfterPerc):
                           TrailPrice= max(TrailPrice,current_price,average_cost)
                           
                           
                       TrailPriceStopLoss=TrailPrice*(1-StopLossPerc)    
                       if LongExit>0 and AllowTrailing!="Y" :# and LongExit>average_cost  : 
                          TrailPriceStopLoss= LongExit 
                          TrailPrice=average_cost
                           
                       CanClosePosition=  ( TrailPriceStopLoss > current_price or current_price/average_cost>TargetProftPerc)
                        
                    elif OpenTradeQuantity<0: #For Closing Sell Position
                       CloseSide="buy"
                       Updown=-correcion
                       if AllowTrailing=="Y"  and average_cost/current_price>(1+TrailStartAfterPerc):
                           TrailPrice= min(TrailPrice,current_price,average_cost)
                       
                       TrailPriceStopLoss=TrailPrice*(1+StopLossPerc)    
                       if ShortExit>0 and AllowTrailing!="Y"  : #and ShortExit<average_cost :
                          TrailPriceStopLoss= ShortExit 
                          TrailPrice=average_cost
                           
                       CanClosePosition= (TrailPriceStopLoss < current_price or average_cost/current_price>TargetProftPerc )
          
            write_to_log("\n\n\n ,CanClosePosition,average_cost  TrailPriceStopLoss: ",CanClosePosition,average_cost, TrailPriceStopLoss)
            #Write Back TrailPrice
            write_to_log("========================data_to_write==========================================")
            if OpenTradeQuantity==0:
                TrailPrice=current_price

            data_to_write(TrailPrice,symbol)     
            
            try: 
                
                if  (CanClosePosition) :
                    write_to_log("========================Close Position==========================================")
                      
                    
                        
                    write_to_log("\nClose Order Processed\n")
                    mark_price = round(getMidPrice(Pair), 0)
                    
                    data = RequestType("NO",Symbol=symbol,Qty=abs(OpenTradeQuantity),ClientOrderID=ClientOrderID, 
                                        orderPrice=round(mark_price*(1-Updown),0) ,
                                        OpType="FC",BuyOrSell=CloseSide)
                    
                    write_to_log("\nClose Position Step2:", data)
                    
                
                if  (isBuyCondMatch or  isSellCondMatch ) and OpenTradeQuantity==0 and AllowTrading=="Y"  :
                    write_to_log("========================Open New Position==========================================")
                    current_price = getMidPrice(Pair) 
                    correcion_factor = 1 + correcion
                    buysellind = "buy"
                    Qty=round(InvestAmt/current_price,3)
                    if isSellCondMatch: 
                        buysellind ="sell"
                        correcion_factor = 1 - correcion
                    
                    df = RequestType("NO",Symbol=symbol,Qty= Qty,ClientOrderID=ClientOrderID,
                                        orderPrice=round(current_price * correcion_factor, 0),
                                         OpType="FC",BuyOrSell=buysellind)
                   
                    write_to_log("\nNew {buysellind} Order Response:", df)  
                    
            except (ValueError, KeyError, IndexError) as e:
                # Handle exceptions related to value conversion, key access, or index out of range
                write_to_log(f"Error in opening positon: {e}")
                # You can log the error or take appropriate action based on your requirements    
            
    except (ValueError, KeyError, IndexError) as e:
            # Handle exceptions related to value conversion, key access, or index out of range
            write_to_log(f"Error Start Trading: {e}")
            # You can log the error or take appropriate action based on your requirements        
    
 
    
def trading_thread():
    write_to_log("************ Thread Started *******************")
    ProcessPriceSetupFileToLocal()
    while True:
        try:
            current_time = datetime.datetime.now()
            time.sleep(5)
            write_to_log("Current Time:", current_time) 
            if current_time.second % 20 == 0:
                dfOP = RequestType("OP")
            write_to_log("************ ETHGUSDPERP *******************")
            StartTrading("ethgusdperp")
            time.sleep(5)
            write_to_log("************ BTCGUSDPERP *******************")
            StartTrading("btcgusdperp")
            #if current_time.minute % 15 == 0:
            #    clear_output(wait=True)
        except Exception as e:
            write_to_log(f"An exception occurred: {e}")
        



if __name__ == "__main__":

 
    # sleep for 30 Seconds
   
    write_to_log("Initiated....")
   
    #df=GetMAVal('ethusd', MAPerid=180,period='1m')
    #last_row = df.iloc[-1]
    #write_to_log(df)
    #write_to_log(last_row)        
    #write_to_log(read_price_setup_from_csv("ethgusdperp"))
    #write_to_log("************ BTCGUSDPERP *******************")
    #StartTrading("ethgusdperp")
    #StartTrading("btcgusdperp")
    #plot(df)
    print("testing")
    #write_to_log(GetMAVal("ethgusdperp", MAPerid=10))
    dfOP = RequestType("OP")
    write_to_log(dfOP)
    trading_thread()