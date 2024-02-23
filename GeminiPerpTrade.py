import requests
import json
import base64
import hmac
import hashlib
import datetime, time
import pandas as pd
import random
import configparser
from IPython.display import clear_output
import threading
import numpy as np
import matplotlib.pyplot as plt
import requests
import json
import os
from pathlib import Path

#%matplotlib qt
    
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
        if time_difference > 300:
           AllowUpdate=1
    except Exception as e:
        write_to_log(f'Error: {e}')
    return AllowUpdate

def read_write_data(content,symbol,filename,ReadOrWrite):
    retVal=content
    file_path = filename+symbol+'.ini'
    if(ReadOrWrite=="W"):
        with open(file_path, 'w') as file:
            file.write('['+symbol+']\n')
            file.write(symbol+'='+str(content))
    else:
        file_path_obj = Path(file_path)
        if file_path_obj.exists():
            GetTrailConfig=read_config(filename=file_path)  
            retVal= GetTrailConfig.get(symbol, symbol)
    return  retVal       

def data_to_write(content,symbol):
    filename = symbol+'TrailPrice.ini'
    with open(filename, 'w') as file:
        file.write('['+symbol+']\n')
        file.write(symbol+'='+str(content))


def write_to_log(*args):
    log_file_path = 'LogReport.log'
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
  

# Function to generate a new nonce
def generate_nonce():
    #nonce = f'{int(time.time()) * 1000001 + int(time.perf_counter() * 1000001)}'
    t = datetime.datetime.now()
    nonce = time.time()
    #write_to_log(nonce)
    return nonce  # Convert current time to milliseconds

def read_config(filename='GeminiConfig.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config

# Read configuration file
config = read_config()

def MyPositions():
    payload_nounce = generate_nonce()
    payload = {'request': '/v1/balances', 'nonce': payload_nounce}
    return payload


def MyTrades():
    payload_nounce = generate_nonce()
    payload = {'request': '/v1/mytrades', 'nonce': payload_nounce}
    return payload

def NewOrder(SymbolScript='',Qty=0.00,ClientOrderID='', orderPrice=0, OrderType='LMT', BuyOrSell=''):
    payload_nounce = generate_nonce()
    #'client_order_id':ClientOrderID,
    match OrderType:
        case 'MC':
                OptionType='maker-or-cancel'
        case 'IC':
                OptionType='immediate-or-cancel'
        case 'FC':    
                OptionType='fill-or-kill'
        
    if OrderType  == 'LMT':
        payload = {
                'request': '/v1/order/new',
                'nonce': payload_nounce,
                'symbol': SymbolScript,
                'amount': str(Qty),
                'price': str(orderPrice),
                'side': BuyOrSell,
                'type': 'exchange limit'
                  }
    elif OrderType  == 'SL':
        payload = {
                'request': '/v1/order/new',
                'nonce': payload_nounce,
                'symbol': SymbolScript,
                'amount': str(Qty),
                'price': str(orderPrice),
                'side': BuyOrSell,
                'type': 'exchange stop limit'
                  }   
    elif OrderType  == 'IC':
        payload = {
                'request': '/v1/order/new',
                'client_order_id':ClientOrderID,
                'nonce': payload_nounce,
                'symbol': SymbolScript,
                'amount': str(Qty),
                'price': str(orderPrice),
                'side': BuyOrSell,
                'type': 'exchange limit',
                'options' : ['immediate-or-cancel']
                  }
    else:
        payload = {
                'request': '/v1/order/new',
                'client_order_id':ClientOrderID,
                'nonce': payload_nounce,
                'symbol': SymbolScript,
                'amount': str(Qty),
                'price': str(orderPrice),
                'side': BuyOrSell,
                'type': 'exchange limit',
                'options' : [OptionType]
                  }
        
    return payload


def Auth(payload,isPrimary='N'):
    if isPrimary=='Y':
        gemini_api_key = config.get('GeminiAPI', 'PA_gemini_api_key')
        gemini_api_secret = config.get('GeminiAPI', 'PA_gemini_api_secret').encode()
    else:
        gemini_api_key = config.get('GeminiAPI', 'gemini_api_key')
        gemini_api_secret = config.get('GeminiAPI', 'gemini_api_secret').encode()
 
    payload['nonce'] = generate_nonce()
   
    encoded_payload = json.dumps(payload).encode()
    b64 = base64.b64encode(encoded_payload)
    # Ensure gemini_api_secret is represented as bytes
    signature = hmac.new(gemini_api_secret, b64, hashlib.sha384).hexdigest()
    request_headers = {
        'Content-Type': 'text/plain',
        'Content-Length': '0',
        'X-GEMINI-APIKEY': gemini_api_key,
        'X-GEMINI-PAYLOAD': b64.decode(),
        'X-GEMINI-SIGNATURE': signature,
        'Cache-Control': 'no-cache'
    }
    return request_headers

def getScriptPrice(symbol):
    base_url = 'https://api.gemini.com/v1'
    response = requests.get(base_url + '/pubticker/'+symbol)
    data = response.json()
    df = pd.DataFrame(data)
    return df

def getMidPrice(symbol,AskBid=''):

    df = getScriptPrice(symbol)
    row=df.iloc[-1]
    write_to_log(row,AskBid)
    midPrice=(float(row['last'])+float(row['ask']))/2

    if AskBid=="Bid":
        midPrice=row['bid']
    if AskBid=="Ask":    
        midPrice=row['ask']

    return float(midPrice)
    
def GetMarkPriceOfETH():
    #mark_price
    response = RequestType('OP')
    data= response.json()
    df = pd.DataFrame(data)
    return df


def getOpenOrders():
    payload_nounce = generate_nonce()
    payload = { 'nonce': payload_nounce,'request': '/v1/orders'}
    return payload

def OpenPositions():
    payload_nounce = generate_nonce()
    payload = {  'request': '/v1/positions', 'nonce': payload_nounce,      }
    return payload

def CancelAllOrder():
    payload_nounce = generate_nonce()
    payload = {  'request': '/v1/order/cancel/all', 'nonce': payload_nounce    }
    return payload

def CancelOrder(order_id):
    payload_nounce = generate_nonce()
    payload = {
    "nonce": "payload_nounce",
    "order_id": order_id,
    "request": "/v1/order/cancel"
    }
    return payload



def isSymbolPresent(df, symbol):
    return symbol.lower() in df['symbol'].str.lower().values
    
def hasOpenOrders(symbol):
    df = RequestType('OO')  # 'OO' stands for Open Orders
    return int('symbol' in df.columns and df[df['symbol'] == symbol].shape[0] > 0)

def hasOpenPosition(symbol):
    df = RequestType('OP')  # 'OP' stands for Open Positions
    return int('symbol' in df.columns and df[df['symbol'] == symbol].shape[0] > 0)
   

def CloseAllOrders():
    open_orders = hasOpenOrders()

    if open_orders:
        for order in open_orders:
            order_id = order.get('order_id', None)
            if order_id:
                # Cancel the open order
                cancel_response = RequestType('CO', order_id)
                write_to_log(f'Canceled order {order_id}: {cancel_response.json()}')
    else:
        write_to_log('No open orders to close.')




# Add other functions as needed
def RequestType(strType, Symbol='',Qty=0.00,ClientOrderID='', orderPrice=0, OpType='', BuyOrSell='',OrderID=0):
    match strType:
        case 'Bal':
            url = 'https://api.gemini.com/v1/balances'
            request_headers=Auth(MyPositions())
            
        case 'MT':
            url = 'https://api.gemini.com/v1/mytrades'
            request_headers=Auth(MyTrades())
            
        case 'NO':
            url = 'https://api.gemini.com/v1/order/new'
            request_headers=Auth(NewOrder(Symbol,Qty,ClientOrderID, orderPrice, OpType, BuyOrSell))    

        case 'OO':
            write_to_log('Checking Open Orders...')
            url = 'https://api.gemini.com/v1/orders'
            request_headers=Auth(getOpenOrders())               
            
        
        case 'OP':
            url = 'https://api.gemini.com/v1/positions'
            request_headers=Auth(OpenPositions())        
            
        case 'CO':
            url = 'https://api.gemini.com/v1/order/cancel/all'
            request_headers=Auth(CancelAllOrder())   

        case 'CNCL':
            url = 'https://api.gemini.com/v1/order/cancel'
            request_headers=Auth(CancelOrder(OrderID))           
    
        case _:  write_to_log('Please provide correct input fuction')

    response = requests.post(url, headers=request_headers)
    data = response.json()
    #write_to_log('data : ',data)
    #data
   
    if isinstance(data, (list, dict)):
        # If the data is a list or dictionary, use DataFrame directly
        df = pd.DataFrame([data]) if isinstance(data, dict) else pd.DataFrame(data)
    else:
        # If the data is neither a list nor a dictionary, create a DataFrame with a single column
        df = pd.DataFrame({'data_column': [data]})

    
    #write_to_log(df)
    return df
	

def read_price_setup_from_csv(symbol):
    df = pd.read_csv('SymbolPriceLvlSetup.txt', dtype={'symbol':'string','side':'string','UpperRange':float,'LowerRange':float})
    price_ranges  = df[df['symbol'] == symbol] 
    return price_ranges
 

def GetMAVal(ConfigKey, MAPerid=100,period='5m',PriceBand=.0015):
    base_url = 'https://api.gemini.com/v2'
    response = requests.get(base_url + '/candles/'+ConfigKey+'/'+period)
    data = response.json()
    df=pd.DataFrame(data)
    column_mapping = {0: 'timestamp', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume'}
    df = df.rename(columns=column_mapping)
    df['timestamp_ts'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.sort_values(by='timestamp', ascending=True)  # Sort by timestamp in descending order
    df.set_index('timestamp', inplace=True)
    pd.set_option('display.float_format', '{:.2f}'.format)
    dfHA=CandleLogic(df,MAPerid,PriceBand)
    
    last_row = df.iloc[-1]
    
    return dfHA


def plot(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    #ax.plot(df['LowerMA'], label='LowerMA', marker='',linewidth=1)
    #ax.plot(df['UpperMA'], label='UpperMA', marker='',linewidth=1)
    ax.plot(df['MA'], label='MA', marker='',linewidth=1)
    ax.plot(df['Close'], label='Close', marker='',linewidth=1)
    ax.set_title('OHLC Chart')
    ax.set_xlabel('Index')
    ax.set_ylabel('Price')
    ax.legend()
    plt.show()


def FormatNumber(val):
    return "{:.9f}".format(val)


def CandleLogic(df,MAPeriod,PriceBand):
    shift=20
    MAFastPeriod=MAPeriod
    write_to_log("MAFastPeriod,",MAFastPeriod)
    # Download historical data from Yahoo Finance
    df['HA_Open']  = (df['Open'].shift(1) + df['Close'].shift(1)) / 2
    df['HA_Close']  = (df['Open'] + df['Low'] + df['Close'] + df['High']) / 4
    df['HA_High']  = df[['High', 'Open', 'Close']].max(axis=1)
    df['HA_Low']  = df[['Low', 'Open', 'Close']].min(axis=1)
    df['IsGreen'] = np.where(df['HA_Open'] < df['HA_Close'], 'G', 'R') 
    df['MAOrg'] = df['HA_Close'].rolling(window=MAPeriod).mean()
    df['MADbl'] = df['MAOrg'].rolling(window=MAPeriod).mean()
    
    df['HighHigh'] = df['HA_High'].rolling(window=MAPeriod).max()
    df['LowLow'] = df['HA_Low'].rolling(window=MAPeriod).min()
    
    
    df['HighRange'] = df['HA_High'].rolling(window=300).mean()
    df['LowRange'] = df['HA_Low'].rolling(window=300).mean()
    df["MAHLRange"]=(df['HighRange']-df['LowRange'])
    df["MASLRatio"]=(df['HighRange']-df['LowRange'])/df['LowRange']
    df['FastMA'] = df['HA_Close'].rolling(window=MAFastPeriod).mean() 
    df['MA'] = df['HA_Close'].rolling(window=MAPeriod).mean() 
    df['UpperMA'] = df['MA']+df['MAHLRange']
    df['LowerMA'] = df['MA']-df['MAHLRange']
    df['FastUpperMA'] = df['FastMA']+df['MAHLRange']
    df['FastLowerMA'] = df['FastMA']-df['MAHLRange']

    #Very fast 
    df['MA10'] = df['HA_Close'].rolling(window=5).mean()
    df['MA20'] = df['HA_Close'].rolling(window=8).mean()
    df['MA30'] = df['HA_Close'].rolling(window=13).mean()
    
    df['MA5'] = df['HA_Close'].rolling(window=5).mean()
    df['MA8'] = df['HA_Close'].rolling(window=8).mean()
    df['MA13'] = df['HA_Close'].rolling(window=13).mean()
    df['MA21High'] = df['HA_High'].rolling(window=21).mean()
    df['MA21Low'] = df['HA_Low'].rolling(window=21).mean()

    df_cleaned = df.dropna()
    return df_cleaned
 
def Get_Trailing_Stop(symbol,current_price, average_cost, OpenTradeQuantity,AllowTrailing,StopLossPerc):
    TrailPrice=0
    write_to_log('******Updating Trailing**********')
    try:
        file_path = symbol+'TrailPrice.ini'
        file_path_obj = Path(file_path)
        if file_path_obj.exists():
            GetTrailConfig=read_config(filename=file_path)  
            TrailPrice= float(GetTrailConfig.get(symbol, symbol))
            write_to_log('******TrailPrice : *',TrailPrice)
    except (ValueError, KeyError, IndexError) as e:
        write_to_log(f'***Error in Getting Trail price: {e}****')
        TrailPrice=0

    if TrailPrice==0 or AllowTrailing=='N':
        TrailPrice=average_cost
    
    if OpenTradeQuantity>0:# For Closing Buy Position
       if AllowTrailing=='Y' and current_price/average_cost>1:
           TrailPrice= max(TrailPrice,current_price,average_cost)
       TrailPriceStopLoss=TrailPrice*(1-StopLossPerc)    

    elif OpenTradeQuantity<0: #For Closing Sell Position
       if AllowTrailing=='Y'  and average_cost/current_price>1:
           TrailPrice= min(TrailPrice,current_price,average_cost)
       TrailPriceStopLoss=TrailPrice*(1+StopLossPerc)

    data_to_write(TrailPrice,symbol)     
    write_to_log("***Min TrailPrice,TrailPriceStopLoss***",TrailPrice,TrailPriceStopLoss)
    return TrailPriceStopLoss

 


def GetSetupParam(ConfigKey):
    '''Read setup parameters from a configuration file and return as a dictionary.'''
    config = read_config('SymbolSetup.ini')
    
    # Create a dictionary to store key-value pairs of setup parameters
    setup_params = {
        'correction': float(config.get('InitParam', 'Correction')),
        'IsProgram':  config.get('InitParam', 'IsProgram'),
        'MAPeriod': int(config.get(ConfigKey, 'MAPeriod')),
        'MATimeFrame' : config.get(ConfigKey, 'MATimeFrame'),
        'shift': int(config.get('InitParam', 'shift')),
        'InnerRangePerc' : float(config.get('InitParam', 'InnerRangePerc')),
        'OuterRangePerc' : float(config.get('InitParam', 'OuterRangePerc')),
        'TradeMethod' : (config.get(ConfigKey, 'TradeMethod')),
        'BuyRange': float(config.get(ConfigKey, 'BuyRange')),
        'StopLossPerc': float(config.get(ConfigKey, 'StopLossPerc')),
        'TargetProftPerc': float(config.get(ConfigKey, 'TargetProftPerc')),
        'Qty': float(config.get(ConfigKey, 'Qty')),
        'InvestAmt': float(config.get(ConfigKey, 'InvestAmt')),
        'ClientOrderID': config.get(ConfigKey, 'ClientOrderID'),
        'Pair': config.get(ConfigKey, 'Pair'),
        'AllowTrading': config.get(ConfigKey, 'AllowTrading'),
        'LongExit': float(config.get(ConfigKey, 'LongExit')),
        'ShortExit': float(config.get(ConfigKey, 'ShortExit')),
        'AllowTrailing': config.get(ConfigKey, 'AllowTrailing'),
        'AlgoType': config.get(ConfigKey, 'AlgoType'),
        'PriceFactor': float(config.get(ConfigKey, 'PriceFactor')),
        'StopTrailingPerc': float(config.get(ConfigKey, 'StopTrailingPerc')),
        'LongEntry' : float(config.get(ConfigKey, 'LongEntry')),
        'ShortEntry' : float(config.get(ConfigKey, 'ShortEntry')),
        'DecimalPlace' : int(config.get(ConfigKey, 'DecimalPlace')),
        'QtyRounding' : int(config.get(ConfigKey, 'QtyRounding'))
        
    }
    
    return setup_params
 
def FastMASingMAStrategy(last_row,current_price,setup_params):
    #Very fast 
    MA5InRagnge=last_row["MA5"]>last_row["MA21Low"] and last_row["MA5"]<last_row["MA21High"]
    MA8InRagnge=last_row["MA8"]>last_row["MA21Low"] and last_row["MA8"]<last_row["MA21High"]
    MA13InRagnge=last_row["MA13"]>last_row["MA21Low"] and last_row["MA13"]<last_row["MA21High"]

    isMABuyCondMatch =   (current_price>last_row["HA_High"] and  current_price>last_row["MA21Low"] and MA5InRagnge and MA8InRagnge and MA13InRagnge  )
    isMASellCondMatch =  ( current_price<last_row["HA_Low"] and  current_price<last_row["MA21High"]  and MA5InRagnge and MA8InRagnge and MA13InRagnge )
    
    write_to_log("current_price,MA21Low,MA21High:   ",current_price,last_row["MA21Low"] ,last_row["MA21High"] )                           
    write_to_log("last_row[5],last_row[MA8],FastMA,last_row[MA13]:   ",last_row["MA5"],last_row["MA8"] ,last_row["MA13"] )                           
      
    MAGapPercnt=last_row["MASLRatio"]  

    return isMABuyCondMatch,isMASellCondMatch,MAGapPercnt


def FixPriceStrategy(last_row,current_price,setup_params):
    #Very fast 
    MAGapPercnt= setup_params["StopLossPerc"] 
    isMABuyCondMatch =   (current_price>last_row["HA_Close"] 
                          and last_row["IsGreen"]=="G" 
                          and current_price>setup_params['LongEntry'] 
                          and current_price<setup_params['LongEntry']*(1+MAGapPercnt))
    isMASellCondMatch =  (current_price<last_row["HA_Close"] 
                         and last_row["IsGreen"]=="R" 
                         and current_price<last_row["HA_Low"] 
                         and current_price<setup_params['ShortEntry'] 
                         and current_price>setup_params['ShortEntry']*(1-MAGapPercnt))
    write_to_log("current_price,LongEntry,ShortEntry:   ",current_price,setup_params["LongEntry"] ,setup_params["ShortEntry"] )                           

    return isMABuyCondMatch,isMASellCondMatch,MAGapPercnt


def MACandleStrategy(df,  current_price):
    last_row=df.iloc[-1]
    last_row_1=df.iloc[-2]
    last_row_2=df.iloc[-3]

    MAGapPercnt=last_row["MASLRatio"]
    isMABuyCondMatch =   (current_price>last_row["HA_Close"] 
                          and last_row["IsGreen"]=="G"
                          and last_row_1["IsGreen"]=="R"
                          and last_row_2["IsGreen"]=="R"
                          and last_row["MA5"]<current_price
                          )
    isMASellCondMatch = ( current_price<last_row["HA_Close"] 
                         and last_row["IsGreen"]=="R"
                         and last_row_1["IsGreen"]=="G"
                         and last_row_2["IsGreen"]=="G"
                         and last_row["MA5"]>current_price
                        ) 
    return isMABuyCondMatch,isMASellCondMatch,MAGapPercnt

def SlowMAStrategy(last_row,  current_price):
     #For MA Price
    MAGapPercnt=last_row["MASLRatio"]  +last_row["MASLRatio"]/2

    MABuyUpperRange=last_row["FastUpperMA"] + last_row["MAHLRange"]
    MABuyLowerRange=(last_row["FastMA"] - last_row["MAHLRange"]/2)

    MASellUpperRange=last_row["FastMA"] + last_row["MAHLRange"]/2
    MASellLowerRange=last_row["FastLowerMA"] - last_row["MAHLRange"] 


    isMABuyCondMatch =   (current_price>last_row["HA_High"] 
                          and current_price<MABuyUpperRange 
                          and current_price>MABuyLowerRange
                          and last_row["MA5"]<current_price
                          )
    isMASellCondMatch = ( current_price<last_row["HA_Low"] 
                         and current_price<MASellUpperRange 
                         and current_price>MASellLowerRange
                         and current_price<last_row["MA5"]
                        ) 
    
    write_to_log("current_price,FastUpperMA,FastMA,FastLowerMA:   ",current_price,last_row["FastUpperMA"] ,last_row["FastMA"],last_row["FastLowerMA"]  )                           
    write_to_log("MABuyUpperRange ,MABuyLowerRange,MASellUpperRange,MASellLowerRange, istrue",MABuyUpperRange ,MABuyLowerRange,MASellUpperRange,MASellLowerRange,last_row["MA5"]>last_row["MA8"])
    
    return isMABuyCondMatch,isMASellCondMatch,MAGapPercnt


def PriceMACrossOverStrategy(last_row,  current_price,BuyRange):
     #For MA Price
    MAGapPercnt=last_row["MASLRatio"]  +last_row["MASLRatio"]/2

    MABuyUpperRange=last_row["FastUpperMA"]*(1+BuyRange)
    MABuyLowerRange=last_row["FastMA"]  

    MASellUpperRange=last_row["FastMA"] 
    MASellLowerRange=last_row["FastLowerMA"]*(1-BuyRange)


    isMABuyCondMatch =   (current_price>last_row["HA_High"] 
                          and current_price<MABuyUpperRange 
                          and current_price>MABuyLowerRange
                          and last_row["MA5"]<current_price
                          )
    isMASellCondMatch = ( current_price<last_row["HA_Low"] 
                         and current_price<MASellUpperRange 
                         and current_price>MASellLowerRange
                         and current_price<last_row["MA5"]
                        ) 
    
    MAGapPercnt=BuyRange
    write_to_log("current_price,FastUpperMA,FastMA,FastLowerMA:   ",current_price,last_row["FastUpperMA"] ,last_row["FastMA"],last_row["FastLowerMA"]  )                           
        
    return isMABuyCondMatch,isMASellCondMatch,MAGapPercnt

def getBuySellCloseSignal(symbol):
    isBuyCondMatch=False
    isSellCondMatch=False
    signal=0
    #try:
    if True:
        write_to_log('************ StartTrading *******************')
        #dfOO = RequestType('OO') 
        #write_to_log(dfOO)
        ProcessPriceSetupFileToLocal()
        setup_params=GetSetupParam(symbol)
        current_price = getMidPrice(setup_params['Pair'] )  
        df=GetMAVal(setup_params['Pair'], MAPerid=setup_params['MAPeriod'],period=setup_params['MATimeFrame'],PriceBand=setup_params['BuyRange'])
        last_row = df.iloc[-1]
        TradeMethod=setup_params['TradeMethod']
        BuyRange=setup_params['BuyRange']

        #isBuyCondMatch,isSellCondMatch,MAGapPercnt = SlowMAStrategy(last_row, current_price)
        if(TradeMethod=="FIX"):
            isBuyCondMatch,isSellCondMatch,MAGapPercnt = FixPriceStrategy(last_row,current_price,setup_params)
        elif (TradeMethod=="MAC"):
            isBuyCondMatch,isSellCondMatch,MAGapPercnt = PriceMACrossOverStrategy(last_row,current_price,BuyRange)    
        else:    
            isBuyCondMatch,isSellCondMatch,MAGapPercnt = MACandleStrategy(df, current_price)
            
        MAGapPercnt=round(float(read_write_data(MAGapPercnt,symbol,"MAGapPercnt","W")),3)
        

        write_to_log("current_price,MATimeFrame,MAPeriod :   ",current_price,setup_params['MATimeFrame'],setup_params['MAPeriod'])
        write_to_log("isBuyCondMatch , isSellCondMatch ,MAGapPercnt :   ",isBuyCondMatch,isSellCondMatch,MAGapPercnt)

        if isBuyCondMatch:
            signal=1
        if isSellCondMatch:
            signal=-1     

        #send_notification("testing singal",symbol,str(current_price))    
        
    return signal ,MAGapPercnt,last_row

def IsPositionOpen(symbol):
    IsPostionOpen=False
    dfOP = RequestType('OP') 
    if not dfOP.empty: 
       filtered_dfOP = dfOP[dfOP['symbol'] == symbol]
       if len(filtered_dfOP)>0:
          IsPostionOpen=True
          write_to_log("***Orders still Open**")
    else:
        write_to_log("**No order Open**")
    return IsPostionOpen

def invest_based_on_signal(signal, current_price, high_high, low_low, investment_amount):
    # Calculate the range (difference) between the highest high and lowest low
    price_range = high_high - low_low
    distance_from_high = high_high - current_price
    proportion =  (price_range - distance_from_high )/ price_range
    if signal=='buy':
       proportion = 1- proportion

    invested_amount = proportion * investment_amount
    write_to_log("high_high,low_low,distance_from_high,proportion,proportion",high_high,low_low,distance_from_high,proportion,proportion)
    return invested_amount


def SendOrder(symbol,Qty,ClientOrderID,orderPrice,BuyOrSellSide,Optiontype="LMT"):

    data = RequestType('NO',Symbol=symbol,
            Qty=Qty,
            ClientOrderID=ClientOrderID, 
            orderPrice=orderPrice ,
            OpType=Optiontype,
            BuyOrSell=BuyOrSellSide)
    write_to_log('New {buysellind} Order Response:', data.iloc[-1])  

def CancelOpenLimitOrders(symbol):
    df = RequestType('OO') 
    if not df.empty:
        write_to_log(df)
        dfOpnOrdersFilt = df[df['symbol'] == symbol]
        if not dfOpnOrdersFilt.empty:
            for index, row in dfOpnOrdersFilt.iterrows():
                write_to_log(row)
                write_to_log(symbol,"******Canceling Orders ********")
                RequestType('CNCL', OrderID=row['order_id'])

def AllowToOpenLimitOrder(symbol):
    write_to_log(symbol,"******Can Open new Limit Order ********")
    CanOpenLimitOrders=False
    dfOpnOrders = RequestType('OO') 
    if dfOpnOrders.empty:
        CanOpenLimitOrders =True
    else :
        dfOpnOrdersFilt = dfOpnOrders[dfOpnOrders['symbol'] == symbol]
        if dfOpnOrdersFilt.empty:
            CanOpenLimitOrders =True
        else:    
            write_to_log(symbol,"**Limit Orders are Present **")
            write_to_log(dfOpnOrdersFilt)
    #Return Value
    return CanOpenLimitOrders

    
def CloseOrder(symbol,setup_params,OpenTradeQuantity,CloseSide,Correction,ClientID,AskBid):
    #*************CLOSING  ORDER*****************************
    write_to_log(symbol,'========================Close Position==========================================')
    orderPrice =round(getMidPrice(setup_params['Pair'],AskBid)*(1+ Correction),setup_params['DecimalPlace'])
    write_to_log(symbol, " Close Order price and Qty",FormatNumber(orderPrice),abs(OpenTradeQuantity))
    SendOrder(symbol, abs(OpenTradeQuantity),ClientID,orderPrice,CloseSide,Optiontype='IC')
    message = f"Close Order Place - {symbol}\nPrice: ${orderPrice}\nQty: {OpenTradeQuantity}\nSignal: {CloseSide}"
    send_notification(message)  

def OpenLimitOrders(symbol,notional_value ,setup_params,CanClosePosition,current_price,ClientID,CloseSide,BuySellSign,MAGapPercnt,average_cost):
    #*************OPENING LIMIT ORDER*****************************
    if (AllowToOpenLimitOrder(symbol) and  notional_value > 2000 and  CanClosePosition==False): #and unrealised_pnl>0
        CancelOpenLimitOrders(symbol)
        write_to_log("Opening Limit Orders")   
        
        OrderPrice=average_cost
        if current_price>average_cost:
           OrderPrice=current_price 

        LimitPrice1=round(OrderPrice*(1+BuySellSign * 1.5 * MAGapPercnt),setup_params['DecimalPlace'])
        LimitPrice2=round(OrderPrice*(1+BuySellSign * 2.5 * MAGapPercnt),setup_params['DecimalPlace'])
        LimitPrice3=round(OrderPrice*(1+BuySellSign * 3.5 * MAGapPercnt),setup_params['DecimalPlace'])
        LimitPrice4=round(OrderPrice*(1+BuySellSign * 5 * MAGapPercnt),setup_params['DecimalPlace'])

        Qty1=round(notional_value*(0.30)/current_price,setup_params['QtyRounding'])
        Qty2=round(notional_value*(0.25)/current_price,setup_params['QtyRounding'])
        Qty3=round(notional_value*(0.25)/current_price,setup_params['QtyRounding'])
        Qty4=round(notional_value*(0.10)/current_price,setup_params['QtyRounding'])

        write_to_log(symbol,'========================Limit Order 1==========================================')
        #==Send Order
        SendOrder(symbol, abs(Qty1),ClientID,LimitPrice1,CloseSide)
        SendOrder(symbol, abs(Qty2),ClientID,LimitPrice2,CloseSide)
        SendOrder(symbol, abs(Qty3),ClientID,LimitPrice3,CloseSide)
        SendOrder(symbol, abs(Qty4),ClientID,LimitPrice4,CloseSide)

def OpenNewOrder(symbol,current_price,signal,last_row): 
    setup_params=GetSetupParam(symbol)  
    ClientID=setup_params['ClientOrderID']
    data_to_write(current_price,symbol)   
    if(signal==1 or signal==-1) and  setup_params['AllowTrading']=='Y': # and False :
        write_to_log('========================Open New Position==========================================')
        
       
        if signal==1:
            AskBid='Bid'
            buysellind = 'buy'
            correction_factor = 1 + setup_params['correction']
        if signal==-1: 
            AskBid='Ask'
            buysellind ='sell'
            correction_factor = 1 - setup_params['correction']

        current_price = getMidPrice(setup_params['Pair'],AskBid) 
        invested_amount=setup_params['InvestAmt']
        #invested_amount = invest_based_on_signal(buysellind, current_price, last_row["HighHigh"], last_row["LowLow"], invested_amount)
        Qty=round(invested_amount/current_price,setup_params['QtyRounding'])    
        orderPrice=round(current_price * correction_factor, setup_params['DecimalPlace'])
        SendOrder(symbol, Qty,ClientID,orderPrice,buysellind,Optiontype='IC')
        message = f"Order opened - {symbol}\nPrice: ${orderPrice}\nQty: {Qty}\nSignal: {buysellind}\ninvested_amount: {invested_amount}"
        send_notification(message)  
        
                       
def OpenCloseTrade(symbol):   
    if True:
      
        average_cost=0
        OpenTradeQuantity=0
        CloseSide=''
        CanClosePosition=False
        TrailPriceStopLoss=0
        mark_price=0.0
        CanOpenLimitOrders=False
        notional_value=0
        AskBid=''

        ProcessPriceSetupFileToLocal()
        setup_params=GetSetupParam(symbol)
        
        ClientID=setup_params['ClientOrderID']
        

        #StopLossParam=MAGapPercnt #setup_params['StopLossPerc']
        signal,MAGapPercnt,last_row=  getBuySellCloseSignal(symbol)
        MAGapPercnt=float(read_write_data(0,symbol,"MAGapPercnt","R"))
        write_to_log("****MAGapPercnt***",MAGapPercnt)
        current_price = getMidPrice(setup_params['Pair'] )  
        dfOP = RequestType('OP') 
        write_to_log(dfOP)
        
        IsPosOpen=IsPositionOpen(symbol)
        current_minute = datetime.datetime.now().minute
        if current_minute % 30 == 0:
           send_notification(f"{symbol}:{signal}")

        if IsPosOpen==False:
        #    signal,MAGapPercnt,last_row=  getBuySellCloseSignal(symbol)
            write_to_log("Canceling Limit Orders as No Orders Opened")   
            CancelOpenLimitOrders(symbol)
            OpenNewOrder(symbol,current_price,signal,last_row) 

        else:
           
        
            filtered_dfOP = dfOP[dfOP['symbol'] == symbol]
            write_to_log('symbol : ',symbol)
            average_cost = float(filtered_dfOP['average_cost'].values[0])
            OpenTradeQuantity = float(filtered_dfOP['quantity'].values[0])
            unrealised_pnl=float(filtered_dfOP['unrealised_pnl'].values[0])
            mark_price=float(filtered_dfOP['mark_price'].values[0])
            notional_value=abs(float(filtered_dfOP['notional_value'].values[0]))

            try:
                if current_minute % 30 == 0:
                    msg=''
                    for index, row in dfOP.iterrows():
                        for column, value in row.items():
                            msg=msg+f"{column}: {value}\n"
                    send_notification(msg)
            except Exception as e:
                 write_to_log(f'An exception occurred: in sending message{e}')
            
   
            
            # For Closing Buy Position
            if OpenTradeQuantity>0:
                AskBid='Ask'
                CloseSide='sell' 
                BuySellSign=1
                Correction=setup_params['correction']
                TrailPriceStopLoss=round(average_cost*(1-MAGapPercnt),setup_params['DecimalPlace'])
                TrailPriceStopLoss=Get_Trailing_Stop(symbol,mark_price, average_cost, OpenTradeQuantity,setup_params['AllowTrailing'],  MAGapPercnt)
                TrailPriceStopLoss=max(last_row["FastLowerMA"], TrailPriceStopLoss ) #round(average_cost*(1-2*MAGapPercnt),setup_params['DecimalPlace']))
                if setup_params["TradeMethod"]=="MAC":
                    TrailPriceStopLoss=last_row["FastLowerMA"]
                CanClosePosition=  ( TrailPriceStopLoss > mark_price or current_price/average_cost>setup_params['TargetProftPerc'])

                OpenLimitOrders(symbol,notional_value ,setup_params,CanClosePosition,current_price,ClientID,CloseSide,BuySellSign,MAGapPercnt,average_cost)

            #For Closing Sell Position    
            elif OpenTradeQuantity<0:
                AskBid='Bid'
                CloseSide='buy'
                BuySellSign=-1
                Correction=-setup_params['correction']
                TrailPriceStopLoss=round(average_cost*(1+MAGapPercnt),setup_params['DecimalPlace'])
                TrailPriceStopLoss=Get_Trailing_Stop(symbol,current_price, average_cost, OpenTradeQuantity,setup_params['AllowTrailing'],   MAGapPercnt)
                TrailPriceStopLoss=min(last_row["FastUpperMA"] ,TrailPriceStopLoss) #round(average_cost*(1+2*MAGapPercnt),setup_params['DecimalPlace']))
                if setup_params["TradeMethod"]=="MAC":
                    TrailPriceStopLoss=last_row["FastUpperMA"]
                
                CanClosePosition= (TrailPriceStopLoss < mark_price or average_cost/mark_price>setup_params['TargetProftPerc'] )

                OpenLimitOrders(symbol,notional_value ,setup_params,CanClosePosition,current_price,ClientID,CloseSide,BuySellSign,MAGapPercnt,average_cost)  

            if(CanClosePosition):
                CancelOpenLimitOrders(symbol)
                CloseOrder(symbol,setup_params,OpenTradeQuantity,CloseSide,Correction,ClientID,AskBid)
 
            write_to_log('CanOpenLimitOrders,CanClosePosition,OpenTradeQuantity,average_cost  TrailPriceStopLoss: ')
            write_to_log(CanOpenLimitOrders,CanClosePosition,FormatNumber(OpenTradeQuantity),FormatNumber(average_cost), FormatNumber(TrailPriceStopLoss))       
        


def send_notification(*argmsg,timeInterval=0):
    config = read_config('C:\\Users\\jaina\\Gaurav\\Gemini\\Config\\APIKey.ini')
    bot_token = config.get('TGSIGNALBOT', 'TOKEN')
    chat_id = config.get('TGSIGNALBOT', 'CHAT_ID')
    current_minute = datetime.datetime.now().minute
    msg = "\n".join(argmsg)
    msg=msg.replace('_',' ')
    
    if timeInterval==0 or (timeInterval>0 and current_minute % timeInterval == 0):
        url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        params = {'chat_id': chat_id, 'text': msg, 'parse_mode': 'Markdown'}
        response = requests.post(url, json=params)
        if response.status_code != 200:
            print("Failed to send message. Status code:", response.status_code)


def open_close_trade(symbol):
    try:
        time.sleep(2)
        write_to_log(f'************ {symbol.upper()} *******************')
        setup_params=GetSetupParam(symbol)
        if setup_params['IsProgram']=='ON':
           OpenCloseTrade(symbol)
    except Exception as e:
        write_to_log(f'An exception occurred: {e}')
        send_notification(f'An exception occurred: {e}',timeInterval=3)


def open_close_trade1(symbol):
    write_to_log(f'************ {symbol.upper()} *******************')
    setup_params=GetSetupParam(symbol)
    if setup_params['IsProgram']=='ON':
        OpenCloseTrade(symbol)
    time.sleep(3)


def Tradejob():
    while True:
        write_to_log('************ Thread Started *******************')
        ProcessPriceSetupFileToLocal()
        current_time = datetime.datetime.now()
        write_to_log('Current Time:', current_time) 
        
        
        dfBal = RequestType('Bal')
        filtered_dfBal = (dfBal[dfBal['currency'] == 'GUSD']) 
        write_to_log(filtered_dfBal)
    

        
        open_close_trade('ethgusdperp')
        open_close_trade('btcgusdperp')
        open_close_trade('solgusdperp')
         
        if current_time.minute % 15 == 0:
            clear_output(wait=True)


def test():
    symbol='solgusdperp'
    setup_params=GetSetupParam(symbol)
    current_price = getMidPrice(setup_params['Pair']) 
    df=GetMAVal(setup_params['Pair'], MAPerid=setup_params['MAPeriod'],period=setup_params['MATimeFrame'],PriceBand=setup_params['BuyRange'])
    plot(df)
     

if __name__ == '__main__':
    LogFileName=""
    #test()
    # sleep for 30 Seconds
    write_to_log('Initiated....')
    Tradejob()
    #CancelOpenLimitOrders('ethgusdperp')
    #CancelOpenLimitOrders('solgusdperp')
    
