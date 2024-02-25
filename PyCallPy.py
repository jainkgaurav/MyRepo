from GeminiTrading import RequestType,read_config,getMidPrice,GetMAVal,getOHLCData,getCurrentCandle,send_notification,RequestType
import time
import binance
import pandas as pd
import requests
import datetime


config = read_config()
gemini_api_key = config.get('GeminiAPI', 'gemini_api_key')
gemini_api_secret = config.get('GeminiAPI', 'gemini_api_secret').encode()
#row=getScriptPrice('ethusd').iloc[-1]

def BinanceData():
    url = "https://api.binance.com/api/v3/klines?symbol=INJUSDT&interval=15m"

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    df = pd.DataFrame(data)

    column_mapping = {0: 'timestamp', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume'}
    df = df.rename(columns=column_mapping)
    df['timestamp_ts'] = pd.to_datetime(df['timestamp'], unit='ms')

    print(df)


def HeikinAshi3Candle(symbol):
    signal=""
    price=getMidPrice(symbol)
    df=GetMAVal(symbol,MAPerid=5 ,period='1hr')  
    last_row=df.iloc[-1]
    last_row_1=df.iloc[-2]
    last_row_2=df.iloc[-3]

    MAGapPercnt=last_row["MASLRatio"]
    isMABuyCondMatch =   (price>last_row["HA_High"] 
                          and last_row["IsGreen"]=="G"
                          and last_row_1["IsGreen"]=="R"
                          and last_row_2["IsGreen"]=="R"
                           
                          )
    isMASellCondMatch = ( price<last_row["HA_Low"] 
                         and last_row["IsGreen"]=="R"
                         and last_row_1["IsGreen"]=="G"
                         and last_row_2["IsGreen"]=="G"
                        ) 
    if isMABuyCondMatch:
        signal="Buy"
        send_notification(f"Heikin Ashi 3 Candle\n{symbol}:\n{price}\n{signal}")
    if isMASellCondMatch:
        signal="Sell"
        send_notification(f"Heikin Ashi 3 Candle\n{symbol}:\n{price}\n{signal}")
     
    return isMABuyCondMatch,isMASellCondMatch,MAGapPercnt

def CheckRangeHABreakOut(symbol):
    signal=""
    price=getMidPrice(symbol)
    df=GetMAVal(symbol,MAPerid=5 ,period='1hr')  
    row=df.iloc[-1]
    row1=df.iloc[-2]
    row2=df.iloc[-3]
    Open,High,Low=getCurrentCandle(row,symbol)
    Range=max(High,price)-min(Low,price)
    isRangeBreakout=row["MAHLRange"]<Range and row1["MAHLRange"]>Range and row2["MAHLRange"]>Range
    isBuy=isRangeBreakout and price>row["High"]  and row["IsGreen"]=="G" #and Open<price 
    isSell=isRangeBreakout and price>row["Low"]  and row["IsGreen"]=="R" #and Open>price
    if isBuy:
        signal="Buy"
        send_notification(f"HeikinAshi Break Out\n{symbol}:\n{price}\n{signal}")
    if isSell:
        signal="Sell"
        send_notification(f"HeikinAshi Break Out\n{symbol}:\n{price}\n{signal}")
    
    return isBuy,isSell,row["MASLRatio"]


def CheckRangeBreakOut(symbol):
    signal=""
    price=getMidPrice(symbol)
    df=getOHLCData(symbol, period='15m')   
    #Open,High,Low=getCurrentCandle(row,symbol)
    

    row=df.iloc[-1]
    MARange=round(row["MAHLRange"],2)
    Range1=round(abs(row["High"]-row["Low"]),2)
    IsRngBO1=MARange<Range1 
    IsCurrRangeBO=abs(row["Close"]-price)>MARange
    
    row1=df.iloc[-2]
    Range2=round(abs(row1["High"]-row1["Low"]),2)
    IsRngBO2=MARange>Range2  
    
    row2=df.iloc[-3]
    Range3=round(abs(row2["High"]-row2["Low"]),2)
    IsRngBO3=MARange>Range3  
    
    IsRangeBO=IsRngBO1==False and IsRngBO2==False and IsRngBO3==False and IsCurrRangeBO

    isBuy=IsRangeBO and price>row["High"]  
    isSell=IsRangeBO and price>row["Low"]  
    
    send_notification(f"Range Break Out\n{symbol}:\n{price}\n{signal}\n{MARange},{Range1},{Range2},{Range3}")
    if isBuy:
        signal="Buy"
        send_notification(f"Range Break Out\n{symbol}:\n{price}\n{signal}\n{MARange},{Range1},{Range2},{Range3}")
    if isSell:
        signal="Sell"
        send_notification(f"Range Break Out\n{symbol}:\n{price}\n{signal}\n{MARange},{Range1},{Range2},{Range3}")
    
    CheckBalanceIfBelowLevel(row["MASLRatio"])
    return isBuy,isSell,row["MASLRatio"]

def CheckBalanceIfBelowLevel(SLLevel):
    dfOP = RequestType('OP') 
    if not dfOP.empty: 
       filtered_dfOP = dfOP[dfOP['symbol'] == symbol]
       if len(filtered_dfOP)>0:
            filtered_dfOP = dfOP[dfOP['symbol'] == symbol]
            average_cost = float(filtered_dfOP['average_cost'].values[0])
            quantity = float(filtered_dfOP['quantity'].values[0])
            unrealised_pnl=float(filtered_dfOP['unrealised_pnl'].values[0])
            mark_price=float(filtered_dfOP['mark_price'].values[0])
            notional_value=abs(float(filtered_dfOP['notional_value'].values[0]))
            send_notification(f"Stop Loss Hit\n{symbol}:\nmark price:{mark_price}\naverage cost:{average_cost}\nnotional value :{notional_value}")
            if (quantity>0 and average_cost*(1-SLLevel)>mark_price) or (quantity<0 and average_cost*(1+SLLevel)<mark_price):
                send_notification(f"Stop Loss Hit\n{symbol}:\nmark price:{mark_price}\naverage cost:{average_cost}\nnotional value :{notional_value}")
            
def process_symbol(symbol):
    print(symbol)
    CheckRangeBreakOut(symbol)
    HeikinAshi3Candle(symbol)
    CheckRangeHABreakOut(symbol)
    

# Main loop
while True:
    try:
        current_minute = datetime.datetime.now().minute  # Assuming you've imported datetime module
        if current_minute % 3 == 0:
            symbols = ['solusd', 'ethusd']  # Add more symbols as needed
            for symbol in symbols:
                process_symbol(symbol)
            time.sleep(30)
    except Exception as e:
        send_notification(f' Exception Call Gemeni Singal File : {e}',timeInterval=6)
    
