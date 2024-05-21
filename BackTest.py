import sqlite3
import datetime
import UtilFunctions as UF
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException
from binance.enums import *
from time import strftime, localtime
from datetime import datetime, timedelta
import time
import numpy as np
import sys
import configparser
import requests
from ta.momentum import RSIIndicator
from ta.momentum import RSIIndicator
from ta.trend import ADXIndicator, MACD,PSARIndicator
import BinanceTrading as BinTrade

def send_notification(*argmsg,timeInterval=0,MY_CHAT_ID=0):
    config = read_config('APIKey.ini')
    bot_token = config.get('TGBotMsg', 'TOKEN')
    if MY_CHAT_ID==0:
       chat_id = config.get('TGBotMsg', 'CHAT_ID')
    else:
       chat_id = config.get('TGBotMsg', 'MY_CHAT_ID')   
    isEnable = config.get('TGBotMsg', 'ENABLE')
    current_minute = datetime.now().minute
    msg = "\n".join(argmsg)
    msg=msg.replace('_',' ')
    
    if timeInterval==0 or (timeInterval>0 and current_minute % timeInterval == 0) and isEnable=="Y":
        url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        params = {'chat_id': chat_id, 'text': msg, 'parse_mode': 'Markdown'}
        response = requests.post(url, json=params)
        if response.status_code != 200:
            print("Failed to send message. Status code:", response.status_code)


def read_config(filename='GeminiConfig.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    # Read configuration file
    #config = read_config()
    return config

param= read_config("APIKey.ini")
client = Client(api_key=param.get("BNBKey","APIKey"),api_secret=param.get("BNBKey","SecKey"), tld='com', testnet=False)

def UpdateDBTrades():
    while 1==1:
        try:
            BinTrade.getAllOpenOrdersToCancel()    
            BinTrade.createTPSL() 
            time.sleep(10)   
            data = client.futures_symbol_ticker()
            df=pd.DataFrame(data)
            for idx,row in df.iterrows():
                symbol=row["symbol"]
                current_price = float(row["price"])
                insert_or_update_trade_entry(symbol,current_price)
                time.sleep(1)
            
        except Exception as e: 
            print("Not found",e) 
            time.sleep(10)
            if "banned" in str(e):
                print("Banned detected, sleeping for 300 seconds")
                time.sleep(300)


def PriceAtSupportResitance(ranges,symbol,df,price,comment):
    margin_percent = 0.01
    range_data =df
    HH = range_data['High'].max()
    LL = range_data['Low'].min()
    cntLL=0
    cntHH=0
    currrow=df.iloc[-1]
    currrow2=df.iloc[-2]
    currrow3=df.iloc[-3]
    Singnal=currrow3["IsHAGreen"]+currrow2["IsHAGreen"]+currrow["IsHAGreen"]
    for start, end in ranges:
        if end > len(df):
            end = len(df)
        
        range_data = df.iloc[start:end]
        range_high = range_data['High'].max()
        range_low = range_data['Low'].min()
        
        if range_high * (1 - margin_percent) <HH and range_high * (1 + margin_percent)>HH:
            cntHH=cntLL+1

        if range_low * (1 - margin_percent) <LL and range_low * (1 + margin_percent)>LL:
            cntLL=cntLL+1    

    if HH/LL>1.15:
        if price/LL>1.01 and price/LL<1.015  and price>currrow2["Open"] :
            if cntLL>1:
                insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"NearResitanceSupport", Gap=f"GAP:{str(round(HH/LL,2))}\n{Singnal}\nHigh:{HH}\nLow{str(LL)}") 
        if HH/price>1.01 and HH/price<1.015 and price<currrow2["Open"] :
            if cntHH>1:
               insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"NearResitanceSupport", Gap=f"GAP:{str(round(HH/LL,2))}\n{Singnal}\nHigh:{HH}\nLow{str(LL)}") 
 
       
def generate_ranges(start, end, gap):
    ranges = []
    for i in range(start, end, gap):
        ranges.append((i, i + gap - 1))
    return ranges


def getSignalsToTrade():
    while 1==1:
        try:
            print("RUNNNIG>>>>>>")
            BinTrade.CloseAllOpenOrders()    
            TotPos=BinTrade.get_all_open_positions()
            if TotPos>=0 and TotPos<4 :
                cnt=1
                data = client.futures_symbol_ticker()
                df=pd.DataFrame(data)
                for idx,row in df.iterrows():
                    symbol=row["symbol"]
                    current_price = float(row["price"])
                    if  symbol.endswith("USDT"):
                            cnt=cnt+1
                            if cnt%50==0:
                               time.sleep(5) 
                            #response = client.futures_change_leverage(symbol=symbol, leverage=20)
                            lenRange=400
                            ranges = generate_ranges(0, lenRange, 100) 
                            dfHist=get_historical_prices(symbol,interval='15m',MinOrDayAgo="20 days", limit=lenRange)
                            try:
                                if dfHist is None:
                                    print(symbol,"No Data")
                                else :        
                                    PriceAtSupportResitance(ranges,symbol,dfHist,current_price,"Range 3time")
                                    getTradeSignal(dfHist,symbol,current_price,'30m') 
                                
                                cnt=cnt+1
                                dfHist=get_historical_prices(symbol,interval='5m',MinOrDayAgo="10 days", limit=lenRange)
                                
                                if dfHist is None:
                                    print(symbol,"No Data")
                                else :        
                                    getTradeSignal(dfHist,symbol,current_price,'30m') 

                            except BinanceAPIException as e: 
                                    print("Not found",symbol,e) 
                                    time.sleep(5)
                                    if "banned" in str(e):
                                        print("Banned detected, sleeping for 300 seconds")
                                        time.sleep(300)        
            
            print("createTPSL")
            BinTrade.createTPSL() 
            print("CloseAllOpenOrders")
            BinTrade.CloseAllOpenOrders()
            data = client.futures_symbol_ticker()
            df=pd.DataFrame(data)
            print("Loop...")
            for idx,row in df.iterrows():
                symbol=row["symbol"]
                current_price = float(row["price"])
                insert_or_update_trade_entry(symbol,current_price)
                #print(symbol)
            time.sleep(5)
                                    
        except BinanceAPIException as e: 
                print("Not found",e) 
                time.sleep(5)
                if "banned" in str(e):
                    print("Banned detected, sleeping for 300 seconds")
                    time.sleep(300)

 

def read_table_to_dataframe():
    db_path = 'Backtest.db'
    conn = sqlite3.connect(db_path)
    query=""
    query=query+"select  Strategy,sum(PnL) PnL,SLRate,TPRate,  count(1) Trades,count(distinct symbol) CntSymbol "
    query=query+" ,sum(case when PnL>0 then 1 else 0 end) Postive, sum(PnL)/ count(1) PerTrade from vwTradeEntry "
    query=query+" group by  SLRate,TPRate,Strategy having sum(PnL)/ count(1)>.5 and  count(1)>2  order by   8 desc,2 desc ,7 desc, 5 desc   limit 10  "

    #query = f"select -1 x, symbol , sum(PnL) PnL from {table_name} group by symbol  union select 0 x,  'Total',  sum(PnL) from {table_name}  order by 1 "
    df = pd.read_sql_query(query, conn)
    result_str = ""  # Initialize an empty string to store the result
    cnt=0
    for index, row in df.iterrows():
        Strategy=row["Strategy"]
        PnL=str(round(row["PnL"],2))
        SLRate=str(round(row["SLRate"],3))
        TPRate=str(round(row["TPRate"],3))
        CntSymbol=str(row["CntSymbol"])
        Postive=str(row["Postive"])
        Trades=str(row["Trades"])
        PerTrade=str(round(row["PerTrade"],2))
        cnt=cnt+1

        result_str += f"\n\n{str(cnt)} --> {Strategy} : {PnL}% :SLRate {SLRate}:TPRate {TPRate}:CntSymbol {CntSymbol}:Postive {Postive}: TotalTrades {Trades}"  # Append each row to the result string
        result_str +=f" PerTrade {PerTrade}%"
    print(result_str)  # Print the result string
    if len(result_str)>5:
        send_notification(result_str)
    conn.close()

def insert_or_update_trade_entry( Symbol, Price,Side="",Strategy="",Gap=""):
    conn = sqlite3.connect('Backtest.db')
    c = conn.cursor() 
    UpdateSLCloseDeal(Symbol,Price,c)
    if len(Strategy)>3:
        tabName="TradeEntry"
        if 1==1:#entry is None:
            if len(Side)>2:
                for k in range(10,30,5):
                    for l in range(10,60,5):
                        if l>=k:
                            SLRate =  k/1000
                            TPRate =  l/1000
                            if Side=="Buy":
                                SLPrice = Price*(1-SLRate)
                                TPPrice = Price*(1+TPRate)
                            else:
                                SLPrice = Price*(1+SLRate)
                                TPPrice = Price*(1-TPRate)
                            selQuery=f"SELECT count(1)   FROM {tabName} WHERE   ClosePrice=0 and Strategy='{Strategy}' and SLRate={str(SLRate)} and TPRate={str(TPRate)} Group by Strategy"
                            c.execute(selQuery) # and
                            cntEntry = c.fetchone()
                            
                            if cntEntry is None or cntEntry[0]<1000:
                                selQuery=f"SELECT * FROM {tabName} WHERE Strategy='{Strategy}' and SLRate={str(SLRate)} and TPRate={str(TPRate)} and ClosePrice=0 and Symbol='{Symbol}'"
                                c.execute(selQuery)  
                                entry = c.fetchone()
                                if entry is None:
                                    c.execute("INSERT INTO "+tabName+" (Symbol, Side, OpenPrice, SLPrice, TPPrice, SLRate, TPRate, Strategy, Gap, CurrPrice) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                            (Symbol, Side, Price, SLPrice, TPPrice, SLRate, TPRate, Strategy, Gap, Price))
                                    if SLRate==.02 and TPRate==.05 :
                                        msg=f"{Strategy}\n{Symbol}\n{Side}\nGap {Gap}\nSL:{str(SLRate)}TP:{str(TPRate)}\nPrice:{Price}"
                                        print(msg)
                                        if Strategy=="NearResitanceSupport":
                                            send_notification(msg)
                                            #BinTrade.NewOrderPlace(Symbol, Side,Price)
                                        else:
                                            send_notification(msg,MY_CHAT_ID=1)
                                            if Strategy=="RSIMACD":
                                                print(msg)
                                                #BinTrade.NewOrderPlace(Symbol, Side,Price)
                                                
                                        

    conn.commit()
    conn.close()
      

def UpdateSLCloseDeal(Symbol, Price,c):
   
        tabName="TradeEntry"
        current_minute = datetime.now().minute  # Assuming you've imported datetime module
        nTime=50
        #if current_minute % nTime == 0 and  datetime.now().second%15==0 :
        #    read_table_to_dataframe() 
            
        strPrice=str(Price) 
        updatequery=f" UPDATE {tabName} SET  SLPrice = CASE WHEN side = 'Buy' and SLPrice<{strPrice} * (1 - SLRate) THEN {strPrice} * (1 - SLRate)"
        updatequery+= f"                                    WHEN side = 'Sell' and SLPrice>{strPrice} * (1 + SLRate) THEN {strPrice} * (1 + SLRate) ELSE SLPrice END ,"
        updatequery+= f" ClosePrice = CASE  WHEN side = 'Buy'  AND (SLPrice > {strPrice} OR TPPrice < {strPrice}) THEN {strPrice}"
        updatequery+= f"                    WHEN side = 'Sell' AND (SLPrice < {strPrice} OR TPPrice > {strPrice}) THEN {strPrice} "
        updatequery+= f"                    else 0 END, CurrPrice={strPrice} WHERE ClosePrice = 0 AND Symbol = '{Symbol}'"
        #print(updatequery)
        c.execute( updatequery)

        
 
 


def get_historical_prices(symbol, interval='15m', MinOrDayAgo='3 days',  limit=500):
     
    try:
        end_date = datetime.now().strftime('%d-%b-%Y  %H:%M:%S')
        start_date = (datetime.now() - timedelta(days=3)).strftime('%d-%b-%Y %H:%M:%S')
        ##print(start_date,end_date)

        candle= client.KLINE_INTERVAL_15MINUTE
        data = client.futures_historical_klines(symbol=symbol, interval=interval, start_str= MinOrDayAgo+" ago UTC")
        
        #print(data)
        df = pd.DataFrame(data, columns=["timestamp", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "volume", "close_time", "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"])
         
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        #df['DateTime'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp', ascending=True)  # Sort by timestamp in descending order
        df.set_index('timestamp', inplace=True)
        pd.set_option('display.float_format', '{:.2f}'.format)

        if limit>0 and len(df)>limit:
           df=df.tail(limit)
       
        df['Open'] = pd.to_numeric(df['OpenPrice'])
        df['High'] = pd.to_numeric(df['HighPrice'])
        df['Low'] = pd.to_numeric(df['LowPrice'])
        df['Close'] = pd.to_numeric(df['ClosePrice'])
        pd.set_option('display.float_format', '{:.2f}'.format)
        df['IsGreen'] = np.where(df['Open'] < df['Close'], 'G', 'R') 
        #df=BinanceDataEx(df)
        df["HighHigh"] = df['High'].max()
        df["LowLow"] = df['Low'].min()

        
        df["HH50"] = df['High'].rolling(window=50).max()
        df["LL50"] = df['Low'].rolling(window=50).min()

        df["MA200"] = df['Close'].rolling(window=200).mean()
        df["MA50"] = df['Close'].rolling(window=50).mean()
        df["MA20"] = df['Close'].rolling(window=20).mean()
        df["MA5"] = df['Close'].rolling(window=5).mean()
    
        df['HA_Open']  = (df['Open'].shift(1) + df['Close'].shift(1)) / 2
        df['HA_Close']  = (df['Open'] + df['Low'] + df['Close'] + df['High']) / 4
        df['HA_High']  = df[['High', 'Open', 'Close']].max(axis=1)
        df['HA_Low']  = df[['Low', 'Open', 'Close']].min(axis=1)
        df['IsHAGreen'] = np.where(df['HA_Open'] < df['HA_Close'], 'G', 'R') 

        
        adx=ADXIndicator( high=df['High']  ,  low=df['Low'] , close=df['Close'] , window  = 14, fillna  = False)
        df["ADX"]=adx.adx()
        return df
    except Exception as e:
           print(e)
           return None
    return None
  
def ProcessOHLC(dfHist):
    
        dfHist["MA9"] = dfHist['Close'].rolling(window=9).mean()
        dfHist["HH10"] = dfHist['High'].rolling(window=10).max()
        dfHist["LL10"] = dfHist['Low'].rolling(window=10).min()
        dfHist["MA10"] = dfHist['Close'].rolling(window=10).min()
        
        dfHist["HH200"] = dfHist['High'].rolling(window=200).max()
        dfHist["LL200"] = dfHist['Low'].rolling(window=200).min()
        dfHist["UpperMA"] = dfHist['High'].rolling(window=200).mean()
        dfHist["LowerMA"] = dfHist['Low'].rolling(window=200).mean()
        dfHist["MidMA"] = dfHist['Close'].rolling(window=200).mean()
        dfHist["MAHH200"] = dfHist['MidMA'].rolling(window=200).max()
        dfHist["MALL200"] = dfHist['MidMA'].rolling(window=200).min()
        dfHist["RSI"]=RSIIndicator(close=dfHist['Close'],window=25).rsi()
        dfHist["RSIMax"]= dfHist['RSI'].shift(2).rolling(window=25).max()
        dfHist["RSIMin"]= dfHist['RSI'].shift(2).rolling(window=25).min()
        
        

        mcd=MACD(close=dfHist['Close'],window_slow=26,window_fast=12,window_sign=9,fillna=False)
        dfHist["MACDLine"]=mcd.macd()
        dfHist["MACDSignal"]=mcd.macd_signal()
        dfHist["MACDDiff"]=mcd.macd_diff()
        curr_row=dfHist.iloc[-1]
        curr_row2=dfHist.iloc[-2]
        curr_row3=dfHist.iloc[-3]
        nthLastRow=dfHist.iloc[-50]
        MAClose=curr_row["MA50"]
        MA200=curr_row["MidMA"]
        MA20=curr_row["MA20"]
        MA10=curr_row["MA10"]
        MA5=curr_row2["MA5"]
        HH200=curr_row2['HH200']
        LL200=curr_row2['LL200']
        MAHH200=curr_row2['MAHH200']
        MALL200=curr_row2['MALL200']
        HH10=curr_row2['HH10']
        LL10=curr_row2['LL10']
        nthHH=nthLastRow['HH200']
        nthLL=nthLastRow['LL200']
        HH50=curr_row2["HH50"]
        LL50=curr_row2["LL50"]
        MA50=curr_row2["MA50"]
        MA50=curr_row2["MA50"]
        HighH50 = nthLastRow['High'].max()
        LowL50 = nthLastRow['Low'].min()
        isHAGreen= curr_row["IsHAGreen"]
        Signal=curr_row3["IsHAGreen"]+curr_row2["IsHAGreen"]+curr_row["IsHAGreen"]
        rsi=round(curr_row["RSI"])
        MACDSignal=round(curr_row["MACDSignal"],2)
        MACDDiff=round(curr_row["MACDDiff"],2)
        CurrHigh=curr_row["High"]
        CurrLow=curr_row["Low"]
        HighHigh=curr_row["HighHigh"]
        LowLow=curr_row["LowLow"]
        RSIMax=curr_row["RSIMax"]
        RSIMin=curr_row["RSIMin"]
       
        return HighHigh,LowLow,MACDSignal,rsi,RSIMax,RSIMin,MA200
    
def getTradeSignal(dfHist,symbol,price,timeFrame):
        if(len(dfHist))<250:
            return
        HighHigh,LowLow,MACDSignal,rsi,RSIMax,RSIMin,MA200=ProcessOHLC(dfHist)
        BuySignal =HighHigh/LowLow>1.05 and price/LowLow<1.01 and price/LowLow>1 and rsi < 30  and  MACDSignal/price<-.02  
        SellSingal =HighHigh/LowLow>1.05 and HighHigh/price<1.01 and HighHigh/price>1 and rsi > 70  and MACDSignal/price>.03  
        print(symbol,len(dfHist))
        if  BuySignal:
            print(symbol, "RSI MACD BUY")  
            insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"RSIMACD" , Gap=f"GAP:{str(HighHigh/LowLow)}")
            #BinTrade.NewOrderPlace(symbol, "Buy",price)

        if  SellSingal:
            print(symbol, "RSI MACD SELL")  
            insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"RSIMACD", Gap=f"GAP:{str(HighHigh/LowLow)}")
            #BinTrade.NewOrderPlace(symbol, "Sell",price)
         
        
        BuySignal=rsi/RSIMax>1 and RSIMax/RSIMin<1.2 and RSIMax<66 and RSIMin>52 #and price>MA200
        SellSingal=RSIMin/rsi>1 and RSIMax/RSIMin<1.2 and RSIMax<45 and RSIMin>33 #and price<MA200
        if  BuySignal:
            print(symbol, "RSI MA200 BUY")  
            insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"RSIMA200" , Gap=f"GAP:{str(HighHigh/LowLow)}")
            BinTrade.NewOrderPlace(symbol, "Buy",price)

        if  SellSingal:
            print(symbol, "RSI MA200 SELL")  
            insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"RSIMA200", Gap=f"GAP:{str(HighHigh/LowLow)}")
            BinTrade.NewOrderPlace(symbol, "Sell",price)

    
 
if __name__ == "__main__":
        
   
    # Parse the command-line argument
    print("test")
    arg = sys.argv[1]
    # Call the appropriate function based on the argument
    if arg == "I":
        getSignalsToTrade()
    elif arg == "U":
        UpdateDBTrades()
        

    else:
        print("Invalid function number.")         