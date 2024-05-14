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

def send_notification(*argmsg,timeInterval=0):
    config = read_config('APIKey.ini')
    bot_token = config.get('TGBotMsg', 'TOKEN')
    chat_id = config.get('TGBotMsg', 'CHAT_ID')
    isEnable = config.get('TGBotMsg', 'ENABLE')
    current_minute = datetime.datetime.now().minute
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
            data = client.futures_symbol_ticker()
            df=pd.DataFrame(data)
            for idx,row in df.iterrows():
                symbol=row["symbol"]
                current_price = float(row["price"])
                insert_or_update_trade_entry(symbol,current_price)
            time.sleep(5)   
        except Exception as e: 
            print("Not found",symbol,e) 
            time.sleep(5)   


def getSignalsToTrade():
    while 1==1:
        cnt=1
        data = client.futures_symbol_ticker()
        df=pd.DataFrame(data)
        for idx,row in df.iterrows():
            symbol=row["symbol"]
            current_price = float(row["price"])
            if  current_price<50000000:# and symbol in ["SOLUSDT","INJUSDT","LTCUSDT","ETHUSDT"]:
                try:
                    cnt=cnt+1
                    if cnt%50==0:
                        time.sleep(10)
                    retParam= getHistoryInfo(symbol,current_price)
                except BinanceAPIException as e: 
                        print("Not found",symbol,e) 

def read_table_to_dataframex(table_name, sqlquery=""):
    db_path = 'Backtest.db'
    conn = sqlite3.connect(db_path)
    query = f"  select 'Total', sum(PnL) TotalPnL from vwREsult  "
    if len(sqlquery)>4:
        df = pd.read_sql_query(sqlquery, conn) 
    else:     
        df = pd.read_sql_query(query, conn)
    print(df)    
    TotalPnL=df.iloc[-1]["TotalPnL"]
    if TotalPnL is not None:
        msg="Total vwMAResult :"+str(TotalPnL)
        send_notification(msg)
    conn.close()

    return df

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
                print(Symbol,Strategy,Gap)
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
                                    c.execute("INSERT INTO "+tabName+" (Symbol,Side, OpenPrice, SLPrice, TPPrice,SLRate,TPRate, Strategy,Gap ) VALUES (?, ?, ?, ?, ?, ? , ? ,?,? )",
                                    (Symbol,Side, Price, SLPrice, TPPrice,SLRate,TPRate, Strategy,Gap))
                                    if SLRate==.02 and TPRate==.045 and Strategy=="15mSuppResi2n":
                                       msg=f"{Strategy}\n{Symbol}\n{Side}\nGap {Gap}\nSL:{str(SLRate)}TP:{str(TPRate)}"
                                       print(msg)
                                       send_notification(msg)
    conn.commit()
    conn.close()
      

def UpdateSLCloseDeal(Symbol, Price,c):
   
        tabName="TradeEntry"
        current_minute = datetime.now().minute  # Assuming you've imported datetime module
        nTime=50
        if current_minute % nTime == 0 and  datetime.now().second%15==0 :
            read_table_to_dataframe() 
            
        strPrice=str(Price) 
        updatequery=f" UPDATE {tabName} SET  SLPrice = CASE WHEN side = 'Buy' and SLPrice<{strPrice} * (1 - SLRate) THEN {strPrice} * (1 - SLRate)"
        updatequery+= f"                                    WHEN side = 'Sell' and SLPrice>{strPrice} * (1 + SLRate) THEN {strPrice} * (1 + SLRate) ELSE SLPrice END ,"
        updatequery+= f" ClosePrice = CASE  WHEN side = 'Buy'  AND (SLPrice > {strPrice} OR TPPrice < {strPrice}) THEN {strPrice}"
        updatequery+= f"                    WHEN side = 'Sell' AND (SLPrice < {strPrice} OR TPPrice > {strPrice}) THEN {strPrice} "
        updatequery+= f"                    else 0 END WHERE ClosePrice = 0 AND Symbol = '{Symbol}'"
        #print(updatequery)
        c.execute( updatequery)

        
 


# Define function to insert or update data
def insert_or_update_entry(symbol, price,sl=0,tp=0,side=""):
    tabName="SymEntry"
    insert_or_update_trade_entry(tabName,symbol, price,sl,tp,side)


# Define function to insert or update data
def insert_or_update_entry_prevday(symbol, price,sl=0,tp=0,side=""):
    tabName="PreDayEntry"
    insert_or_update_trade_entry(tabName,symbol, price,sl,tp,side)
# Define function to insert or update data
def insert_or_update_entry_ma(symbol, price,sl=0,tp=0,side=""):
    tabName="MAEntry"
    insert_or_update_trade_entry(tabName,symbol, price,sl,tp,side)
    


def get_historical_prices(symbol, interval='15m', MinOrDayAgo='3 days',  limit=20):
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

        df['Open'] = pd.to_numeric(df['OpenPrice'])
        df['High'] = pd.to_numeric(df['HighPrice'])
        df['Low'] = pd.to_numeric(df['LowPrice'])
        df['Close'] = pd.to_numeric(df['ClosePrice'])
        pd.set_option('display.float_format', '{:.2f}'.format)
        df['IsGreen'] = np.where(df['Open'] < df['Close'], 'G', 'R') 
        #df=BinanceDataEx(df)
        df["HighHigh"] = df['High'].max()
        df["LowLow"] = df['Low'].min()

        df["HHCurr"] = df['High'].rolling(window=200).max()
        df["LLCurr"] = df['Low'].rolling(window=200).min()

    

        df["MA200"] = df['Close'].rolling(window=200).mean()
        df["UpperMA"] = df['High'].rolling(window=200).mean()
        df["LowerMA"] = df['Low'].rolling(window=200).mean()
        df["MidMA"] = df['Close'].rolling(window=200).mean()
        df["MA50"] = df['Close'].rolling(window=50).mean()
        df["MA20"] = df['Close'].rolling(window=20).mean()
        df["MA5"] = df['Close'].rolling(window=5).mean()

        df["MAHH"] = df['MidMA'].max()
        df["MALL"] = df['MidMA'].min()

    
        df['HA_Open']  = (df['Open'].shift(1) + df['Close'].shift(1)) / 2
        df['HA_Close']  = (df['Open'] + df['Low'] + df['Close'] + df['High']) / 4
        df['HA_High']  = df[['High', 'Open', 'Close']].max(axis=1)
        df['HA_Low']  = df[['Low', 'Open', 'Close']].min(axis=1)
        df['IsHAGreen'] = np.where(df['HA_Open'] < df['HA_Close'], 'G', 'R') 
    except Exception as e:
           print(e)
    return df
  
  

def getHistoryInfo(symbol,price):
        try: 
            if symbol.endswith("USDT")==False  :
                return
            

            #price=currPrice(symbol)
            
            dfHist=get_historical_prices(symbol,interval='15m',MinOrDayAgo="10 days", limit=250)
            dfHist["MA9"] = dfHist['Close'].rolling(window=9).mean()
            last_row=dfHist.iloc[-2]
            curr_row=dfHist.iloc[-1]
            nthLastRow=dfHist.iloc[-20]
            MAClose=curr_row["MA50"]
            HH=last_row['HHCurr']
            LL=last_row['LLCurr']
            HH=last_row['HHCurr']
            LL=last_row['LLCurr']
            nthHH=nthLastRow['HHCurr']
            nthLL=nthLastRow['LLCurr']
            
            for n in range(50, 4, -1):
                Gap=(1+n/100)
                if MAClose/last_row["Low"]>Gap and curr_row["MA20"]<price :# and price>dfHist.iloc[-2]["High"] 
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mPriceGAP" , Gap=f"GAP:{str(Gap)}")
                if last_row["High"]/MAClose>Gap and curr_row["MA20"]>price :
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mPriceGAP", Gap=f"GAP:{str(Gap)}")

                if HH/LL>Gap and  price/LL>1 and  price/LL<1.005 and curr_row["MA5"]>curr_row["MA9"] and curr_row["IsGreen"]=="G":
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mSuppResiGap", Gap=f"GAP:{str(Gap)}") 
                if HH/LL>Gap and HH/price>1  and HH/price<1.005  and curr_row["MA5"]<curr_row["MA9"] and curr_row["IsGreen"]=="R":
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mSuppResiGap", Gap=f"GAP:{str(Gap)}") 

                if nthHH/nthLL>Gap and  price/nthLL>1 and  price/nthLL<1.01  and curr_row["MA5"]>curr_row["MA9"] and curr_row["IsGreen"]=="G":
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mSuppResiGap20C", Gap=f"GAP:{str(Gap)}") 
                if nthHH/nthLL>Gap and nthHH/price>1  and nthHH/price<1.01   and curr_row["MA5"]<curr_row["MA9"] and curr_row["IsGreen"]=="R":
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mSuppResiGap20C", Gap=f"GAP:{str(Gap)}") 

                if HH/LL>Gap and  price/LL>1.01 and  price/LL<1.015 and curr_row["MA5"]>curr_row["MA9"] and  curr_row["MA9"]<price : 
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mSuppResi2n", Gap=f"GAP:{str(Gap)}") 
                if HH/LL>Gap and HH/price>1.01 and HH/price<1.02  and curr_row["MA5"]<curr_row["MA9"] and curr_row["MA9"]>price: 
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mSuppResi2n", Gap=f"GAP:{str(Gap)}") 

                if nthHH/nthLL>Gap and  price/nthLL>1.005 and  price/nthLL<1.01 and curr_row["MA5"]>curr_row["MA9"] and curr_row["IsGreen"]=="G":
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mStrategy5", Gap=f"GAP:{str(Gap)}") 
                if nthHH/nthLL>Gap and nthHH/price>1.005  and nthHH/price<1.01  and curr_row["MA5"]<curr_row["MA9"] and curr_row["IsGreen"]=="R":
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mStrategy5", Gap=f"GAP:{str(Gap)}") 


            if HH/LL>1.035:
                consecutive = 0
                for n in range(5,50):  # Iterate over window sizes from 15 to 5
                    nrow = dfHist.iloc[-n]
                    if nrow["Low"] > nrow["MA20"] and  curr_row["MA5"]<curr_row["MA9"] :
                        consecutive += 1
                    else:
                        if  consecutive >= 15 :
                            insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"STG1",Gap=f"{str(consecutive)}") 
                            consecutive=0
                            break
                        consecutive = 0  # Reset counter if MA20 crosses above the threshold

                consecutive = 0
                for n in range(5,50): 
                    nrow = dfHist.iloc[-n]
                    if nrow["High"] < nrow["MA20"] and curr_row["MA5"]>curr_row["MA9"]:  
                        consecutive += 1
                    else:
                        if  consecutive >= 15  :
                            insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"STG1",Gap=f"{str(consecutive)}")    
                            consecutive=0
                            break
                        consecutive = 0  # Reset counter if MA20 crosses above the threshold


            dfHist=get_historical_prices(symbol,interval='1h',MinOrDayAgo="10 days", limit=250)
            dfHist["MA9"] = dfHist['Close'].rolling(window=9).mean()
            last_row=dfHist.iloc[-2]
            curr_row=dfHist.iloc[-1]
            MAClose=curr_row["MA20"]
            HH=last_row['HHCurr']
            LL=last_row['LLCurr']
            nthLastRow=dfHist.iloc[-20]
            nthHH=nthLastRow['HHCurr']
            nthLL=nthLastRow['LLCurr']
            
    
            for n in range(100, 7, -1):
                Gap=(1+n/100)
                if MAClose/last_row["Low"]>Gap and curr_row["MA5"]<price : 
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1hPriceGAP", Gap=f"GAP:{str(Gap)}")
                if last_row["High"]/MAClose>Gap and curr_row["MA5"]>price :
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1hPriceGAP", Gap=f"GAP:{str(Gap)}")

                if HH/LL>Gap and  price/LL>1.01 and  price/LL<1.013 and curr_row["MA5"]>curr_row["MA9"] and curr_row["MA5"]<price : 
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1hSuppResi", Gap=f"GAP:{str(Gap)}") 
                if HH/LL>Gap and HH/price>1.01 and HH/price<1.013  and curr_row["MA5"]<curr_row["MA9"] and curr_row["MA5"]>price: 
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1hSuppResi", Gap=f"GAP:{str(Gap)}") 

                if nthHH/nthLL>Gap and  price/nthLL>1.01 and  price/nthLL<1.013 and curr_row["MA5"]>curr_row["MA9"] and curr_row["IsGreen"]=="G":
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1hSuppResiGap20C", Gap=f"GAP:{str(Gap)}") 
                if nthHH/nthLL>Gap and nthHH/price>1.01  and nthHH/price<1.013  and curr_row["MA5"]<curr_row["MA9"] and curr_row["IsGreen"]=="R":
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1hSuppResiGap20C", Gap=f"GAP:{str(Gap)}") 
                
                
                if nthHH/nthLL>Gap and price/nthHH>1.01  and price/nthHH<1.013  and curr_row["MA5"]>curr_row["MA9"] and curr_row["IsGreen"]=="G":
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1hSRBreakOut20C", Gap=f"GAP:{str(Gap)}") 
                if nthHH/nthLL>Gap and  nthLL/price>1.01 and  nthLL/price<1.013 and curr_row["MA5"]<curr_row["MA9"] and curr_row["IsGreen"]=="R":
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1hSRBreakOut20C", Gap=f"GAP:{str(Gap)}")     


                if HH/LL>Gap and  price/LL>1.01 and  price/LL<1.02 and curr_row["MA5"]>curr_row["MA9"] and  curr_row["MA9"]<price : 
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1hSuppResi2n", Gap=f"GAP:{str(Gap)}") 
                if HH/LL>Gap and HH/price>1.01 and HH/price<1.02  and curr_row["MA5"]<curr_row["MA9"] and curr_row["MA9"]>price: 
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1hSuppResi2n", Gap=f"GAP:{str(Gap)}") 


            if HH/LL>1.04:
                consecutive = 0
                for n in range(5,50):  # Iterate over window sizes from 15 to 5
                    nrow = dfHist.iloc[-n]
                    if nrow["Low"] > nrow["MA20"] and  curr_row["MA5"]<curr_row["MA9"] :
                        consecutive += 1
                    else:
                        if  consecutive >= 15 :
                            insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"STG2",Gap=f"{str(consecutive)}") 
                            consecutive=0
                            break
                        consecutive = 0  # Reset counter if MA20 crosses above the threshold

                consecutive = 0
                for n in range(5,50): 
                    nrow = dfHist.iloc[-n]
                    if nrow["High"] < nrow["MA20"] and curr_row["MA5"]>curr_row["MA9"]:  
                        consecutive += 1
                    else:
                        if  consecutive >= 15  :
                            insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"STG2",Gap=f"{str(consecutive)}")    
                            consecutive=0
                            break
                        consecutive = 0  # Reset counter if MA20 crosses above the threshold
        except Exception as e:
                print(e)


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