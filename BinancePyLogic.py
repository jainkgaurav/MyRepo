from GeminiTrading import RequestType,read_config,getMidPrice,GetMAVal,getOHLCData,getCurrentCandle,send_notification,RequestType,HACandleLogic
import requests
import pandas as pd
from binance.client import Client
from datetime import datetime, timedelta
import ccxt as cx
import time
import numpy as np
from bs4 import BeautifulSoup
import pandas as pd
from ta.volatility import BollingerBands
import yfinance as yf
from ta.momentum import RSIIndicator

def HACandleRSILogic(df):
    print("HA Candle")
    
    df['HA_Open']  = (df['Open'].shift(1) + df['Close'].shift(1)) / 2
    df['HA_Close']  = (df['Open'] + df['Low'] + df['Close'] + df['High']) / 4
    df['HA_High']  = df[['High', 'Open', 'Close']].max(axis=1)
    df['HA_Low']  = df[['Low', 'Open', 'Close']].min(axis=1)
    df['IsGreen'] = np.where(df['HA_Open'] < df['HA_Close'], 'G', 'R') 
    df["RSI"]=RSIIndicator(close=df['Close'],window=22).rsi()
    df["HA_RSI"]=RSIIndicator(close=df['HA_Close'],window=22).rsi()

    return df
 


def BinanceData(symbol,price):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}USDT&interval=15m"
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    df = pd.DataFrame(data)
    column_mapping = {0: 'Date', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume',6: 'aa',7: 'bb',8: 'cc',9: 'dd',10: 'ee',11: 'ff'}
    df = df.rename(columns=column_mapping)
    df['DateTime'] = pd.to_datetime(df['Date'], unit='ms')
    df = df.sort_values(by='Date', ascending=True)  # Sort by timestamp in descending order
    df.set_index('Date', inplace=True)
    pd.set_option('display.float_format', '{:.2f}'.format)
    
    df['HighRange'] = df['High'].rolling(window=250).mean()
    df['LowRange'] = df['Low'].rolling(window=250).mean()
    df["MAHLRange"]=(df['HighRange']-df['LowRange'])
    df["MASLRatio"]=(df['HighRange']-df['LowRange'])/df['LowRange']
    print(df.iloc[-1])
    dfHA=HACandleLogic(df)
    print(dfHA.iloc[-1])
    

    time.sleep(3)
 

def get_all_trading_pairs():
    
        url = "https://api.coingecko.com/api/v3/exchanges/binance"
        response = requests.get(url)
        if response.status_code == 200:
            exchange_info = response.json()
            #print(exchange_info)
            trading_pairs =  [(ticker['base'], ticker['last']) for ticker in exchange_info['tickers']]
            # Print cryptocurrency names and market prices
            for base, last in trading_pairs:
                try:
                    listx=['BTC','ETH','INJ']
                    if base in listx:
                        print(f"Name: {base}, Market Price: {last}")    
                        BinanceData(base,last)             
                except Exception as e:
                    print(f"FAILED :{e} Name: {base}, Market Price: {last}")    
            return trading_pairs
        else:
            print("Failed to fetch trading pairs from CoinGecko")
          
get_all_trading_pairs()    
            


            return
            for n in range(50, 15 , -1):
                for m in range(25, 8, -1):    
                    NGap=(1+n/100)
                    MGap=(1+m/100)
                   
                    if n>m and MA50/price>NGap and  MA10/price>MGap and MA10>MA50 and price/LL10>1.008  and price/LL10<1.012  :
                        insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1HrMA20MA5Gap1Perc" , Gap=f"GAP:{str(MGap)}:{str(NGap)}")
                    if n>m and price/MA50>NGap and  price/MA10>MGap and MA10<MA50 and HH10/price>1.008  and HH10/price<1.012  :
                        insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1HrMA20MA5Gap1Perc", Gap=f"GAP:{str(MGap)}:{str(NGap)}")

            return
            consecutive=0
            for n in range(3,len(dfHist)-1):
                nrow = dfHist.iloc[-n]
                if  CurrLow /LowLow >1 and CurrLow /LowLow <1.005  :
                    consecutive += 1
            if HighHigh/LowLow>1.15:
                if (consecutive>1 and consecutive <5) :
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"MaxHighLowGAP",Gap=f"{str(consecutive)}") 
                if consecutive>6:
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"MaxHighLowGAP",Gap=f"{str(consecutive)}") 

            
            consecutive=0
            for n in range(3,len(dfHist)-1):
                nrow = dfHist.iloc[-n]
                if  HighHigh/CurrHigh >1 and  HighHigh/CurrHigh <1.005  :
                    consecutive += 1
            if HighHigh/LowLow>1.15:
                if (consecutive>1 and consecutive <5) :
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"MaxHighLowGAP",Gap=f"{str(consecutive)}") 
                if consecutive>6:
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"MaxHighLowGAP",Gap=f"{str(consecutive)}") 

            

            
            consecutive = 0
            for n in range(10,300):  # Iterate over window sizes from 15 to 5
                nrow = dfHist.iloc[-n]
                if nrow["Low"] > MA50 and  MA50/price>1 and MA50/price<1.015:
                    consecutive += 1
                else:
                    if  consecutive >= 50 :
                        insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1hrConsecutive",Gap=f"{str(consecutive)}") 
                        consecutive=0
                        break
                    consecutive = 0  # Reset counter if MA20 crosses above the threshold
                    break

            consecutive = 0
            for n in range(10,300): 
                nrow = dfHist.iloc[-n]
                if nrow["High"] < MA50 and   price/MA50>1 and price/MA50<1.015:  
                    consecutive += 1
                else:
                    if  consecutive >= 50  :
                        insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1hrConsecutive",Gap=f"{str(consecutive)}")    
                        consecutive=0
                        break
                    consecutive = 0  # Reset counter if MA20 crosses above the threshold
                    break


            '''
              for n in range(100, 25, -1):
                Gap=(1+n/100)
                if HH200/CurrLow>Gap and Signal=="RRG" and price>MA5   :
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1HrPriceGAP" , Gap=f"GAP:{str(Gap)}")
                if CurrHigh/LL200>Gap and Signal=="GRR" and price<MA5  :
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1HrPriceGAP", Gap=f"GAP:{str(Gap)}")
                
            for n in range(100, 9, -1):
                Gap=(1+n/100)    
                if MA200/curr_row["MA9"]>Gap and rsi<50 and price>curr_row["MA9"] and curr_row["MA20"]<curr_row["MA9"]   :
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1HrRRGRsi" , Gap=f"GAP:{str(Gap)}")
                if curr_row["MA9"]/MA200>Gap  and rsi>50 and price<curr_row["MA9"] and curr_row["MA20"]>curr_row["MA9"]  :
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1HrRRGRsi", Gap=f"GAP:{str(Gap)}")
                  
            
            if MA200/LL200>1 and MA200/LL200<1.05 and price/MA200>1.01 and price/MA200<1.03  and rsi<30 :
                insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1HrMA200RSI" , Gap=f"GAP:{str(Gap)}")
            if  HH200/MA200>1 and HH200/MA200<1.05 and MA200/price>1.01 and MA200/price<1.03  and rsi>70 :
                insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1HrMA200RSI", Gap=f"GAP:{str(Gap)}")

            if  MA200/LL200>1 and MA200/LL200<1.05 and curr_row2["High"]>MA200 and curr_row2["Low"]<MA200 and rsi>60 and price>curr_row2["Close"] :
                insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1HrMA200Rsi4060" , Gap=f"GAP:{str(Gap)}")
            if  HH200/MA200>1 and HH200/MA200<1.05  and curr_row2["High"]>MA200 and curr_row2["Low"]<MA200 and rsi<40 and price<curr_row2["Close"] :
                insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1HrMA200Rsi4060", Gap=f"GAP:{str(Gap)}")
            '''
 
            for n in range(50, 10, -1):
                Gap=(1+n/100)
                if n>10:
                    if MAClose/last_row["Low"]>Gap and curr_row["MA200"]<price :# and price>dfHist.iloc[-2]["High"] 
                        insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mPriceGAP" , Gap=f"GAP:{str(Gap)}")
                        print(symbol)
                    if last_row["High"]/MAClose>Gap and curr_row["MA200"]>price :
                        insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mPriceGAP", Gap=f"GAP:{str(Gap)}")
                        print(symbol)

                if HH/LL>Gap and  price/LL>1.00 and  price/LL<1.01 and last_row["High"]<price : 
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mSuppResi2n", Gap=f"GAP:{str(Gap)}") 
                if HH/LL>Gap and HH/price>1.0 and HH/price<1.01  and last_row["Low"]>price : 
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mSuppResi2n", Gap=f"GAP:{str(Gap)}") 

            
            return           
 
            if nthHH/nthLL>Gap and  price/nthLL>1 and  price/nthLL<1.005  and curr_row["MA5"]>curr_row["MA9"] and curr_row["IsGreen"]=="G":
                insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mSuppResiGap20C", Gap=f"GAP:{str(Gap)}") 
            if nthHH/nthLL>Gap and nthHH/price>1  and nthHH/price<1.005   and curr_row["MA5"]<curr_row["MA9"] and curr_row["IsGreen"]=="R":
                insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mSuppResiGap20C", Gap=f"GAP:{str(Gap)}") 

            if nthHH/nthLL>Gap and nthLL<LL50 and price/LL>1 and  price/LL<1.005 and curr_row["MA5"]>curr_row["MA9"] and  curr_row["MA9"]<price : 
                insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mSuppResiNearFar", Gap=f"GAP:{str(Gap)}") 
            if nthHH/nthLL>Gap and nthHH>HH50 and HH/price>1 and HH/price<1.005  and curr_row["MA5"]<curr_row["MA9"] and curr_row["MA9"]>price: 
                insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mSuppResiNearFar", Gap=f"GAP:{str(Gap)}") 

            if nthHH/nthLL>Gap and  price/nthLL>1.0 and  price/nthLL<1.005 and curr_row["MA5"]>curr_row["MA9"] and curr_row["IsGreen"]=="G":
                insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"15mStrategy5", Gap=f"GAP:{str(Gap)}") 
            if nthHH/nthLL>Gap and nthHH/price>1  and nthHH/price<1.005  and curr_row["MA5"]<curr_row["MA9"] and curr_row["IsGreen"]=="R":
                insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"15mStrategy5", Gap=f"GAP:{str(Gap)}") 

            return
            

            dfHist=get_historical_prices(symbol,interval='1h',MinOrDayAgo="10 days", limit=250)
            dfHist["MA9"] = dfHist['Close'].rolling(window=9).mean()
            last_row=dfHist.iloc[-2]
            curr_row=dfHist.iloc[-1]
            MAClose=curr_row["MA20"]
            HH=last_row['HHCurr']
            LL=last_row['LLCurr']
            HH50=last_row["HH50"]
            LL50=last_row["LL50"]
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

                if nthHH/nthLL>Gap and nthLL<LL50 and price/nthLL>1.01 and  price/nthLL<1.013  and curr_row["MA5"]>curr_row["MA9"] and curr_row["IsGreen"]=="G":
                    insert_or_update_trade_entry( symbol, price,Side="Buy",Strategy=f"1hSRBreakNearFar", Gap=f"GAP:{str(Gap)}") 
                if nthHH/nthLL>Gap and nthHH>HH50 and nthHH/price>1.01  and nthHH/price<1.013  and curr_row["MA5"]<curr_row["MA9"] and curr_row["IsGreen"]=="R":
                    insert_or_update_trade_entry( symbol, price,Side="Sell",Strategy=f"1hSRBreakNearFar", Gap=f"GAP:{str(Gap)}")     


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
       
