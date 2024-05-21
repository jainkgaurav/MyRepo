import time
from binance.client import Client
from binance.exceptions import BinanceAPIException
from binance.enums import *
import pandas as pd
import BinanceFuct as BF
from decimal import Decimal
import UtilFunctions as UF
import pandas as pd
import time
import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import ADXIndicator, MACD,PSARIndicator
import UtilFunctions as UF
from time import strftime, localtime
from datetime import datetime, timedelta
import warnings
import BackTest as BT
warnings.simplefilter(action='ignore', category=FutureWarning)
import sys
import sqlite3

param= UF.read_config("APIKey.ini")
client = Client(api_key=param.get("BNBKey","APIKey"),api_secret=param.get("BNBKey","SecKey"), tld='com', testnet=False)
 
def get_account_balance():
    balance = client.futures_account_balance()
    #print(balance)
    balance = client.futures_account_balance()[6]['balance']
    return float(balance)
 
def get_open_positions(symbol):
    account_info = client.futures_account()['positions']
    df=pd.DataFrame(account_info)
    try:
        if len(df)>0:
            df['Amount'] = pd.to_numeric(df['positionAmt'])
            dfAmt=df[(df["Amount"]!=0)]
            if len(dfAmt)>0:
                dfSym=dfAmt[(dfAmt["symbol"]==symbol)]
                if(len(dfSym)>0):
                    return dfSym.iloc[-1]
    except:
        erro=1

    return None

def createDataTable(df,tableName):
    conn = sqlite3.connect('BinanceTrade.db')
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {tableName}")
    df.to_sql(tableName, conn, index=False)
    conn.commit()
    cursor.close()

def DropTableIfNoData(tableName):
    conn = sqlite3.connect('BinanceTrade.db')
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {tableName}")
    conn.commit()
    cursor.close()

def DeleteTableIfNoData(tableName):
    conn = sqlite3.connect('BinanceTrade.db')
    cursor = conn.cursor()
    cursor.execute(f"Delete From  {tableName}")
    conn.commit()
    cursor.close()

def readDBtoDf(tableName):
    conn = sqlite3.connect('BinanceTrade.db')
    cursor = conn.cursor()
    df = pd.read_sql(f'SELECT * FROM {tableName}', conn)
    cursor.close()
    return df

def CheckIfTableExists(tableName):
    isExist=0
    conn = sqlite3.connect('BinanceTrade.db')
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}'")
    table_exists = cursor.fetchone()
    if not table_exists:
        isExist=0
    else:
        isExist=1
    cursor.close()
    return isExist
 
def get_all_open_positions():
    tabelName="AccountInfo"
    account_info = client.futures_account()['positions']
    try:
        dfAccInfo=pd.DataFrame(account_info)
        dfAccInfo['Amount'] = pd.to_numeric(dfAccInfo['positionAmt'])
        df=dfAccInfo[(dfAccInfo["Amount"]!=0)]
        if len(df)==0:
           if CheckIfTableExists(tabelName):
              DeleteTableIfNoData(tabelName)
           return 0  
        else:
           if len(df)>0:
            createDataTable(df,tabelName)
            df=readDBtoDf(tabelName)
            return len(df)
    except Exception as e:
        print("ERRRRRR",e)
        return -1
    return 0

def get_all_open_positions_tp():
    retval=0
    account_info = client.futures_account()['positions']
    try:
        dfAccInfo=pd.DataFrame(account_info)
        dfAccInfo['Amount'] = pd.to_numeric(dfAccInfo['positionAmt'])
        df=None
        b=0
        s=0
        dfBuy=dfAccInfo[(dfAccInfo["Amount"]>0)]
        if len(dfBuy)>0:
            b=1
        dfSell=dfAccInfo[(dfAccInfo["Amount"]<0)]
        if len(dfSell)>0:
            s=1
        df=dfAccInfo[(dfAccInfo["Amount"]!=0)]
          
        if b==1 and s==1:
            print(df)
            return 3
        elif b==1 and s==0:
           return 1
        elif b==0 and s==1:
            
            return 2 
        else:
            return 0
    except:
        erro=1
        return 0
    return 0



def createTPSL():
    
    account_info = client.futures_account()
    positions = [position for position in account_info['positions'] if float(position['positionAmt']) != 0]
    for position in positions:
        symbol = position['symbol']
        
        position_amount = position['positionAmt']
        entry_price = float(position['entryPrice'])
        asset_details = next((item for item in client.futures_exchange_info()['symbols'] if item['symbol'] == symbol), None)
        asset_precision = asset_details['pricePrecision'] if asset_details else 2
        try:
            data = client.futures_symbol_ticker()
            PlaceTargetProft(symbol, position_amount, entry_price, asset_precision)
            PlaceOrUpdateSL(symbol, position_amount, entry_price, asset_precision,data)    
        except BinanceAPIException as e:
            if e.code == -2021:
                pass
            else:
                raise e
       
 
def PlaceOrUpdateSL(symbol, position_amount, entry_price, asset_precision,data):
    open_orders = client.futures_get_open_orders(symbol=symbol)
    sl_order_exists = any(order for order in open_orders if order['type'] == 'STOP_MARKET')
    SLRate=.01
    df=pd.DataFrame(data)
    row=df[df["symbol"]==symbol]
    currprice=float(row["price"])
    if not sl_order_exists:
        if float(position_amount)>0:
            stop_price = round(entry_price *(1-SLRate), asset_precision)
            print("stop_price",abs(float(position_amount)),stop_price)
            client.futures_create_order(symbol=symbol, side=client.SIDE_SELL, type='STOP_MARKET', quantity=abs(float(position_amount)), stopPrice=stop_price)
        if float(position_amount)<0:
            stop_price = round(entry_price  *(1+SLRate), asset_precision)
            print("stop_price",abs(float(position_amount)),stop_price)
            client.futures_create_order(symbol=symbol, side=client.SIDE_BUY, type='STOP_MARKET', quantity=abs(float(position_amount)), stopPrice=stop_price)    
    '''
    else:

        if float(position_amount)!=0 and 1==0:
            print("STOP_MARKET ",float(position_amount))
            for order in open_orders:
                if order['type'] == 'STOP_MARKET':
                    if float(position_amount)>0:
                        if abs(round(float(order['stopPrice']),asset_precision))<  round(currprice *(1-SLRate), asset_precision):
                            newPrice=round(currprice *(1-SLRate), asset_precision)
                            client.futures_cancel_order(symbol=order['symbol'], orderId=order['orderId'])
                            time.sleep(5)
                            client.futures_create_order(symbol=symbol, side=client.SIDE_SELL, type='STOP_MARKET', quantity=abs(float(position_amount)), stopPrice=newPrice,reduceOnly=True )
                            time.sleep(5)
                    else:
                        if  abs(round(float(order['stopPrice']),asset_precision))> round(currprice *(1+SLRate), asset_precision):
                            newPrice= round(currprice *(1+SLRate), asset_precision)
                            client.futures_cancel_order(symbol=order['symbol'], orderId=order['orderId'])
                            time.sleep(5)
                            client.futures_create_order(symbol=symbol, side=client.SIDE_BUY, type='STOP_MARKET', quantity=abs(float(position_amount)), stopPrice=newPrice,reduceOnly=True )
                            time.sleep(5)
 

   ''' 
def PlaceTargetProft(symbol, position_amount, entry_price, asset_precision):
    print("place_tp_sl_orders",symbol)
    open_orders = client.futures_get_open_orders(symbol=symbol)
    #print(open_orders)
    tp_order_exists = any(order for order in open_orders if order['type'] == 'TAKE_PROFIT_MARKET')
    print(tp_order_exists)
    TPRate=.015
    if not tp_order_exists:
        if float(position_amount)>0:
            profit_price = round(entry_price * (1+TPRate) , asset_precision)
            print("profit_price",abs(float(position_amount)),profit_price)
            client.futures_create_order(symbol=symbol, side=client.SIDE_SELL, type='TAKE_PROFIT_MARKET', quantity=abs(float(position_amount)),   stopPrice=profit_price)
        if float(position_amount)<0:
            profit_price = round(entry_price * (1-TPRate), asset_precision)
            print("profit_price",abs(float(position_amount)),profit_price)
            client.futures_create_order(symbol=symbol, side=client.SIDE_BUY, type='TAKE_PROFIT_MARKET', quantity=abs(float(position_amount)),   stopPrice=profit_price)    
  
def CloseAllOpenOrders():
    get_all_open_positions()
    open_orders = client.futures_get_open_orders()
    df=pd.DataFrame(open_orders)
    
    if len(open_orders) > 0:
        createDataTable(df,"OpenOrders")
        dfAccInfo=readDBtoDf("AccountInfo")
        for order in open_orders:
            if len(dfAccInfo)==0:
                cancel_result = client.futures_cancel_order(symbol=order['symbol'], orderId=order['orderId'])
                time.sleep(2)
            else:
                dfx=dfAccInfo[dfAccInfo["symbol"]==order['symbol']]    
                if len(dfx)==0:
                    cancel_result = client.futures_cancel_order(symbol=order['symbol'], orderId=order['orderId'])
                    time.sleep(2)
    else:
        DeleteTableIfNoData("OpenOrders")                

def CloseOpenOrders(symbol):
    open_orders = client.futures_get_open_orders(symbol=symbol)
    if len(open_orders) > 0:
        for order in open_orders:
            print("canceling")
            cancel_result = client.futures_cancel_order(symbol=order['symbol'], orderId=order['orderId'])
            time.sleep(1)


def getCurrPrice(data,df,symbol):
    df=pd.DataFrame(data)
    row=df[df["symbol"]==symbol]
    return float(row["price"])


def currPrice(symbol):
    data = client.futures_symbol_ticker()
    df=pd.DataFrame(data)
    row=df[df["symbol"]==symbol]
    return float(row["price"])
 
def createNewOrder(symbol,price,side):
    print("check to create oder ",symbol,price,side)
    if get_account_balance()>2:
       CloseOpenOrders(symbol)
       msg=f"Order Created {symbol} at {price} in {side}"
       if price<10000:
          qty=round(get_account_balance() * 3/price,3)  
       if price<1000:
          qty=round(get_account_balance() * 3/price,1)
       if price<100:
          qty=int(get_account_balance() * 3/price)
        
       totpos=get_all_open_positions()
       if totpos>=0 and totpos<4 :
          if side=="Buy":
              side="BUY"
          else:
              side="SELL"     
          client.futures_create_order(symbol=symbol , side=side,   type=client.FUTURE_ORDER_TYPE_MARKET, quantity=qty, isIsolated='TRUE')
        
    return 0

def NewOrderPlace(symbol, side,price):
    if side=="Buy" or side=="Sell":
        if get_open_positions(symbol) is None:
            print("Entry createNewOrder--",symbol,price,side)  
            createNewOrder(symbol,price,side)   
            time.sleep(10)
            createTPSL() 



def StartTrading():
    cnt=0
    data = client.futures_symbol_ticker()
    df=pd.DataFrame(data)
    for idx,row in df.iterrows():
        symbol=row["symbol"]
        current_price = float(row["price"])
        try:
            response = client.futures_change_leverage(symbol=symbol, leverage=10)
            cnt=cnt+1
            if cnt%50==0:
                time.sleep(10)
            lenRange=400
            ranges = BT.generate_ranges(0, lenRange, 100) 
            dfHist=BT.get_historical_prices(symbol,interval='15m',MinOrDayAgo="10 days", limit=lenRange)
            side=BT.PriceAtSupportResitance(ranges,symbol,dfHist,current_price,"Range 3time")
            NewOrderPlace(symbol, side,current_price)
        except BinanceAPIException as e: 
            print("Not found",symbol,e) 


def NewOrder():
    while 1==1:
        try:
            StartTrading()    
            print("Wating next minn")
            time.sleep(10)         
        except Exception as e:
            print("Error",e)
            time.sleep(60)

def CloseOrder():
    while 1==0:
        try:
            createTPSL()    
            time.sleep(10)         
        except Exception as e:
            print("Error",e)
            time.sleep(60)

def CallFunction():
    arg = sys.argv[1]
     
    if arg == "I":
        print("Staring New Order Checking")
        NewOrder()
    elif arg == "U":
        print("Staring CloseOrder Checking")
        CloseOrder("SL")
    else:
        print("Invalid function number.")   


def StartSolanaTrading():
    cnt=0
    data = client.futures_symbol_ticker()
    df=pd.DataFrame(data)
    Totpos=get_all_open_positions()==0
    for idx,row in df.iterrows():
        symbol=row["symbol"]
        price = float(row["price"])
        cnt=cnt+1
        if cnt%50==0:
           time.sleep(10)
        try:
            if 1==1 and symbol.endswith("USDT"): 
                if Totpos==0:    
                    dfHist=BT.get_historical_prices(symbol,interval='3m',MinOrDayAgo="3 days", limit=300)
                    if not dfHist.empty:
                        row=dfHist.iloc[-2]
                        HighHigh=row["HighHigh"]
                        LowLow=row["LowLow"]
                        if HighHigh/LowLow>1.06:
                            rowPos=get_open_positions(symbol)
                            print(rowPos)
                            if row["MA200"]<row["High"] and  row["MA200"]>row["Low"] and price<row["MA200"]:
                                if rowPos is None:
                                    response = client.futures_change_leverage(symbol=symbol, leverage=20)
                                    createNewOrder(symbol,price,"SELL")   
                                    time.sleep(5)
                            if row["MA200"]<row["High"] and  row["MA200"]>row["Low"] and price>row["MA200"]:
                                if rowPos is None:
                                    response = client.futures_change_leverage(symbol=symbol, leverage=20)
                                    createNewOrder(symbol,price,"BUY")   
                                    time.sleep(5)
                else:
                    #getAllOpenOrdersToCancel()
                    createTPSL()
                    time.sleep(3)
        except BinanceAPIException as e: 
            print("Not found",symbol,e) 


if __name__ == "__main__":
    print("StartSolanaTrading")
    while 1==1:
        try:
            current_minute = datetime.now().minute
            if  current_minute % 2== 0:
                StartSolanaTrading()
                time.sleep(40)
        except BinanceAPIException as e: 
            print("Not found",e) 
 
