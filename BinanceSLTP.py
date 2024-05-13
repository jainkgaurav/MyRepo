import BinanceTrading as BT
import time
#BT.createTPSL()
#BT.getSupportResistance()
while 1==1:
    try:
        #BT.createTPSL()
        BT.getSupportResistance()
        time.sleep(300)
    except Exception as e:
        #print(f'{symbol} Exception Call Gemeni Singal File : {e}')
        print("Error",e)
        time.sleep(300)
        t=1
