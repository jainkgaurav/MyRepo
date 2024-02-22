from GeminiTrading import RequestType,read_config,getScriptPrice,getMidPrice
import ssl
import websocket
import json
import base64
import hmac
import hashlib
import time

# Call function1 directly
def testFun():
    test="ga"
    print(test)


def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def MarketData():
    ws = websocket.WebSocketApp(
        "wss://api.gemini.com/v1/marketdata/BTCUSD",
        on_message=on_message)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

def OrderEvents():
    payload = {"request": "/v1/order/events","nonce": time.time()}
    encoded_payload = json.dumps(payload).encode()
    b64 = base64.b64encode(encoded_payload)
    signature = hmac.new(gemini_api_secret, b64, hashlib.sha384).hexdigest()


    ws = websocket.WebSocketApp("wss://api.gemini.com/v1/order/events",
                                on_message=on_message,
                                header={
                                    'X-GEMINI-PAYLOAD': b64.decode(),
                                    'X-GEMINI-APIKEY': gemini_api_key,
                                    'X-GEMINI-SIGNATURE': signature
                                })
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

config = read_config()
gemini_api_key = config.get('GeminiAPI', 'gemini_api_key')
gemini_api_secret = config.get('GeminiAPI', 'gemini_api_secret').encode()
df=getScriptPrice('ethusd')
print(df)
d=getMidPrice('ethusd')*1.0005
print(getMidPrice('ethusd')*1.0005,getMidPrice('ethusd')*(1-.0005))

