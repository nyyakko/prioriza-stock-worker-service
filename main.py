from pipe21 import *
from yfinance import Tickers
import json
import os
import pika
import redis

RABBIT_HOST = os.getenv("RABBIT_HOST")
RABBIT_PORT = os.getenv("RABBIT_PORT")
REDIS_HOST  = os.getenv("REDIS_HOST")
REDIS_PORT  = os.getenv("REDIS_PORT")

database = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

def request_handler(channel, method, properties, body):
    request = json.loads(body.decode())

    requestId = request["id"]

    database.set(requestId, json.dumps({ "id": requestId, "status": "processing", "result": None }))
    data = Tickers(" ".join(request["data"]))

    def getTickerInfoFn(ticker):
        closingPrice = data.tickers[ticker].history(period="1d")["Close"]
        if not closingPrice.empty:
            return {
                "ticker": ticker,
                "price": closingPrice.iat[0],
                "sector": {
                    "sector": data.tickers[ticker].info.get("sectorKey"),
                    "industry": data.tickers[ticker].info.get("industryKey")
                }
            }
        else:
            return {
                "error": f"ticker '{ticker}' not found"
            }

    result = request["data"] | Map(getTickerInfoFn) | Pipe(list)

    database.set(requestId, json.dumps({ "id": requestId, "status": "finished", "result": json.dumps(result) }))

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        # NOTE: I'm disabling heartbeat entirely because its not
        # relevant to what i'm trying to achieve here
        heartbeat=0,
    ),
)
channel = connection.channel()

channel.queue_declare(queue="stock-message-queue")
channel.basic_consume(queue="stock-message-queue", on_message_callback=request_handler, auto_ack=True)

channel.start_consuming()
