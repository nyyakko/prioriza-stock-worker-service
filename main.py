import pika
import json
import redis
from pipe21 import *
from yfinance import Tickers
import os

database = redis.Redis(host=os.getenv("REDIS_HOST"), port=int(os.getenv("REDIS_PORT")), db=0)

def request_handler(channel, method, properties, body):
    request = json.loads(body.decode())

    requestId = request["id"]

    database.set(requestId, json.dumps({ "id": requestId, "status": "processing", "result": None }))
    data = Tickers(" ".join(request["data"]))

    def transformerFn(ticker):
        try:
            closingPrice = data.tickers[ticker].history(period="1d")["Close"].iat[0]
            return {
                "ticker": ticker,
                "price": closingPrice,
                "sector": {
                    "sector": data.tickers[ticker].info.get("sectorKey"),
                    "industry": data.tickers[ticker].info.get("industryKey")
                }
            }
        except RuntimeError as e:
            return {
                "ticker": ticker,
                "error": "ticker not found"
            }

    result = request["data"] | Map(transformerFn) | Pipe(list)

    database.set(requestId, json.dumps({ "id": requestId, "status": "finished", "result": json.dumps(result) }))

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=os.getenv("RABBIT_HOST"),
        port=int(os.getenv("RABBIT_PORT")),
        # NOTE: I'm disabling heartbeat entirely because its not
        # relevant to what i'm trying to achieve here
        heartbeat=0,
    ),
)
channel = connection.channel()

channel.queue_declare(queue="stock-message-queue")
channel.basic_consume(queue="stock-message-queue", on_message_callback=request_handler, auto_ack=True)

channel.start_consuming()
