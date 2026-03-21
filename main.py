from contextlib import contextmanager
from pipe21 import *
from yfinance import Tickers
import json
import os
import pika
import redis
import signal

worker = {
    "services": {
        "rabbit": {
            "host": os.getenv("RABBIT_HOST"),
            "port": os.getenv("RABBIT_PORT")
        },
        "redis": {
            "host": os.getenv("REDIS_HOST"),
            "port": os.getenv("REDIS_PORT")
        }
    },
    "state": {
        "isProcessingMessage": False,
        "isScheduledToDeletion": False
    }
}

database = redis.Redis(host=worker["services"]["redis"]["host"], port=worker["services"]["redis"]["port"], db=0)

def signal_handler(_signum, _frame):
    global worker
    state = worker["state"]
    state["isScheduledToDeletion"] = True

    print("[WARN] Received a signal to terminate, scheduling to deletion")

    if not state["isProcessingMessage"]:
        sys.exit()

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

@contextmanager
def signal_handler_context():
    global worker
    state = worker["state"]
    state["isProcessingMessage"] = True
    try:
        yield
    finally:
        state["isProcessingMessage"] = False
        if state["isScheduledToDeletion"]:
            print("[WARN] Scheduled to deletion, terminating...")
            sys.exit()

class StockMessageConsumer:
    def __init__(self, queueName):
        self.queueName = queueName
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=worker["services"]["rabbit"]["host"],
                port=worker["services"]["rabbit"]["port"],
                # NOTE: I'm disabling heartbeat entirely because its not
                # relevant to what i'm trying to achieve here
                heartbeat=0,
            ),
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queueName)

    def __enter__(self): return self
    def __exit__(self): self.stop()

    def set_message_handler(self, handler):
        self.channel.basic_consume(queue=self.queueName, on_message_callback=handler, auto_ack=True)

    def start(self): self.channel.start_consuming()

    def stop(self): self.channel.close()

def request_handler(channel, method, properties, body):
    with signal_handler_context():
        request = json.loads(body.decode())
        requestId = request["id"]

        database.set(requestId, json.dumps({ "id": requestId, "status": "processing", "result": None }))

        data = Tickers(" ".join(request["data"]))

        def get_ticker_info(ticker):
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

        result = request["data"] | Map(get_ticker_info) | Pipe(list)

        database.set(requestId, json.dumps({ "id": requestId, "status": "finished", "result": json.dumps(result) }))

consumer = StockMessageConsumer(queueName="stock-message-queue")
consumer.set_message_handler(request_handler)
consumer.start()
