import json
from websocket import WebSocketApp
from loguru import logger
from pydantic import BaseModel
from typing import List
import ssl
import threading

class Trade(BaseModel):
    product_id: str
    quantity: float
    price: float
    timestamp_ms: int

class KrakenWebsocketAPI:
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str):
        self.product_id = product_id
        self.trades = []
        self.ws = WebSocketApp(
            self.URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self._connect()

    def _connect(self):
        # Launch WebSocket in a separate thread to avoid blocking
        self.ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={'sslopt': {"cert_reqs": ssl.CERT_NONE}})
        self.ws_thread.daemon = True
        self.ws_thread.start()

    def on_open(self, ws):
        logger.info(f"WebSocket connection opened, subscribing to {self.product_id}")
        self._subscribe(self.product_id)

    def on_message(self, ws, message):
        logger.debug(f"Received message: {message}")
        message = json.loads(message)

        if message.get("channel") == "trade":
            for trade_data in message["data"]:
                trade = Trade(
                    product_id=self.product_id,
                    price=trade_data["price"],
                    quantity=trade_data["qty"],
                    timestamp_ms=self.to_ms(trade_data["timestamp"])
                )
                self.trades.append(trade)
                
        else:
            logger.debug("No trade data in this message.")

    def get_trades(self) -> List[Trade]:
        trades_copy = self.trades[:]
        self.trades.clear()
        return trades_copy

    def _subscribe(self, product_id: str):
        subscription_message = {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [product_id],  # Wrap product_id in a list
                "snapshot": False
            },
        }
        self.ws.send(json.dumps(subscription_message))
        logger.info("Subscription message sent.")


    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.info("WebSocket connection closed.")

    @staticmethod
    def to_ms(timestamp: str) -> int:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
