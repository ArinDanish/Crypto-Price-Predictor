from typing import List
from quixstreams import Application
from src.kraken_websocket_api import (
    KrakenWebsocketAPI,
    Trade,
)
from loguru import logger



def produce_trades(
    kafka_broker_address: str,
    kafka_topic: str,
    product_id: str,
):
    """
    reads trdes from Kraken Websocket API and saves them in the given 'kafka topic'

    Args:
        kafka_borker_addresss (str) : The address of the kafka broker
        kafka_topic (str) : The Kafka topic to save the trades
        product_id (str): The product id to get trades from 

    Return:
        None
    
    
    """


    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic, value_serializer='json')

    kraken_api= KrakenWebsocketAPI(product_id=product_id)

    with app.get_producer() as producer:

        while True:
                
                trades: List[Trade] = kraken_api.get_trades()

                for trade in trades:
                                        
                        message = topic.serialize(key=trade.product_id, value=trade.model_dump())

                        producer.produce(topic=topic.name, value=message.value, key=message.key)
                        logger.debug(f"Pushed trade to Kafka: {trade}")

                        

if __name__=="__main__":

    from src.config import config

    produce_trades(
        kafka_broker_address=config.kafka_broker_address,
        kafka_topic=config.kafka_topic,
        product_id=config.product_id
    )
