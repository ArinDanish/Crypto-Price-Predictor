from quixstreams import Application
from datetime import datetime , timedelta

from loguru import logger
import sys
logger.remove()
logger.add(sys.stdout, level="DEBUG")


def init_ohlcv_candle(trade: dict):
    candle = {
        'open': trade['price'],
        'high': trade['price'],
        'low': trade['price'],
        'close': trade['price'],
        'volume': trade['quantity'], 
    }
    logger.debug(f"Initialized candle: {candle}")

    return candle


def update_ohlcv_candle(candle: dict, trade: dict):

    logger.debug(f"Updating candle with trade: {trade}")
    """
    Updates OHLCV candle with new trade.
    """
    candle['high'] = max(candle['high'], trade['price'])
    candle['low'] = min(candle['low'], trade['price'])
    candle['close'] = trade['price']
    candle['volume'] +=trade['quantity']

    logger.debug(f"Updated candle: {candle}")

    return candle

def transform_trade_to_ohlcv(
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_output_topic: str,
    kafka_consumer_group: str,
):
    """
    Reads incoming trades from kafka_input_topic , transform them in to OHLC data
    and outputs them to given kafka_output_topic

    Args:
        kafka_borker_address(str): the address of the kafka borker
        kafka_input_topic(str): the Kafka topic to read the trade from
        kafka_output_topic(str): the Kafka topic to save the OHLC data
        kafka_consumer_group(str): the Kafka consumer group

    Returns:
            None
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group,
        auto_offset_reset='earliest'
        
    )

    input_topic = app.topic(name=kafka_input_topic, value_deserializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    sdf = app.dataframe(input_topic)

    sdf.update(lambda trade: logger.debug(f"Received trade: {trade}"))

    

    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=60)).reduce(
        reducer=update_ohlcv_candle,
        initializer=init_ohlcv_candle).final
    
    logger.debug("Completed tumbling window setup for data aggregation.")

    # sdf = sdf.select(['open', 'high', 'low', 'close'])

    #  # unpacking the trades we want
    # sdf['open'] = sdf['trade']['open']
    # sdf['high'] = sdf['trade']['high']
    # sdf['low'] = sdf['trade']['low']
    # sdf['close'] = sdf['trade']['close']

    # Print the output to the console
    # sdf = sdf[[ 'open', 'high', 'low', 'close']]

    # sdf = sdf.to_topic(output_topic)

    logger.debug("Starting application to process trades.")

    app.run()


if __name__ == "__main__":

    from src.config import config

    transform_trade_to_ohlcv(
        kafka_broker_address=config.kafka_broker_address, 
        kafka_input_topic=config.kafka_input_topic,
        kafka_output_topic=config.kafka_output_topic,
        kafka_consumer_group=config.kafka_consumer_group,
    )