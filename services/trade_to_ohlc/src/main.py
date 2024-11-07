from quixstreams import Application
from datetime import datetime , timedelta
from loguru import logger

def init_ohlcv_candle(trade: dict):
    """
    Returns the OHLCV candle when the first trade in that window happens.
    """
    return {
        'open': trade['price'],
        'high': trade['price'],
        'low': trade['price'],
        'close': trade['price'],
        'volume': trade['quantity'], 
    }

def update_ohlcv_candle(candle: dict, trade: dict):
    """
    Updates OHLCV candle with new trade.
    """
    candle['high'] = max(candle['high'], trade['price'])
    candle['low'] = min(candle['low'], trade['price'])
    candle['close'] = trade['price']
    candle['volume'] =trade['quantity']

    return candle

def transform_trade_to_ohlc(
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
        consumer_group=kafka_consumer_group
    )

    input_topic = app.topic(name=kafka_input_topic, value_deserializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_deserializer='json')

    sdf = app.dataframe(input_topic)

    sdf.update(logger.debug)

    sdf = (
        sdf.tumbling_window(duration_ms=timedelta(seconds=1))
        .reduce(reducer=update_ohlcv_candle, initializer=init_ohlcv_candle )
        .final()
    )

    # Print the output to the console
    sdf.update(logger.debug)

    app.run(sdf)


if __name__ == "__main__":

    transform_trade_to_ohlc(
        kafka_broker_address='localhost:19092',
        kafka_input_topic='trade',
        kafka_output_topic='ohlcv',
        kafka_consumer_group='consumer_group_trade_to_ohlcv',
    )