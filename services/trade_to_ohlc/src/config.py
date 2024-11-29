from pydantic_settings import BaseSettings

class AppConfig(BaseSettings):

    kafka_broker_address : str
    kafka_input_topic : str
    kafka_output_topic : str
    kafka_consumer_group : str

    class Config:
        env_file = ".env"

config = AppConfig()