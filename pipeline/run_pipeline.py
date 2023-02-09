from producer import *
from warehouse import *
import schedule


def main():
    test_producer = KafkaProducer(bootstrap_servers=config['kafka_broker'])
    # kafka_producer(producer)

    # schedule to send data every minute
    if datetime.datetime.now(timezone(TIME_ZONE)).time() > datetime.time(16, 0, 0) or datetime.datetime.now(
            timezone(TIME_ZONE)).time() < datetime.time(9, 30, 0):
        schedule.every(60).seconds.do(kafka_producer_single, test_producer, SYMBOL_LIST, tick=True, fake=True)
    else:
        schedule.every(60).seconds.do(kafka_producer_fake, test_producer, SYMBOL_LIST, tick=True, fake=False)
    schedule.every(3600).seconds.do(kafka_producer_news, test_producer)
    while True:
        schedule.run_pending()

    stock_database = CassandraStorage()
    stock_database.stream_to_cassandra()
    stock_database.tick_stream_to_cassandra()
    stock_database.historical_to_cassandra()


if __name__ == '__main__':
    main()
