import functools
import random
import time

from kafka import KafkaConsumer


def backoff(tries, sleep):
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            for i in range(tries):
                result = function(*args, **kwargs)
                if not result:
                    print(f'Failed attempt_no {i} to backoff')
                    time.sleep(sleep)
            print('Backoff finished!')

        return wrapper

    return decorator


@backoff(tries=3, sleep=1)
def message_handler(message):
    rand_val = random.randint(0, 20)
    print(f'Rand Val: {rand_val}')
    if rand_val % 2 == 0:
        print(f'Handled message: {message}')
        return True
    return False


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer("alexbuyan_task3",
                             group_id="alexbuyan_group_task3",
                             bootstrap_servers='localhost:29092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    for message in consumer:
        # save to DB
        message_handler(message)
        print(message)


if __name__ == '__main__':
    create_consumer()
