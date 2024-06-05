"""
    This program listens for work messages continuously. 
    Start multiple versions to add more workers.  

    Author: Graham Miller
    Date: June 5, 2024

"""

import pika
import sys
import struct
from datetime import datetime
from collections import deque

# define deques
smoker_deque = deque(maxlen=5)
food_A_deque = deque(maxlen=20)
food_B_deque = deque(maxlen=20)

# define a callback function to be called when a message is received
def smoker_callback(ch, method, properties, body):
    timestamp, temp = struct.unpack('!df', body)
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    print(f"Received from smoker queue: {timestamp_str}, Temperature: {temp}F")
    smoker_deque.append(temp)

    if len(smoker_deque) == smoker_deque.maxlen and (smoker_deque[0] - temp) >= 15:
        print(f"Smoker Alert: Smoker Temperature has Fallen by 15°F in 2.5 minutes")

    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a callback function to be called when a message is received
def food_A_callback(ch, method, properties, body):
    timestamp, temp = struct.unpack('!df', body)
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    print(f"Received from food A queue: {timestamp_str}, Temperature: {temp}F")
    food_A_deque.append(temp)

    if len(food_A_deque) == food_A_deque.maxlen and (food_A_deque[0] - temp) <= 1:
        print(f"Food A Alert: Temperature change less than 1°F in the last 10 minutes")

    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a callback function to be called when a message is received
def food_B_callback(ch, method, properties, body):
    timestamp, temp = struct.unpack('!df', body)
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    print(f"Received from food B queue: {timestamp_str}, Temperature: {temp}F")
    food_B_deque.append(temp)

    if len(food_B_deque) == food_B_deque.maxlen and (food_B_deque[0] - temp) <= 1:
        print(f"Food B Alert: Temperature change less than 1°F in the last 10 minutes")

    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a main function to run the program
def main(hn: str = "localhost"):
    """ Continuously listen for task messages on a named queue."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        channel = connection.channel()

        # Ensure queues are declared
        queues = ["01-smoker", "02-food-A", "02-food-B"]
        for queue in queues:
            channel.queue_delete(queue=queue)
            channel.queue_declare(queue=queue, durable=True)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue="01-smoker", on_message_callback=smoker_callback, auto_ack=False)
        channel.basic_consume(queue="02-food-A", on_message_callback=food_A_callback, auto_ack=False)
        channel.basic_consume(queue="02-food-B", on_message_callback=food_B_callback, auto_ack=False)

        print(" [*] Ready for work. To exit press CTRL+C")
        channel.start_consuming()

    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

if __name__ == "__main__":
    main()