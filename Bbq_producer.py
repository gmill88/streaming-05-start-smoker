"""
    This program sends a message to a queue on the RabbitMQ server.

    Author: Graham Miller
    Date: May 29, 2024

"""
# Imports
import pika
import sys
import webbrowser
import csv

# Configure Logging
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Offer to open RabbitMQ Admin Page
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logger.info("Opened RabbitMQ")



def connect_rabbitmq():
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()

        queues = ["01-smoker", "02-food-A", "02-food-B"]
        for queue_name in queues:
            ch.queue_delete(queue=queue_name)
            ch.queue_declare(queue=queue_name, durable=True)

        return conn, ch 
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)

def csv_processing():
    try:

        csv_path = "/Users/grahammiller/streaming-05-start-smoker/smoker-temps.csv"
        with open(csv_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for data_row in reader:
                timestamp = data_row['Time (UTC)']
                smoker_temp_str = data_row['Channel1']
                food_A_temp_str = data_row['Channel2']
                food_B_temp_str = data_row['Channel3']

                if smoker_temp_str:
                    smoker_temp = float(smoker_temp_str)
                    send_message("01-smoker", (timestamp, smoker_temp))
                if food_A_temp_str:
                    food_A_temp = float(food_A_temp_str)
                    send_message("02-food-A", (timestamp, food_A_temp))
                if food_B_temp_str:
                    food_B_temp = float(food_B_temp_str)
                    send_message("02-food-B", (timestamp, food_B_temp))
    except FileNotFoundError:
        logger.error("File not found.")
        sys.exit(1)
    except ValueError as e:
        print(f"Error processing file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        sys.exit(1)

def send_message(queue_name: str, message: tuple):

    try:
        conn, ch = connect_rabbitmq()
        ch.basic_publish(exchange="", routing_key=queue_name, body=str(message))
        logger.info(f"Sent message to {queue_name}: {message}")
    except Exception as e:
        logger.error(f"Error sending message to {queue_name}: {e}")  
    finally:
        conn.close()  


if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    csv_processing()
