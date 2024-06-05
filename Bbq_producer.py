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
import struct
import time

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

# Connect to RabbitMQ server
def connect_rabbitmq():
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()

        queues = ["01-smoker", "02-food-A", "02-food-B"]
        for queue_name in queues:
            # Declare the queue
            ch.queue_declare(queue=queue_name, durable=True)

        return conn, ch 
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)

# Process CSV and send message to RabbitMQ queues
def csv_processing():
    try:
        csv_path = "smoker-temps.csv"
        with open(csv_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for data_row in reader:
                timestamp_str = data_row['Time (UTC)']
                smoker_temp_str = data_row['Channel1']
                food_A_temp_str = data_row['Channel2']
                food_B_temp_str = data_row['Channel3']

                timestamp = time.mktime(time.strptime(timestamp_str, "%m/%d/%y %H:%M:%S"))

                # Checks if strings are empty
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
    """ 
    Send message to queue
    
    Parameters:
        message; The message sent to the queue
        queue_name: name of the queue
    """

    try:
        conn, ch = connect_rabbitmq()
        packed_message = struct.pack('!df', message[0], message[1])
        ch.basic_publish(exchange="", routing_key=queue_name, body=packed_message)
        logger.info(f"Sent message to {queue_name}: {message}")
    except Exception as e:
        logger.error(f"Error sending message to {queue_name}: {e}")  
    finally:
        # Close connection to the server
        conn.close()  

if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    csv_processing()