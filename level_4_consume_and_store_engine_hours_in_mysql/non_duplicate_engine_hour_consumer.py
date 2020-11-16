# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
import mysql.connector
import sys
import json

cnx = mysql.connector.connect(
    user='root',
    password='',
    host='127.0.0.1',
    database='dev_fuel_db'
)

cursor = cnx.cursor()


def construct_sql():
    return """


    INSERT INTO `engine_hours_manager_enginehours`(`unit`, `beginning`, `initial_location`, `end`, `final_location`, `total_time`, `off_time`, `engine_hours`, `in_motion`, `idling`, `mileage`, `productivity`, `movement_productivity`, `driver`, `penalties`, `utilization`, `consumed`, `consumed_by_math`, `avg_consumption_by_math`, `consumed_by_math_in_motion`, `avg_consumed_by_math_in_motion`, `consumed_by_math_in_idle_run`, `avg_consumed_by_math_in_idle_run`, `avg_consumed_by_math_in_trips`) 
    VALUES (

     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s,
     %s,
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s

     );

    """


# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name from where the message will recieve
consumerTopicName = 'non_duplicate_engine_hour_records'

# Initialize consumer variable
engine_consumer = KafkaConsumer(consumerTopicName, group_id='engine_hours_group', bootstrap_servers=bootstrap_servers)

# Read and print message from consumer
for msg in engine_consumer:
    print("Topic Name=%s,Message=%s" % (msg.topic, msg.value))
    data = json.loads(msg.value)
    # print(data)
    cursor.executemany(construct_sql(), data)
    # print(cursor)
    cnx.commit()

# Terminate the script
cnx.close()
sys.exit()
