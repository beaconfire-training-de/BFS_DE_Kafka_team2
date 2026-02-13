"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import csv
import json
import os
import time
from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
from pyspark.sql import SparkSession
import pandas as pd
from confluent_kafka.serialization import StringSerializer
import psycopg2

employee_topic_name = "bf_employee_cdc"

class cdcProducer(Producer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
        self.running = True

        self.last_action_id = 0 

    def fetch_cdc(self,):
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="db_source",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            #your logic should go here
            cur.execute("""
                SELECT action_id, emp_id, first_name, last_name, dob, city, salary, action
                FROM emp_cdc
                WHERE action_id > %s
                ORDER BY action_id
            """, (self.last_action_id,))

            rows = cur.fetchall()
            for row in rows:
                action_id, emp_id, first_name, last_name, dob, city, salary, action = row
                message = {
                    "action_id": action_id,
                    "emp_id": emp_id,
                    "emp_FN": first_name,
                    "emp_LN": last_name,
                    "emp_dob": str(dob),
                    "emp_city": city,
                    "emp_salary": salary,
                    "action": action
                }

                # Send to Kafka
                self.produce(
                employee_topic_name,
                key=str(emp_id).encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
                )

                # Update the last_action_id to the current action_id
                self.last_action_id = action_id

                # print the message being sent to Kafka
                print(f"[producer] Sent message: {json.dumps(message)}")


            self.flush()
            cur.close()
            conn.close()

        except Exception as err:
            print(f"Error fetching CDC: {err}")

    

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = cdcProducer()
    
    print("Producer started...")
    while producer.running:
        producer.fetch_cdc()
        time.sleep(1)  
    
