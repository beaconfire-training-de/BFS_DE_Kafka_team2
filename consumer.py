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


import json
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException,Message
from __future__ import annotations
from typing import Callable, List
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from producer import employee_topic_name

class cdcConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            print(f"[consumer] group_id={self.group_id} subscribed to {topics}")
            while self.keep_runnning:
                msg = self.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    # End of partition is not a real error
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    processing_func(msg)
                except Exception as err:
                    # Keep the consumer alive; you can optionally push failures to a DLQ.
                    print(f"[consumer][ERROR] failed to process message: {err}", file=sys.stderr)
        except KeyboardInterrupt:
            print("[consumer] received KeyboardInterrupt, shutting down...")

        finally:
            self.close()

def _normalize_action(action: str) -> str:
    a = (action or "").strip().lower()
    if a in {"i", "ins", "insert", "create", "c"}:
        return "insert"
    if a in {"u", "upd", "update", "modify", "m"}:
        return "update"
    if a in {"d", "del", "delete", "remove", "r"}:
        return "delete"
    return a



def update_dst(msg):
    e = Employee(**(json.loads(msg.value())))
    e = Employee(**payload)
    action = _normalize_action(e.action)
    if action not in {"insert", "update", "delete"}:
        print(f"[consumer][WARN] unknown action '{e.action}' for emp_id={e.emp_id}; skipping")
        return
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            port = '5433', # change this port number to align with the docker compose file
            password="postgres")
        conn.autocommit = True
        cur = conn.cursor()
        #your logic goes here
        if action == "delete":
            cur.execute("DELETE FROM employees WHERE emp_id = %s", (e.emp_id,))
            print(f"[consumer] DELETE emp_id={e.emp_id} (rows={cur.rowcount})")

        else:
            # Try UPDATE first (covers 'update' and also makes 'insert' idempotent)
            cur.execute(
                """
                UPDATE employees
                SET first_name = %s,
                    last_name  = %s,
                    dob        = %s,
                    city       = %s,
                    salary     = %s
                WHERE emp_id = %s
                """,
                (e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city, e.emp_salary, e.emp_id),
            )

            if cur.rowcount == 0:
                # Row does not exist yet => INSERT with the same emp_id from source
                cur.execute(
                    """
                    INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (e.emp_id, e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city, e.emp_salary),
                )
                print(f"[consumer] INSERT emp_id={e.emp_id}")
            else:
                print(f"[consumer] UPDATE emp_id={e.emp_id}")



        cur.close()
    except Exception as err:
        print(err)

if __name__ == '__main__':
    group_id = sys.argv[1] if len(sys.argv) > 1 else "bf_employee_cdc_group"
    consumer = cdcConsumer(group_id=group_id)
    consumer.consume([employee_topic_name], update_dst)