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
import os
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

# Keep topic names here to avoid importing producer.py (which may pull extra deps)
EMPLOYEE_TOPIC = "bf_employee_cdc"
DLQ_TOPIC = "bf_employee_cdc_dlq"  # optional: only used if exists


class cdcConsumer(Consumer):
    # if running outside Docker: host=localhost port=29092
    # if running inside Docker:  host=kafka      port=9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ""):
        conf = {
            "bootstrap.servers": f"{host}:{port}",
            "group.id": group_id,
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest",
        }
        super().__init__(conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            print(f"[consumer] group_id={self.group_id} subscribed to {topics}", flush=True)

            while self.keep_runnning:
                msg = self.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    processing_func(msg)
                except Exception as err:
                    print(f"[consumer][ERROR] failed to process message: {err}", flush=True)

        except KeyboardInterrupt:
            print("[consumer] received KeyboardInterrupt, shutting down...", flush=True)
        finally:
            self.close()


def _decode_value(msg):
    v = msg.value()
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="replace")
    return v


def _is_valid_event(data: dict) -> (bool, str):
    """Optional validation rules. Return (is_valid, reason)."""
    try:
        emp_id = data.get("emp_id")
        if emp_id is not None and int(emp_id) < 0:
            return False, "negative emp_id"

        salary = data.get("emp_salary")
        if salary is not None and int(salary) <= 10000:
            return False, "salary <= 10000"

        dob = data.get("emp_dob")
        if dob:
            if str(dob) < "2007-01-01":
                return False, "dob < 2007-01-01"
    except Exception as e:
        return False, f"validation exception: {e}"

    return True, ""


def _send_to_dlq(kafka_host: str, kafka_port: str, data: dict, reason: str):
    """Best-effort DLQ producer; does nothing if DLQ topic not used."""
    try:
        p = Producer({"bootstrap.servers": f"{kafka_host}:{kafka_port}", "acks": "all"})
        payload = dict(data)
        payload["_dlq_reason"] = reason
        p.produce(DLQ_TOPIC, value=json.dumps(payload).encode("utf-8"))
        p.flush(2)
    except Exception:
        pass


def update_dst(msg):
    raw = _decode_value(msg)
    if not raw:
        return

    data = json.loads(raw)

    # Kafka payload keys (confirmed from your console-consumer output):
    # action_id, emp_id, emp_FN, emp_LN, emp_dob, emp_city, emp_salary, action
    action = (data.get("action") or "").lower().strip()

    emp_id = data.get("emp_id")
    first_name = data.get("emp_FN")
    last_name = data.get("emp_LN")
    dob = data.get("emp_dob")
    city = data.get("emp_city")
    salary = data.get("emp_salary")

    # Optional validation + DLQ
    kafka_host = os.environ.get("KAFKA_HOST", "localhost")
    kafka_port = os.environ.get("KAFKA_PORT", "29092")
    valid, reason = _is_valid_event(data)
    if not valid:
        print(f"[consumer][DLQ] invalid event emp_id={emp_id}: {reason}", flush=True)
        _send_to_dlq(kafka_host, kafka_port, data, reason)
        return

    # Destination DB (db_dst) — default host mapping port 5433
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST", "127.0.0.1"),
            database=os.environ.get("DB_NAME", "postgres"),
            user=os.environ.get("DB_USER", "postgres"),
            port=os.environ.get("DB_DST_PORT", "5433"),
            password=os.environ.get("DB_PASSWORD", "postgres"),
            connect_timeout=3,
        )
        conn.autocommit = True
        cur = conn.cursor()

        if action == "delete":
            cur.execute("DELETE FROM employees WHERE emp_id = %s;", (emp_id,))
            return

        cur.execute(
            """
            UPDATE employees
               SET first_name=%s, last_name=%s, dob=%s, city=%s, salary=%s
             WHERE emp_id=%s;
            """,
            (first_name, last_name, dob, city, salary, emp_id),
        )

        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO employees(emp_id, first_name, last_name, dob, city, salary)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (emp_id, first_name, last_name, dob, city, salary),
            )

    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    group_id = sys.argv[1] if len(sys.argv) > 1 else "bf_cdc_group_1"
    consumer = cdcConsumer(group_id=group_id)
    consumer.consume([EMPLOYEE_TOPIC], update_dst)
