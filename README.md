# BFS_DE_Kafka_team2


## setup_source.sql -- Shuxuan Li

### employees table
- emp_id SERIAL PRIMARY KEY: auto-incrementing employee ID
- Columns: first_name, last_name, dob, city, salary

### emp_cdc table (CDC audit log)
- action_id SERIAL PRIMARY KEY: auto-incrementing change sequence
- Mirrors all employees columns + action VARCHAR(100) to record operation type

### employee_cdc_trigger_func()
- PostgreSQL trigger function, RETURNS TRIGGER
- TG_OP = 'INSERT': captures NEW row → inserts into emp_cdc with action = 'INSERT'
- TG_OP = 'UPDATE': captures NEW row → inserts into emp_cdc with action = 'UPDATE'
- TG_OP = 'DELETE': captures OLD row → inserts into emp_cdc with action = 'DELETE'

### employee_cdc_trigger
- AFTER INSERT OR UPDATE OR DELETE ON employees
- FOR EACH ROW EXECUTE FUNCTION employee_cdc_trigger_func()


## setup_dst.sql -- Shuxuan Li

### employees table
- Identical schema to source employees table
- No trigger: destination is write target for the Kafka consumer


## producer.py -- Duc Anh Nguyen

### How It Works:

1. Reads CDC events from emp_cdc table in source DB

2. Formats each row into a JSON message compatible with employee.py.

3. Sends messages to Kafka topic bf_employee_cdc

4. Tracks last_action_id in memory to avoid resending rows: self.last_action_id

5. Continuously polls the source database every 1 second


### Database Query Logic

    SELECT action_id, emp_id, first_name, last_name, dob, city, salary, action
    
    FROM emp_cdc
    
    WHERE action_id > last_action_id
    
    ORDER BY action_id

- Only select rows that have the next action_id. This can make sure there are no duplicates.

- For each row:

- Map DB columns → JSON keys matching employee.py

- Send JSON to Kafka with produce(topic, key=str(emp_id), value=json.dumps(message))

- Update last_action_id = action_id

- Flush producer buffer to Kafka


## Consumer.py -- Huiyu Song

### _decode_value
  v.decode("utf-8", errors="replace")

### _is_valid_event
Data Entry Sanity Check 
1. emp_id is not None and int(emp_id) < 0:
2. salary is not None and int(salary) <= 10000:
3. str(dob) < "2007-01-01":
4. Fail Gracfefuly: except Exception as e:

### _send_to_dlq
p.produce(DLQ_TOPIC, value=json.dumps(payload).encode("utf-8"))
        p.flush(2)
    except Exception:
        pass
        
### update_dst(msg)
1. json.loads(raw)
2. action = (data.get("action") or "").lower().strip()
3. valid, reason = _is_valid_event(data)
4. _send_to_dlq(kafka_host, kafka_port, data, reason)
5. psycopg2.connect , cur = conn.cursor() cur.execute()  --- Insert and Delete
6. cur.close()
7. conn.close()
