# BFS_DE_Kafka_team2










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
