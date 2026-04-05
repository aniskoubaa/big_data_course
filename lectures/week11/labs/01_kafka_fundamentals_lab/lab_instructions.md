# Lab 09-1: Apache Kafka Fundamentals

**Course:** SE446 — Big Data Systems  
**Week:** 09 | **Estimated Duration:** 75 minutes

---

## Overview

In this lab you will work hands-on with Apache Kafka — the distributed messaging system that powers real-time data pipelines at companies like LinkedIn, Netflix, and Uber. You will create topics, produce and consume messages, experiment with partitioning strategies, and observe how consumer groups distribute work.

By the end of this lab you will be able to:
- Create and inspect Kafka topics from the command line.
- Produce messages with and without keys using a Python client.
- Consume messages and understand offsets and consumer groups.
- Predict which partition a keyed message will land in.
- Explain how consumer group rebalancing works.

---

## Background Concepts

> Read this section carefully before writing code.

### 1. What Problem Does Kafka Solve?

In a traditional system, components communicate **directly** — the web app writes to a database, which is read by an analytics service, which feeds a dashboard. As the number of components grows, the connections explode:

```
Without Kafka (point-to-point):          With Kafka (hub-and-spoke):
                                         
  App ──────▶ DB                           App ──┐
  App ──────▶ Analytics                    App ──┼──▶ KAFKA ──┬──▶ DB
  App ──────▶ Dashboard                    Sensor┘           ├──▶ Analytics
  Sensor ───▶ DB                                              └──▶ Dashboard
  Sensor ───▶ Analytics
  
  10 connections for 5 components          3 producers + 3 consumers = 6 connections
```

Kafka **decouples** producers from consumers. Producers don't need to know who reads their data, and consumers don't need to know who produces it.

### 2. Topics, Partitions, and Offsets

A **topic** is a named stream of messages (think of it as a table). Each topic is divided into **partitions** — ordered, immutable sequences of messages. Each message in a partition gets a unique, monotonically increasing **offset** (like a row number).

```
Topic: "crimes" (3 partitions)

Partition 0:  [offset 0] [offset 1] [offset 2] [offset 3]  ──▶ newest
Partition 1:  [offset 0] [offset 1] [offset 2]              ──▶ newest
Partition 2:  [offset 0] [offset 1]                          ──▶ newest
```

**Why partitions matter:**
- **Parallelism:** Each partition can be read by a different consumer simultaneously.
- **Ordering:** Messages within a partition are strictly ordered. Across partitions, there is no ordering guarantee.
- **Throughput:** More partitions = higher throughput (up to a point).

### 3. Producers and Keys

A producer sends messages to a topic. Each message has a **value** (the data) and an optional **key**:

- **No key:** Messages are distributed across partitions using round-robin. Good for even load balancing.
- **With key:** `hash(key) % num_partitions` determines the partition. All messages with the same key go to the **same partition**, preserving order for that key.

This is critical: if you need all events for "district 8" to be processed in order, you must use "district-8" as the key.

### 4. Consumers and Consumer Groups

A **consumer** reads messages from partitions. A **consumer group** is a team of consumers that divide the work:

```
Topic with 3 partitions:

Consumer Group "analytics" (3 consumers):
  Consumer A → Partition 0
  Consumer B → Partition 1
  Consumer C → Partition 2
  
Consumer Group "archiver" (1 consumer):
  Consumer X → Partitions 0, 1, 2 (reads all three)
```

**Rules:**
- Each partition is assigned to exactly **one** consumer in a group.
- If consumers > partitions, extra consumers **sit idle**.
- If a consumer dies, its partitions are **rebalanced** to surviving consumers.
- Two **different groups** reading the same topic each get **all** messages (independent).

### 5. Retention — Messages Are Not Deleted After Reading

Unlike traditional message queues (RabbitMQ), Kafka **retains** messages for a configurable period (default: 7 days). A consumer tracks its progress via its offset. This means:
- A new consumer can read **all historical messages** (`auto.offset.reset=earliest`).
- A crashed consumer can resume from its last committed offset (no data loss).
- Multiple consumer groups can independently process the same data.

---

## Prerequisites

- SSH access to the cluster: `ssh <username>@134.209.172.50`
- Kafka running on the cluster (broker at `localhost:9092`)
- Python 3 with `confluent-kafka` installed

---

## Setup

```bash
# SSH to master node
ssh <your_username>@134.209.172.50

# Verify Kafka is running
kafka-topics.sh --list --bootstrap-server localhost:9092
```

If you see a list of existing topics (possibly empty), Kafka is running correctly.

---

## Part 1: Topic Management

**Goal:** Create, inspect, and understand Kafka topics from the command line.

### Step 1: Create a Topic

```bash
kafka-topics.sh --create \
    --topic lab-crimes \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1
```

- `--partitions 3`: Split data across 3 partitions for parallelism.
- `--replication-factor 1`: Only 1 copy (we have a single-broker cluster). In production, you'd use 3.

### Step 2: Inspect the Topic

```bash
kafka-topics.sh --describe \
    --topic lab-crimes \
    --bootstrap-server localhost:9092
```

Record the output:

| Field | Value |
|-------|-------|
| Partition Count | |
| Replication Factor | |
| Leader for Partition 0 | |
| Leader for Partition 1 | |
| Leader for Partition 2 | |

### Step 3: List All Topics

```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Questions — Part 1

1. What does "replication factor" mean? Why would you set it to 3 in production? What risk does replication-factor 1 carry?
2. Each partition has a "leader" broker. What is the leader responsible for? What happens if the leader crashes?
3. You have a 10-node Kafka cluster. What is the maximum replication factor you can set? What is the minimum for fault tolerance?
4. If you create a topic with 6 partitions and your consumer group has 4 consumers, how are partitions distributed?

---

## Part 2: Producing Messages

**Goal:** Publish messages to Kafka using both CLI and Python, with and without keys.

### Step 1: CLI Producer (Quick Test)

Open two terminal windows. In Terminal 1, start a consumer:
```bash
kafka-console-consumer.sh --topic lab-crimes \
    --bootstrap-server localhost:9092
```

In Terminal 2, start a producer and type messages:
```bash
kafka-console-producer.sh --topic lab-crimes \
    --bootstrap-server localhost:9092
```

Type some JSON lines:
```
{"type":"THEFT","district":8,"hour":14}
{"type":"BATTERY","district":11,"hour":22}
{"type":"ROBBERY","district":3,"hour":2}
```

Observe: messages appear in Terminal 1 in real time.

### Step 2: Python Producer (No Keys)

```python
from confluent_kafka import Producer
import json, time

producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Delivery callback — called when message is confirmed delivered
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to partition {msg.partition()} | offset {msg.offset()}")

crimes = [
    {"id": 1, "type": "THEFT",     "district": 8,  "hour": 14, "arrest": False},
    {"id": 2, "type": "BATTERY",   "district": 11, "hour": 22, "arrest": True},
    {"id": 3, "type": "ROBBERY",   "district": 3,  "hour": 2,  "arrest": False},
    {"id": 4, "type": "THEFT",     "district": 8,  "hour": 16, "arrest": False},
    {"id": 5, "type": "ASSAULT",   "district": 5,  "hour": 20, "arrest": True},
    {"id": 6, "type": "BURGLARY",  "district": 7,  "hour": 3,  "arrest": False},
]

for crime in crimes:
    producer.produce(
        topic="lab-crimes",
        value=json.dumps(crime).encode("utf-8"),
        callback=delivery_report
    )
    producer.poll(0)  # trigger delivery callbacks

producer.flush()  # wait for all deliveries
print(f"\n{len(crimes)} messages produced (no key — round-robin partitioning)")
```

**Record:** Which partition did each message land in? Is the distribution approximately even?

### Step 3: Python Producer (With Keys)

```python
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Produce 20 messages keyed by district
for i in range(20):
    district = (i % 5) + 1  # districts 1 through 5
    crime = {
        "id": 200 + i,
        "type": ["THEFT", "BATTERY", "ROBBERY", "ASSAULT"][i % 4],
        "district": district,
        "hour": i % 24,
        "arrest": i % 3 == 0
    }
    producer.produce(
        topic="lab-crimes",
        key=str(district).encode("utf-8"),  # key = district number
        value=json.dumps(crime).encode("utf-8"),
        callback=delivery_report
    )
    producer.poll(0)

producer.flush()
print("20 keyed messages produced")
```

### Step 4: Verify Key-Based Partitioning

```python
from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'verify-keys-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['lab-crimes'])

# Collect: which key went to which partition?
key_partition = {}
count = 0
while count < 40:
    msg = consumer.poll(timeout=2.0)
    if msg is None:
        break
    if msg.error():
        continue
    key = msg.key().decode("utf-8") if msg.key() else "NONE"
    partition = msg.partition()
    key_partition.setdefault(key, set()).add(partition)
    count += 1

consumer.close()

print("\n=== Key → Partition Mapping ===")
for key, partitions in sorted(key_partition.items()):
    status = "✅ Consistent" if len(partitions) == 1 else "❌ Split across partitions!"
    print(f"  Key '{key}' → Partition(s) {partitions}  {status}")
```

### Questions — Part 2

1. With 3 partitions and no key, messages should distribute roughly evenly. Did your results confirm this? What algorithm does Kafka use for keyless distribution?
2. With keys, each unique district value should map to exactly one partition. Were any keys split across multiple partitions? If so, explain why (hint: did you use the same topic for keyed and keyless messages?).
3. The `callback` function in `delivery_report` is invoked asynchronously. Why is `producer.poll(0)` needed after each `produce()` call? What happens if you omit it?
4. The producer call `produce()` returns immediately — the message is not yet sent. Where does the message go before delivery? What triggers the actual network send?
5. *(Challenge)* You produce messages with keys "A", "B", "C", "D", "E" to a topic with 3 partitions. Keys are hashed via `murmur2(key) % 3`. Can you guarantee that "A" and "B" will be in different partitions? Why or why not?

---

## Part 3: Consumer Groups

**Goal:** Observe how Kafka distributes partitions across consumers in a group, and what happens when consumers join or leave.

### Step 1: Single Consumer Reading All Partitions

```python
from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'single-consumer-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['lab-crimes'])

partition_counts = {}
for _ in range(50):
    msg = consumer.poll(timeout=2.0)
    if msg is None:
        break
    if msg.error():
        continue
    p = msg.partition()
    partition_counts[p] = partition_counts.get(p, 0) + 1

consumer.close()

print("=== Single Consumer — Partition Reads ===")
for p, c in sorted(partition_counts.items()):
    print(f"  Partition {p}: {c} messages")
print(f"  Total: {sum(partition_counts.values())} messages")
```

A single consumer in the group reads **all** 3 partitions.

### Step 2: Two Consumers in the Same Group

Run this in **two separate SSH sessions** (Terminal 1 and Terminal 2), both using the **same `group.id`**:

```python
# Run in BOTH terminals (same code, same group.id)
from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'two-consumer-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['lab-crimes'])

print("Waiting for messages (press Ctrl+C to stop)...")
try:
    while True:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            continue
        if msg.error():
            continue
        crime = json.loads(msg.value().decode("utf-8"))
        print(f"[Partition {msg.partition()}] Offset {msg.offset()} | {crime.get('type', '?')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

While both consumers are running, produce new messages from a third terminal. Observe how messages are split.

### Step 3: Record Consumer Group Behavior

| Scenario | Consumers | Partitions per Consumer | Idle Consumers |
|----------|:---------:|:-----------------------:|:--------------:|
| 1 consumer, 3 partitions | 1 | 3 | 0 |
| 2 consumers, 3 partitions | 2 | ? and ? | ? |
| 3 consumers, 3 partitions | 3 | ? | ? |
| 4 consumers, 3 partitions | 4 | ? | ? |

### Step 4: Observe Rebalancing

1. Start 3 consumers (same `group.id`).
2. Kill one consumer (Ctrl+C).
3. Observe: does the remaining 2 consumers pick up the orphaned partition?
4. This process is called a **rebalance** — Kafka automatically redistributes partitions.

### Questions — Part 3

1. When 2 consumers share 3 partitions, how are partitions assigned? Which consumer gets 2 partitions and which gets 1? Is this assignment deterministic?
2. You have 3 partitions and 5 consumers in the same group. How many consumers are idle? Is there any benefit to having more consumers than partitions?
3. A consumer crashes and Kafka triggers a rebalance. During rebalance, can the other consumers process messages? What is the impact of frequent rebalances on throughput?
4. Two consumer groups (`group-A` and `group-B`) subscribe to the same topic. Does `group-A` "steal" messages from `group-B`? Explain the difference between the **queue** pattern and the **pub/sub** pattern in Kafka.
5. *(Challenge)* In a rebalance, consumer offsets are committed. If `enable.auto.commit=true` (default), commits happen periodically. What can go wrong if a consumer crashes between processing a message and the next auto-commit? How does `enable.auto.commit=false` help?

---

## Part 4: Monitoring and Inspection

**Goal:** Use Kafka CLI tools to inspect consumer group offsets and detect consumer lag.

### Step 1: Check Consumer Group Offsets

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group single-consumer-group
```

This shows:

| Column | Meaning |
|--------|---------|
| TOPIC | Topic name |
| PARTITION | Partition number |
| CURRENT-OFFSET | Where the consumer is reading |
| LOG-END-OFFSET | Latest message in the partition |
| LAG | `LOG-END-OFFSET - CURRENT-OFFSET` (how far behind) |

### Step 2: Measure Consumer Lag

1. Produce 100 new messages quickly.
2. Run the `--describe` command immediately.
3. Record the LAG values.
4. Start a consumer, let it catch up, and run `--describe` again.

| Timing | Partition 0 LAG | Partition 1 LAG | Partition 2 LAG | Total LAG |
|--------|:---------------:|:---------------:|:---------------:|:---------:|
| Before consumer starts | | | | |
| After consumer catches up | | | | |

### Questions — Part 4

1. What does a LAG of 0 mean? What about a steadily increasing LAG?
2. In a production system, you have a consumer group with a LAG of 50,000 and growing. What are two ways to reduce the lag?
3. Why is monitoring consumer lag critical for real-time pipelines? What is the business impact of high lag in a fraud detection system?

---

## Reflection Questions

Answer these after completing all parts.

1. A ride-sharing app produces events: `ride_requested`, `driver_assigned`, `ride_started`, `ride_completed`. All events for the same ride must be processed **in order**. How would you design the Kafka topic — how many partitions, and what key would you use?

2. You are designing a data pipeline: website clickstream → Kafka → real-time analytics + batch archival. Draw the architecture showing which components are producers, which are consumers, and how many consumer groups you need.

3. A colleague stores all messages in a single Kafka partition "for simplicity." Your cluster has 6 consumer instances. What is the maximum throughput this design can achieve compared to a 6-partition topic? Explain quantitatively.

4. Kafka retains messages for 7 days. A new team joins and wants to process the last 30 days of data. Can they? What Kafka configuration change would enable this? What is the trade-off?

---

## Deliverables

Submit a report (PDF or Markdown) containing:

1. Output from topic describe command (Part 1).
2. Delivery report output showing partition assignments (Part 2).
3. Key-partition mapping showing consistent hashing (Part 2, Step 4).
4. Consumer group partition distribution table (Part 3).
5. Consumer lag measurements (Part 4).
6. Written answers to **all** questions (Parts 1–4 + Reflection).
7. Commit your Python scripts to your team's GitHub repo.

---

## Grading

| Component | Points |
|-----------|:------:|
| Part 1: Topic management + 4 questions | 15 |
| Part 2: Producing (keyed + keyless) + 5 questions | 25 |
| Part 3: Consumer groups + rebalancing + 5 questions | 25 |
| Part 4: Monitoring + lag measurement + 3 questions | 15 |
| Reflection questions (4 questions) | 20 |
| **Total** | **100** |
