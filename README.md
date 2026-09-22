# FilteredFanout

One completion event, several workers. Each worker should see only its own slice, and a failure in one should not stall the others. Ingest finishes writing, then publishes one message. It does not know who is listening. Each consumer is a queue subscribed to that topic, with a filter so it only receives the messages it can act on, and a dead-letter queue so its failures stay on its own backlog.

```
ingest
  │
  │  publish once
  ▼
SNS topic
  ├─ filter: volume signals  → queue → worker → DLQ
  ├─ filter: pressure        → queue → worker → DLQ
  └─ filter: whatever is next → queue → worker → DLQ
```

SNS is only the fan-out. It does not hold a backlog. SQS is where a slow or failing worker piles up, retries, and eventually dead-letters, while the other consumers keep moving.

A new consumer is another `add_consumer` call. The publisher stays the same.
