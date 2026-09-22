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

## Usage

```python
from aws_cdk import Stack
from constructs import Construct

from filtered_fanout import FilteredFanout


class IngestStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        fanout = FilteredFanout(self, "IngestComplete")
        lost = fanout.add_consumer(
            "lost-production",
            filter={"signal_type": ["Gas Today"]},
        )
        setpoints = fanout.add_consumer(
            "setpoints",
            filter={"signal_type": ["Tubing Pressure"]},
        )
        lost.add_worker(lost_production_fn)  # optional
```

`lost.queue` is the queue. Attach a Lambda with `add_worker`, poll it from Fargate, or leave it unwired. `fanout.topic` is the topic ingest publishes to.

The filter matches if the published attribute intersects the list. A message carrying both `Gas Today` and `Tubing Pressure` is delivered to both queues. A message carrying neither is delivered to neither. SNS drops it. Nobody wakes up to discard it.

## Publishing

Put the filter keys on message attributes, not only in the body. Attributes are the stable contract. The body can grow without changing who receives the message.

```python
sns.publish(
    TopicArn=topic_arn,
    Message=json.dumps({
        "operator": operator_name,
        "well": well_name,
        "latest_time": latest_time,
    }),
    MessageAttributes={
        "signal_type": {
            "DataType": "String.Array",
            "StringValue": json.dumps(["Gas Today", "Tubing Pressure"]),
        },
    },
)
```

`String.Array` values are a JSON array string. The filter `["Gas Today"]` matches when that value is one of the entries. Keys are ANDed together. Values in one list are ORed.

## What you get

One standard SNS topic, and a topic policy that grants `sns:Publish` to this account and to no other principal.

For each consumer:

- a standard SQS queue
- its own dead-letter queue, `maxReceiveCount` 5, retained for 14 days
- an SNS subscription whose filter is the map you passed
- raw message delivery, so the worker sees your JSON and not the SNS envelope
- the queue policy CDK adds, which allows `sqs:SendMessage` only from this topic
- an alarm when that dead-letter queue has any visible messages
