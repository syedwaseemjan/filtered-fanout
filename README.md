# FilteredFanout

One completion event, several workers. Each worker should see only its own slice, and a failure in one should not stall the others. An order is placed, then the app publishes one message. It does not know who is listening. Each consumer is a queue subscribed to that topic, with a filter so it only receives the messages it can act on, and a dead-letter queue so its failures stay on its own backlog.

```
app
  │
  │  publish once
  ▼
SNS topic
  ├─ filter: email  → queue → worker → DLQ
  ├─ filter: sms    → queue → worker → DLQ
  └─ filter: whatever is next → queue → worker → DLQ
```

SNS is only the fan-out. It does not hold a backlog. SQS is where a slow or failing worker piles up, retries, and eventually dead-letters, while the other consumers keep moving.

A new consumer is another `add_consumer` call. The publisher stays the same.

## Usage

```python
from aws_cdk import Stack
from constructs import Construct

from filtered_fanout import FilteredFanout


class OrdersStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        fanout = FilteredFanout(self, "OrderPlaced")
        email = fanout.add_consumer(
            "email",
            filter={"channel": ["email"]},
        )
        sms = fanout.add_consumer(
            "sms",
            filter={"channel": ["sms"]},
        )
        email.add_worker(send_email_fn)  # optional
```

`email.queue` is the queue. Attach a Lambda with `add_worker`, poll it from Fargate, or leave it unwired. `fanout.topic` is the topic the app publishes to.

The filter matches if the published attribute intersects the list. A message carrying both `email` and `sms` is delivered to both queues. A message carrying neither is delivered to neither. SNS drops it. Nobody wakes up to discard it.

## Publishing

Put the filter keys on message attributes, not only in the body. Attributes are the stable contract. The body can grow without changing who receives the message.

```python
sns.publish(
    TopicArn=topic_arn,
    Message=json.dumps({
        "order_id": order_id,
        "customer": customer_name,
    }),
    MessageAttributes={
        "channel": {
            "DataType": "String.Array",
            "StringValue": json.dumps(["email", "sms"]),
        },
    },
)
```

`String.Array` values are a JSON array string. The filter `["email"]` matches when that value is one of the entries. Keys are ANDed together. Values in one list are ORed.

## What you get

One standard SNS topic, and a topic policy that grants `sns:Publish` to this account and to no other principal.

For each consumer:

- a standard SQS queue
- its own dead-letter queue, `maxReceiveCount` 5, retained for 14 days
- an SNS subscription whose filter is the map you passed
- raw message delivery, so the worker sees your JSON and not the SNS envelope
- the queue policy CDK adds, which allows `sqs:SendMessage` only from this topic
- an alarm when that dead-letter queue has any visible messages

The alarm has no action until you add one. `consumer.alarm.add_alarm_action(...)` is the hook. Pass `visibility_timeout` to `add_consumer` when the worker runs longer than the 30 second SQS default, or the message becomes visible again while that worker is still running.

The dead-letter queue here is the SQS redrive queue (the worker received the message and failed until `maxReceiveCount`); an SNS subscription dead-letter queue, which CDK can also set, is the other path, for when SNS could not hand the message to the queue at all, and this construct does not add one.

`add_worker` attaches a Lambda event source with `ReportBatchItemFailures`. Return the failed message ids and the rest of the batch is not retried:

```python
def handler(event, context):
    failures = []
    for record in event["Records"]:
        try:
            handle(json.loads(record["body"]))
        except Exception:
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
```

## Filtering on the body

Attributes are the default. Filtering on the body works when the subscription sets the filter scope to `MessageBody`. It is the version that is easier to get wrong when the JSON shape shifts. The same map is matched against top-level JSON fields.

```python
from filtered_fanout import FilterScope

fanout.add_consumer(
    "email",
    filter={"channel": ["email"]},
    filter_scope=FilterScope.MESSAGE_BODY,
)
```

## When to leave it alone

A single consumer can be a queue with no topic. Work that must be strictly ordered per key wants a FIFO queue and a single consumer, not this fan-out. A workflow with waits, branches, and human steps wants Step Functions. Many event types, replay, or cross-account routing wants EventBridge. A nightly sweep can stay as a schedule for backfill. The event path replaces the hourly "check every order" run, and the schedule can remain for orders the event missed.

A follow-on hop, such as sending a receipt after the order ships, is this same construct used again. The worker publishes to a second topic. The first publisher does not change.

## v0.1

This is the construct, the assertion tests, one LocalStack test, and this guide. `cdk synth` already gives SAM or Serverless users a template if they still write YAML. There is no second implementation, and no sample app.

## Tests

Assertion tests synthesize a stack. They run without AWS, and they are the ones that run on every push:

```
pip install -e ".[dev]"
pytest -m "not integration"
```

The integration test publishes email only, sms only, both, and a message that matches neither. The email queue gets the first and third, the sms queue gets the second and third, and the last is dropped. LocalStack is where that runs. Moto's SNS filter support is incomplete, especially for body scope, so a green moto run is not the proof.

```
docker compose up -d
pip install -e ".[dev,integration]"
LOCALSTACK_URL=http://localhost:4566 pytest -m integration
```

With the integration extra installed and `LOCALSTACK_URL` unset, the test starts LocalStack itself.
