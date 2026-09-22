# FilteredFanout

One completion event, several workers. Each worker should see only its own slice, and a failure in one should not stall the others. Ingest finishes writing, then publishes one message. It does not know who is listening. Each consumer is a queue subscribed to that topic, with a filter so it only receives the messages it can act on, and a dead-letter queue so its failures stay on its own backlog.
