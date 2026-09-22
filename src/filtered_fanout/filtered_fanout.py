"""SNS fan-out to filtered SQS consumers, each with its own dead-letter queue."""

from __future__ import annotations

from enum import Enum
from typing import Mapping, Sequence

from aws_cdk import Duration
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk import aws_sqs as sqs
from constructs import Construct

# A message the worker keeps failing is dead-lettered after this many receives.
DEFAULT_MAX_RECEIVE_COUNT = 5
DLQ_RETENTION = Duration.days(14)


class FilterScope(Enum):
    """Where SNS evaluates the subscription filter.

    Message attributes are the stable contract: the body can grow without
    changing who receives the message. Message body matches a filter against
    top-level JSON fields, and it breaks when that shape shifts.
    """

    MESSAGE_ATTRIBUTES = "MessageAttributes"
    MESSAGE_BODY = "MessageBody"


class FilteredFanout(Construct):
    """One SNS topic, several filtered SQS consumers.

    Ingest publishes once and does not know who is listening. Each consumer
    is a queue subscribed to ``topic`` with its own filter and its own
    dead-letter queue, so a failing worker piles up on its own backlog.

    The dead-letter queue is the SQS redrive queue: the worker received the
    message and failed until ``maxReceiveCount``. This construct does not
    set an SNS subscription dead-letter queue, which is the other path
    (SNS could not deliver to the queue at all).
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        max_receive_count: int = DEFAULT_MAX_RECEIVE_COUNT,
    ) -> None:
        super().__init__(scope, id)
        _validate_receive_count(max_receive_count)
        self.max_receive_count = max_receive_count

        self.topic = sns.Topic(self, "Topic")
        self.topic.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowPublishFromThisAccount",
                actions=["sns:Publish"],
                principals=[iam.AccountRootPrincipal()],
                resources=[self.topic.topic_arn],
            )
        )
        self.consumers: list[FanoutConsumer] = []

    def add_consumer(
        self,
        id: str,
        *,
        filter: Mapping[str, Sequence[str]],
        filter_scope: FilterScope = FilterScope.MESSAGE_ATTRIBUTES,
        max_receive_count: int | None = None,
        visibility_timeout: Duration | None = None,
    ) -> FanoutConsumer:
        """Subscribe a new queue. The publisher does not change.

        ``filter`` is a map of attribute name (or, for body scope, top-level
        JSON field) to the values that should be delivered. SNS ORs values
        in one list and ANDs keys. A ``String.Array`` attribute matches when
        its entries intersect the list.

        ``max_receive_count`` overrides the fan-out default for this consumer.
        ``visibility_timeout`` overrides the 30 second SQS default. Set it
        above the worker's runtime so a message is not delivered again while
        that worker is still running.
        """
        receive_count = (
            self.max_receive_count if max_receive_count is None else max_receive_count
        )
        _validate_filter(filter)
        _validate_receive_count(receive_count)
        if visibility_timeout is not None and not isinstance(visibility_timeout, Duration):
            raise TypeError("visibility_timeout must be a Duration")
        if not isinstance(filter_scope, FilterScope):
            raise TypeError(
                "filter_scope must be FilterScope.MESSAGE_ATTRIBUTES "
                "or FilterScope.MESSAGE_BODY"
            )

        consumer = FanoutConsumer(
            self,
            id,
            topic=self.topic,
            filter=filter,
            filter_scope=filter_scope,
            max_receive_count=receive_count,
            visibility_timeout=visibility_timeout,
        )
        self.consumers.append(consumer)
        return consumer


class FanoutConsumer(Construct):
    """A queue, its dead-letter queue, and the subscription that feeds it.

    ``queue`` is what the caller polls. ``add_worker`` is optional.
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        topic: sns.ITopic,
        filter: Mapping[str, Sequence[str]],
        filter_scope: FilterScope,
        max_receive_count: int,
        visibility_timeout: Duration | None,
    ) -> None:
        super().__init__(scope, id)
        # SQS default. Stored even when the template omits it, so add_worker
        # can tell a slow function from a queue that will redeliver too soon.
        self.visibility_timeout = visibility_timeout or Duration.seconds(30)
        self.dead_letter_queue = sqs.Queue(
            self,
            "DeadLetterQueue",
            retention_period=DLQ_RETENTION,
        )
