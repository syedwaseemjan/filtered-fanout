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
