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
