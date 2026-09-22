"""SNS fan-out to filtered SQS consumers, each with its own dead-letter queue."""

from __future__ import annotations

from enum import Enum
from typing import Mapping, Sequence

from aws_cdk import Duration
