"""Synthesize a fan-out and assert the template, not a live account."""

import aws_cdk as cdk
import pytest
from aws_cdk import Duration
from aws_cdk import assertions
from aws_cdk import aws_lambda as lambda_

from filtered_fanout import FilterScope, FilteredFanout
