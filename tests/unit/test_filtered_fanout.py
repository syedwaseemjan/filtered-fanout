"""Synthesize a fan-out and assert the template, not a live account."""

import aws_cdk as cdk
import pytest
from aws_cdk import Duration
from aws_cdk import assertions
from aws_cdk import aws_lambda as lambda_

from filtered_fanout import FilterScope, FilteredFanout


def _stack():
    app = cdk.App()
    stack = cdk.Stack(app, "Test")
    fanout = FilteredFanout(stack, "IngestComplete")
    lost = fanout.add_consumer(
        "lost-production",
        filter={"signal_type": ["Gas Today"]},
    )
    setpoints = fanout.add_consumer(
        "setpoints",
        filter={"signal_type": ["Tubing Pressure"]},
    )
    return stack, fanout, lost, setpoints


def _queues(template: assertions.Template):
    queues = template.find_resources("AWS::SQS::Queue")
    consumers = {}
    dead_letters = {}
    for logical_id, resource in queues.items():
        props = resource.get("Properties", {})
        if "RedrivePolicy" in props:
            consumers[logical_id] = props
        else:
            dead_letters[logical_id] = props
    return consumers, dead_letters
