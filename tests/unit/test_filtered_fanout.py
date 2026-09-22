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


def test_two_consumers_fan_out_to_their_own_queues():
    stack, _, _, _ = _stack()
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::SNS::Topic", 1)
    template.resource_count_is("AWS::SNS::Subscription", 2)

    consumers, dead_letters = _queues(template)
    assert len(consumers) == 2
    assert len(dead_letters) == 2

    redrive_targets = []
    for props in consumers.values():
        redrive = props["RedrivePolicy"]
        assert redrive["maxReceiveCount"] == 5
        target = redrive["deadLetterTargetArn"]["Fn::GetAtt"][0]
        assert target in dead_letters
        redrive_targets.append(target)
    assert len(set(redrive_targets)) == 2

    subscribed = set()
    filters = []
    for resource in template.find_resources("AWS::SNS::Subscription").values():
        props = resource["Properties"]
        assert props["RawMessageDelivery"] is True
        assert props.get("FilterPolicyScope", "MessageAttributes") == "MessageAttributes"
        endpoint = props["Endpoint"]["Fn::GetAtt"][0]
        assert endpoint in consumers
        assert endpoint not in dead_letters
        subscribed.add(endpoint)
        filters.append(props["FilterPolicy"])
    assert subscribed == set(consumers)
    assert {"signal_type": ["Gas Today"]} in filters
    assert {"signal_type": ["Tubing Pressure"]} in filters


def test_topic_policy_allows_publish_only_from_this_account():
    stack, _, _, _ = _stack()
    template = assertions.Template.from_stack(stack)
    template.resource_count_is("AWS::SNS::TopicPolicy", 1)

    policies = template.find_resources("AWS::SNS::TopicPolicy")
    statements = next(iter(policies.values()))["Properties"]["PolicyDocument"]["Statement"]
    assert len(statements) == 1
    statement = statements[0]
    assert statement["Effect"] == "Allow"
    assert statement["Action"] == "sns:Publish"
    assert statement["Principal"]["AWS"] == {
        "Fn::Join": [
            "",
            [
                "arn:",
                {"Ref": "AWS::Partition"},
                ":iam::",
                {"Ref": "AWS::AccountId"},
                ":root",
            ],
        ]
    }
