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


def test_queue_policy_allows_send_only_from_this_topic():
    stack, _, _, _ = _stack()
    template = assertions.Template.from_stack(stack)
    template.resource_count_is("AWS::SQS::QueuePolicy", 2)

    topics = list(template.find_resources("AWS::SNS::Topic"))
    assert len(topics) == 1
    consumers, dead_letters = _queues(template)

    for resource in template.find_resources("AWS::SQS::QueuePolicy").values():
        props = resource["Properties"]
        queue_id = props["Queues"][0]["Ref"]
        assert queue_id in consumers
        assert queue_id not in dead_letters
        statement = props["PolicyDocument"]["Statement"][0]
        assert statement["Effect"] == "Allow"
        assert statement["Action"] == "sqs:SendMessage"
        assert statement["Principal"] == {"Service": "sns.amazonaws.com"}
        assert statement["Condition"]["ArnEquals"]["aws:SourceArn"] == {"Ref": topics[0]}


def test_dead_letter_alarm_fires_when_any_message_is_visible():
    stack, _, _, _ = _stack()
    template = assertions.Template.from_stack(stack)
    template.resource_count_is("AWS::CloudWatch::Alarm", 2)
    _, dead_letters = _queues(template)

    for resource in template.find_resources("AWS::CloudWatch::Alarm").values():
        props = resource["Properties"]
        assert props["ComparisonOperator"] == "GreaterThanThreshold"
        assert props["Threshold"] == 0
        assert props["EvaluationPeriods"] == 1
        assert props["Statistic"] == "Maximum"
        assert props["Namespace"] == "AWS/SQS"
        assert props["MetricName"] == "ApproximateNumberOfMessagesVisible"
        assert props["TreatMissingData"] == "notBreaching"
        queue_id = props["Dimensions"][0]["Value"]["Fn::GetAtt"][0]
        assert props["Dimensions"][0]["Name"] == "QueueName"
        assert queue_id in dead_letters


def test_body_filter_sets_message_body_scope():
    app = cdk.App()
    stack = cdk.Stack(app, "Test")
    fanout = FilteredFanout(stack, "IngestComplete")
    fanout.add_consumer(
        "lost-production",
        filter={"signal_types": ["Gas Today"]},
        filter_scope=FilterScope.MESSAGE_BODY,
    )
    template = assertions.Template.from_stack(stack)
    template.has_resource_properties(
        "AWS::SNS::Subscription",
        {
            "FilterPolicy": {"signal_types": ["Gas Today"]},
            "FilterPolicyScope": "MessageBody",
            "RawMessageDelivery": True,
        },
    )


def test_worker_reports_batch_item_failures():
    stack, _, lost, _ = _stack()
    fn = lambda_.Function(
        stack,
        "LostProduction",
        runtime=lambda_.Runtime.PYTHON_3_12,
        handler="index.handler",
        code=lambda_.Code.from_inline(
            "def handler(event, context):\n    return {'batchItemFailures': []}\n"
        ),
    )
    lost.add_worker(fn)
    template = assertions.Template.from_stack(stack)

    consumers, dead_letters = _queues(template)
    mappings = template.find_resources("AWS::Lambda::EventSourceMapping")
    assert len(mappings) == 1
    props = next(iter(mappings.values()))["Properties"]
    assert props["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert props["BatchSize"] == 10
    source = props["EventSourceArn"]["Fn::GetAtt"][0]
    assert source in consumers
    assert source not in dead_letters
