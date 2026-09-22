"""Prove the subscription filter selects messages.

Moto's SNS filter support is incomplete, especially for body scope.
This test runs against LocalStack. A green moto run is not the proof.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
import uuid

import pytest

pytestmark = pytest.mark.integration

EMAIL = "email"
SMS = "sms"


@pytest.fixture(scope="module")
def localstack_url():
    configured = os.environ.get("LOCALSTACK_URL")
    if configured:
        endpoint = configured.rstrip("/")
        _wait_until_ready(endpoint)
        yield endpoint
        return

    localstack = pytest.importorskip("testcontainers.community.localstack")
    container = localstack.LocalStackContainer(image="localstack/localstack:4")
    container.with_services("sns", "sqs")
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"could not start LocalStack: {exc}")
    endpoint = container.get_url().rstrip("/")
    try:
        _wait_until_ready(endpoint)
        yield endpoint
    finally:
        container.stop()


def test_filter_delivers_email_sms_and_both(localstack_url: str):
    boto3 = pytest.importorskip("boto3")
    suffix = uuid.uuid4().hex[:8]
    sns = _client(boto3, "sns", localstack_url)
    sqs = _client(boto3, "sqs", localstack_url)

    topic_arn = sns.create_topic(Name=f"orders-{suffix}")["TopicArn"]
    email_url = sqs.create_queue(QueueName=f"email-{suffix}")["QueueUrl"]
    sms_url = sqs.create_queue(QueueName=f"sms-{suffix}")["QueueUrl"]
    email_arn = sqs.get_queue_attributes(
        QueueUrl=email_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    sms_arn = sqs.get_queue_attributes(
        QueueUrl=sms_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    try:
        _allow_topic(sqs, email_url, email_arn, topic_arn)
        _allow_topic(sqs, sms_url, sms_arn, topic_arn)
        _subscribe(sns, topic_arn, email_arn, [EMAIL])
        _subscribe(sns, topic_arn, sms_arn, [SMS])

        _publish(sns, topic_arn, "email-only", [EMAIL])
        _publish(sns, topic_arn, "sms-only", [SMS])
        _publish(sns, topic_arn, "both", [EMAIL, SMS])
        _publish(sns, topic_arn, "neither", ["fax"])

        found = _collect(
            sqs,
            {"email": email_url, "sms": sms_url},
        )
        assert found["email"] == {"email-only", "both"}
        assert found["sms"] == {"sms-only", "both"}
    finally:
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=email_url)
        sqs.delete_queue(QueueUrl=sms_url)


def _client(boto3, service: str, endpoint: str):
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _allow_topic(sqs, queue_url: str, queue_arn: str, topic_arn: str) -> None:
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "Policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "sns.amazonaws.com"},
                            "Action": "sqs:SendMessage",
                            "Resource": queue_arn,
                            "Condition": {
                                "ArnEquals": {"aws:SourceArn": topic_arn}
                            },
                        }
                    ],
                }
            )
        },
    )


def _subscribe(sns, topic_arn: str, queue_arn: str, allowed: list[str]) -> None:
    arn = sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        ReturnSubscriptionArn=True,
    )["SubscriptionArn"]
    sns.set_subscription_attributes(
        SubscriptionArn=arn,
        AttributeName="RawMessageDelivery",
        AttributeValue="true",
    )
    sns.set_subscription_attributes(
        SubscriptionArn=arn,
        AttributeName="FilterPolicyScope",
        AttributeValue="MessageAttributes",
    )
    sns.set_subscription_attributes(
        SubscriptionArn=arn,
        AttributeName="FilterPolicy",
        AttributeValue=json.dumps({"channel": allowed}),
    )


def _publish(sns, topic_arn: str, message_id: str, channels: list[str]) -> None:
    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps({"id": message_id}),
        MessageAttributes={
            "channel": {
                "DataType": "String.Array",
                "StringValue": json.dumps(channels),
            }
        },
    )


def _collect(sqs, queues: dict[str, str], timeout: float = 30) -> dict[str, set[str]]:
    found = {name: set() for name in queues}
    deadline = time.monotonic() + timeout
    expected = {
        "email": {"email-only", "both"},
        "sms": {"sms-only", "both"},
    }
    while time.monotonic() < deadline:
        _drain_once(sqs, queues, found)
        if found == expected:
            # One more beat, so a message that should have been dropped can still show up.
            time.sleep(1)
            _drain_once(sqs, queues, found)
            break
    return found


def _drain_once(sqs, queues: dict[str, str], found: dict[str, set[str]]) -> None:
    for name, url in queues.items():
        response = sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        for message in response.get("Messages", []):
            assert "TopicArn" not in message["Body"]
            body = json.loads(message["Body"])
            assert "id" in body, message["Body"]
            found[name].add(body["id"])
            sqs.delete_message(
                QueueUrl=url,
                ReceiptHandle=message["ReceiptHandle"],
            )


def _wait_until_ready(endpoint: str) -> None:
    deadline = time.monotonic() + 90
    health = endpoint + "/_localstack/health"
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health, timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"LocalStack did not become ready at {endpoint}") from last_error
