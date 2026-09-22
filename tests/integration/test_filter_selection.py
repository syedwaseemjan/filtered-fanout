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

VOLUME = "Gas Today"
PRESSURE = "Tubing Pressure"


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


def test_filter_delivers_volume_pressure_and_both(localstack_url: str):
    boto3 = pytest.importorskip("boto3")
    suffix = uuid.uuid4().hex[:8]
    sns = _client(boto3, "sns", localstack_url)
    sqs = _client(boto3, "sqs", localstack_url)

    topic_arn = sns.create_topic(Name=f"ingest-{suffix}")["TopicArn"]
    volume_url = sqs.create_queue(QueueName=f"volume-{suffix}")["QueueUrl"]
    pressure_url = sqs.create_queue(QueueName=f"pressure-{suffix}")["QueueUrl"]
    volume_arn = sqs.get_queue_attributes(
        QueueUrl=volume_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    pressure_arn = sqs.get_queue_attributes(
        QueueUrl=pressure_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    try:
        _allow_topic(sqs, volume_url, volume_arn, topic_arn)
        _allow_topic(sqs, pressure_url, pressure_arn, topic_arn)
        _subscribe(sns, topic_arn, volume_arn, [VOLUME])
        _subscribe(sns, topic_arn, pressure_arn, [PRESSURE])

        _publish(sns, topic_arn, "volume-only", [VOLUME])
        _publish(sns, topic_arn, "pressure-only", [PRESSURE])
        _publish(sns, topic_arn, "both", [VOLUME, PRESSURE])
        _publish(sns, topic_arn, "neither", ["Casing Pressure"])

        found = _collect(
            sqs,
            {"volume": volume_url, "pressure": pressure_url},
        )
        assert found["volume"] == {"volume-only", "both"}
        assert found["pressure"] == {"pressure-only", "both"}
    finally:
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=volume_url)
        sqs.delete_queue(QueueUrl=pressure_url)


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
