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
