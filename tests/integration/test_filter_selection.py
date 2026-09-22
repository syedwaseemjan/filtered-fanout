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
