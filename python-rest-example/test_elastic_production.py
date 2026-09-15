"""Narrow unit tests for the Elastic REST example's pure logic.

These tests avoid any network access: they exercise batching, backoff
math, Retry-After parsing, JWT claim construction, and endpoint URL
selection using an in-memory RSA key and a stubbed HTTP response.
"""

import os
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(__file__))

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

import elastic_production as rest_example


def _write_test_private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    fd, path = tempfile.mkstemp(suffix=".p8")
    with os.fdopen(fd, "wb") as f:
        f.write(pem)
    return path


class RowBatchTests(unittest.TestCase):
    def test_add_and_ndjson(self):
        batch = rest_example.RowBatch()
        batch.add(b'{"a":1}')
        batch.add(b'{"a":2}')
        self.assertEqual(batch.to_ndjson_bytes(), b'{"a":1}\n{"a":2}\n')
        self.assertFalse(batch.is_empty())

    def test_empty_batch(self):
        batch = rest_example.RowBatch()
        self.assertTrue(batch.is_empty())


class BackoffTests(unittest.TestCase):
    def test_backoff_is_bounded_and_grows(self):
        for attempt in range(10):
            delay = rest_example.compute_backoff_seconds(attempt)
            self.assertGreaterEqual(delay, 0)
            self.assertLessEqual(delay, rest_example.MAX_BACKOFF_SECONDS)

    def test_backoff_honors_retry_after(self):
        delay = rest_example.compute_backoff_seconds(5, retry_after=2.0)
        self.assertEqual(delay, 2.0)

    def test_backoff_caps_large_retry_after(self):
        delay = rest_example.compute_backoff_seconds(0, retry_after=10_000.0)
        self.assertEqual(delay, rest_example.MAX_BACKOFF_SECONDS)

    def test_parse_retry_after_missing(self):
        response = SimpleNamespace(headers={})
        self.assertIsNone(rest_example.parse_retry_after(response))

    def test_parse_retry_after_numeric(self):
        response = SimpleNamespace(headers={"Retry-After": "3"})
        self.assertEqual(rest_example.parse_retry_after(response), 3.0)

    def test_parse_retry_after_http_date(self):
        response = SimpleNamespace(headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
        self.assertGreaterEqual(rest_example.parse_retry_after(response), 0)

    def test_parse_retry_after_invalid(self):
        response = SimpleNamespace(headers={"Retry-After": "not-a-date"})
        self.assertIsNone(rest_example.parse_retry_after(response))


class TokenProviderJwtTests(unittest.TestCase):
    def setUp(self):
        self.key_path = _write_test_private_key()
        self.provider = rest_example.TokenProvider(
            account="my_org-my_account",
            user="my_user",
            private_key_file=self.key_path,
        )

    def tearDown(self):
        os.remove(self.key_path)

    def test_generates_jwt_with_expected_claims(self):
        token = self.provider._generate_jwt()
        import jwt as pyjwt

        claims = pyjwt.decode(token, options={"verify_signature": False})
        self.assertIn("MY_ORG-MY_ACCOUNT.MY_USER.SHA256:", claims["iss"])
        self.assertEqual(claims["sub"], "MY_ORG-MY_ACCOUNT.MY_USER")
        self.assertIn("exp", claims)
        self.assertIn("iat", claims)

    def test_valid_jwt_reuses_cached_token_until_near_expiry(self):
        first = self.provider._valid_jwt()
        second = self.provider._valid_jwt()
        self.assertEqual(first, second)


class EndpointUrlTests(unittest.TestCase):
    def _client(self, pipe=None):
        provider = SimpleNamespace()
        return rest_example.ElasticRestIngestClient(
            token_provider=provider,
            database="DB",
            schema="SCHEMA",
            table="TABLE",
            pipe=pipe,
        )

    def test_table_endpoint_default(self):
        client = self._client()
        url = client._endpoint_url("acct.ingest.snowflakecomputing.com")
        self.assertEqual(
            url,
            "https://acct.ingest.snowflakecomputing.com/v2/streaming/data/databases/DB"
            "/schemas/SCHEMA/tables/TABLE/rows",
        )

    def test_pipe_endpoint_when_configured(self):
        client = self._client(pipe="MY_PIPE")
        url = client._endpoint_url("acct.ingest.snowflakecomputing.com")
        self.assertEqual(
            url,
            "https://acct.ingest.snowflakecomputing.com/v2/streaming/data/databases/DB"
            "/schemas/SCHEMA/pipes/MY_PIPE/channels/ELASTIC/rows",
        )


class SubmitRowBatchingTests(unittest.TestCase):
    def test_submit_row_flushes_when_batch_full(self):
        provider = SimpleNamespace()
        client = rest_example.ElasticRestIngestClient(
            token_provider=provider,
            database="DB",
            schema="SCHEMA",
            table="TABLE",
            batch_max_rows=2,
        )
        dispatched = []
        client._dispatch = lambda batch: dispatched.append(batch)

        client.submit_row({"c1": 1}, "id-1")
        client.submit_row({"c1": 2}, "id-2")
        self.assertEqual(len(dispatched), 0)  # not flushed until a 3rd row arrives

        client.submit_row({"c1": 3}, "id-3")
        self.assertEqual(len(dispatched), 1)
        self.assertEqual(len(dispatched[0].rows), 2)

        client.flush()
        self.assertEqual(len(dispatched), 2)
        self.assertEqual(len(dispatched[1].rows), 1)


if __name__ == "__main__":
    unittest.main()
