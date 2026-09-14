"""Unit tests for the producer's data-contract helpers."""

import unittest

from producer.event_replayer import (
    REQUIRED_COLUMNS,
    clean_value,
    transform_row,
    validate_columns,
)


class EventReplayerTests(unittest.TestCase):
    def test_clean_value_normalizes_whitespace_and_missing_values(self):
        self.assertEqual(clean_value("  cart  "), "cart")
        self.assertIsNone(clean_value("   "))
        self.assertIsNone(clean_value(None))

    def test_validate_columns_rejects_missing_required_fields(self):
        with self.assertRaisesRegex(ValueError, "user_session"):
            validate_columns(sorted(REQUIRED_COLUMNS - {"user_session"}))

    def test_transform_row_keeps_contract_and_nulls_blank_optional_values(self):
        event = transform_row(
            {
                "event_time": " 2019-10-01 00:00:00 UTC ",
                "event_type": " purchase ",
                "product_id": "123",
                "category_id": "456",
                "category_code": "electronics.phone",
                "brand": " ",
                "price": "99.95",
                "user_id": "789",
                "user_session": "abc",
            }
        )

        self.assertEqual(set(event), REQUIRED_COLUMNS)
        self.assertEqual(event["event_type"], "purchase")
        self.assertIsNone(event["brand"])


if __name__ == "__main__":
    unittest.main()
