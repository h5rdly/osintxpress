import unittest
import polars as pl
import json
from osintxpress import OsintEngine, SourceAdapter


class TestTelegramParser(unittest.TestCase):

    def test_parses_standard_message(self):

        engine = OsintEngine(worker_threads=1)

        # Let the Engine knows which Parser to use. we don't call engine.start_all()
        engine.add_telegram_source("tg_test", "mock_channel", 1, "mock_hash", "mock_session")
        
        raw_json = json.dumps({
            "message_id": 999,
            "channel": "test_channel",
            "text": "Missile launch detected.",
            "date": 1710760000,
            "media": "path/to/photo.jpg"
        })

        engine.inject_test_data("tg_test", [raw_json])
        
        data = engine.poll()
        
        self.assertIn("tg_test", data)
        df = pl.from_arrow(data["tg_test"])
        self.assertEqual(len(df), 1)
        self.assertEqual(df["message_id"][0], 999)
        self.assertEqual(df["channel"][0], "test_channel")
        self.assertEqual(df["text"][0], "Missile launch detected.")
        self.assertEqual(df["media"][0], "path/to/photo.jpg")


if __name__ == '__main__':

    unittest.main()