import unittest
from unittest.mock import MagicMock, patch
import json
from kafkainspect import hash_payload, get_field_from_json, main

# Mock Message class to simulate Kafka messages
class MockMessage:
    def __init__(self, key, value, offset, partition=0, ts=1660000000):
        self._key = key
        self._value = value
        self._offset = offset
        self._partition = partition
        self._ts = ts

    def key(self):
        return self._key

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def timestamp(self):
        return (1, self._ts) # type, timestamp

    def error(self):
        return None

class TestKafkaInspectHelpers(unittest.TestCase):

    def test_hash_payload(self):
        """Tests that the hashing function returns the correct SHA256 hash."""
        payload = b'hello world'
        expected_hash = 'b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9'
        self.assertEqual(hash_payload(payload), expected_hash)

    def test_get_field_from_json_simple(self):
        """Tests extracting a top-level field from a JSON object."""
        payload = b'{"user": "test", "id": 123}'
        field_path = 'id'
        expected_value = b'123'
        self.assertEqual(get_field_from_json(payload, field_path), expected_value)

    def test_get_field_from_json_nested(self):
        """Tests extracting a nested field from a JSON object."""
        payload = b'{"user": {"name": "test", "details": {"id": 456}}}'
        field_path = 'user.details.id'
        expected_value = b'456'
        self.assertEqual(get_field_from_json(payload, field_path), expected_value)

    def test_get_field_from_json_nonexistent(self):
        """Tests extracting a field that does not exist."""
        payload = b'{"user": {"name": "test"}}'
        field_path = 'user.id'
        self.assertIsNone(get_field_from_json(payload, field_path))

    def test_get_field_from_json_invalid_json(self):
        """Tests behavior with an invalid JSON string."""
        payload = b'this is not json'
        field_path = 'user.id'
        self.assertIsNone(get_field_from_json(payload, field_path))

    def test_get_field_from_json_path_too_deep(self):
        """Tests a path that is deeper than the object structure."""
        payload = b'{"user": "test"}'
        field_path = 'user.id.more'
        self.assertIsNone(get_field_from_json(payload, field_path))
        
    def test_get_field_from_json_object_as_value(self):
        """Tests extracting a field whose value is a JSON object."""
        payload = b'{"data": {"id": 1, "value": "test"}}'
        field_path = 'data'
        # The extracted object should be re-serialized consistently
        expected_payload = json.dumps({"id": 1, "value": "test"}, sort_keys=True).encode('utf-8')
        self.assertEqual(get_field_from_json(payload, field_path), expected_payload)

class TestKafkaInspectE2E(unittest.TestCase):

    def _create_mock_poll(self, messages):
        """Creates a poll function that simulates consumer behavior."""
        def mock_poll(timeout):
            if messages:
                return messages.pop(0)
            return None
        return mock_poll

    @patch('kafkainspect.Consumer')
    def test_deduplication(self, MockConsumer):
        """Tests the end-to-end deduplication logic with a mock consumer."""
        mock_consumer_instance = MockConsumer.return_value
        messages = [
            MockMessage(b'k1', b'value1', 1),
            MockMessage(b'k2', b'value2', 2),
            MockMessage(b'k1', b'value1', 3), # Duplicate
        ]
        mock_consumer_instance.poll.side_effect = self._create_mock_poll(messages)

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test', '--dedup-by', 'value', '--max-messages', '5']
        with patch('sys.argv', argv):
            main()
            mock_consumer_instance.close.assert_called_once()

    @patch('kafkainspect.Consumer')
    def test_search_messages(self, MockConsumer):
        """Tests the message search functionality."""
        mock_consumer_instance = MockConsumer.return_value
        messages = [
            MockMessage(b'k1', b'hello world', 1),
            MockMessage(b'k2', b'another message', 2),
            MockMessage(b'k3', b'hello again', 3),
        ]
        mock_consumer_instance.poll.side_effect = self._create_mock_poll(messages)

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test', '--search', 'hello', '--max-messages', '5']
        with patch('sys.argv', argv):
            main()
            mock_consumer_instance.close.assert_called_once()

    @patch('kafkainspect.Consumer')
    def test_peek_messages(self, MockConsumer):
        """Tests the peek functionality."""
        mock_consumer_instance = MockConsumer.return_value
        messages = [
            MockMessage(b'k1', b'msg1', 1),
            MockMessage(b'k2', b'msg2', 2),
            MockMessage(b'k3', b'msg3', 3),
        ]
        mock_consumer_instance.poll.side_effect = self._create_mock_poll(messages)

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test', '--peek', '2']
        with patch('sys.argv', argv):
            main()
            mock_consumer_instance.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
