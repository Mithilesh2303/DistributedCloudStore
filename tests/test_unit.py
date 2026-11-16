import hashlib
import os
import shutil
import tempfile
import time
import unittest

from controller.src.database import Database
from worker.src.storage import StorageEngine


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_meta.db")
        self.db = Database(self.db_path)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_worker_registration_and_heartbeat(self):
        # Register worker
        w = self.db.register_worker("worker1", "worker1", 8001)
        self.assertEqual(w["worker_id"], "worker1")
        self.assertEqual(w["status"], "HEALTHY")

        # Fetch worker
        fetched = self.db.get_worker("worker1")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["host"], "worker1")

        # Heartbeat
        time.sleep(0.01)
        updated = self.db.update_heartbeat("worker1")
        self.assertTrue(updated)

        # Non-existent worker heartbeat
        not_updated = self.db.update_heartbeat("nonexistent")
        self.assertFalse(not_updated)

    def test_object_and_chunk_lifecycle(self):
        obj_id = "test-obj-123"
        self.db.register_worker("worker1", "worker1", 8001)
        self.db.register_worker("worker2", "worker2", 8001)

        # Create object
        obj = self.db.create_object(
            object_id=obj_id,
            name="test.txt",
            size=2000,
            content_type="text/plain",
            created_at="2026-11-16T12:00:00Z",
            checksum="dummy_checksum",
            total_chunks=2,
            custom_metadata={"author": "test_user"},
        )
        self.assertEqual(obj["object_id"], obj_id)
        self.assertEqual(obj["custom_metadata"]["author"], "test_user")

        # Add chunks
        self.db.add_chunk(
            chunk_id=f"{obj_id}_chunk_0",
            object_id=obj_id,
            chunk_index=0,
            size=1000,
            checksum="hash0",
            replicas=["worker1", "worker2"],
        )
        self.db.add_chunk(
            chunk_id=f"{obj_id}_chunk_1",
            object_id=obj_id,
            chunk_index=1,
            size=1000,
            checksum="hash1",
            replicas=["worker1", "worker2"],
        )

        # Retrieve chunks
        chunks = self.db.get_chunks_for_object(obj_id)
        self.assertEqual(len(chunks), 2)
        self.assertIn("worker1", chunks[0]["replicas"])
        self.assertIn("worker2", chunks[0]["replicas"])

        # Update metadata
        updated = self.db.update_object_metadata(
            obj_id, {"env": "prod", "author": "mithilesh"}, "2026-11-16T12:05:00Z"
        )
        self.assertEqual(updated["custom_metadata"]["env"], "prod")
        self.assertEqual(updated["custom_metadata"]["author"], "mithilesh")

        # List objects
        obj_list, total = self.db.list_objects(limit=10, offset=0)
        self.assertEqual(total, 1)
        self.assertEqual(len(obj_list), 1)

        # Worker chunk counts
        counts = self.db.get_worker_chunk_counts()
        self.assertEqual(counts.get("worker1"), 2)
        self.assertEqual(counts.get("worker2"), 2)

        # Delete object (cascades to chunks and replicas)
        deleted = self.db.delete_object(obj_id)
        self.assertIsNotNone(deleted)
        self.assertIsNone(self.db.get_object(obj_id))
        self.assertEqual(len(self.db.get_chunks_for_object(obj_id)), 0)


class TestStorageEngine(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.storage = StorageEngine(self.temp_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_chunk_storage_and_retrieval(self):
        chunk_id = "test_chunk_0"
        data = b"Hello, Distributed Cloud Object Store!"
        written = self.storage.store_chunk(chunk_id, data)
        self.assertEqual(written, len(data))

        # Retrieve
        retrieved = self.storage.get_chunk(chunk_id)
        self.assertEqual(retrieved, data)

        # Check hash
        self.assertEqual(hashlib.sha256(retrieved).hexdigest(), hashlib.sha256(data).hexdigest())

        # List
        chunk_list = self.storage.list_chunks()
        self.assertEqual(len(chunk_list), 1)
        self.assertEqual(chunk_list[0]["chunk_id"], chunk_id)
        self.assertEqual(chunk_list[0]["size"], len(data))

        # Delete
        self.assertTrue(self.storage.delete_chunk(chunk_id))
        self.assertIsNone(self.storage.get_chunk(chunk_id))
        self.assertEqual(len(self.storage.list_chunks()), 0)


if __name__ == "__main__":
    unittest.main()
