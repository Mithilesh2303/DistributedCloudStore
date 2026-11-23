import json
import os
import shutil
import tempfile
import time
import unittest

from journal_node.src.journal import JournalEngine
from standby_controller.src.database import StandbyDatabase
from standby_controller.src.checkpoint import FSImageManager


class TestHAAndJournaling(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.journal_dir = os.path.join(self.temp_dir, "journal")
        self.standby_db_path = os.path.join(self.temp_dir, "standby_meta.db")
        self.fsimage_dir = os.path.join(self.temp_dir, "fsimages")

        self.journal = JournalEngine(self.journal_dir)
        self.standby_db = StandbyDatabase(self.standby_db_path)
        self.fsimage_mgr = FSImageManager(self.fsimage_dir, [])

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_journal_engine_append_and_query(self):
        # 1. Append edits
        edits = [
            {
                "txid": 1,
                "op": "OP_REGISTER_WORKER",
                "timestamp": time.time(),
                "data": {"worker_id": "worker1", "host": "worker1", "port": 8001},
            },
            {
                "txid": 2,
                "op": "OP_CREATE_OBJECT",
                "timestamp": time.time(),
                "data": {
                    "object_id": "obj-1",
                    "name": "file.txt",
                    "size": 1024,
                    "content_type": "text/plain",
                    "created_at": "2026-11-23T00:00:00Z",
                    "checksum": "abc123hash",
                    "total_chunks": 1,
                    "custom_metadata": {"owner": "test"},
                },
            },
        ]
        latest_txid, added = self.journal.append_edits(edits)
        self.assertEqual(latest_txid, 2)
        self.assertEqual(added, 2)

        # 2. Query edits since txid 0
        fetched_all = self.journal.get_edits(since_txid=0)
        self.assertEqual(len(fetched_all), 2)
        self.assertEqual(fetched_all[0]["op"], "OP_REGISTER_WORKER")
        self.assertEqual(fetched_all[1]["op"], "OP_CREATE_OBJECT")

        # 3. Query edits since txid 1 (tailing new edits)
        fetched_new = self.journal.get_edits(since_txid=1)
        self.assertEqual(len(fetched_new), 1)
        self.assertEqual(fetched_new[0]["txid"], 2)

    def test_standby_database_apply_edits_and_query(self):
        # 1. Apply OP_REGISTER_WORKER
        self.standby_db.apply_edit(
            "OP_REGISTER_WORKER",
            {"worker_id": "w1", "host": "w1_host", "port": 8001},
        )
        worker = self.standby_db.get_worker("w1")
        self.assertIsNotNone(worker)
        self.assertEqual(worker["host"], "w1_host")

        # 2. Apply OP_CREATE_OBJECT
        self.standby_db.apply_edit(
            "OP_CREATE_OBJECT",
            {
                "object_id": "test-obj",
                "name": "photo.png",
                "size": 5000,
                "content_type": "image/png",
                "created_at": "2026-11-23T12:00:00Z",
                "checksum": "sha256photo",
                "total_chunks": 1,
                "custom_metadata": {"album": "vacation"},
            },
        )
        obj = self.standby_db.get_object("test-obj")
        self.assertIsNotNone(obj)
        self.assertEqual(obj["name"], "photo.png")
        self.assertEqual(obj["custom_metadata"]["album"], "vacation")

        # 3. Apply OP_ADD_CHUNK
        self.standby_db.apply_edit(
            "OP_ADD_CHUNK",
            {
                "chunk_id": "test-obj_chunk_0",
                "object_id": "test-obj",
                "chunk_index": 0,
                "size": 5000,
                "checksum": "sha256photo",
                "replicas": ["w1", "w2"],
            },
        )
        chunks = self.standby_db.get_chunks_for_object("test-obj")
        self.assertEqual(len(chunks), 1)
        self.assertIn("w1", chunks[0]["replicas"])
        self.assertIn("w2", chunks[0]["replicas"])

        # 4. Apply OP_UPDATE_METADATA
        self.standby_db.apply_edit(
            "OP_UPDATE_METADATA",
            {
                "object_id": "test-obj",
                "custom_metadata": {"album": "vacation_2026", "rating": 5},
                "modified_at": "2026-11-23T12:05:00Z",
            },
        )
        updated_obj = self.standby_db.get_object("test-obj")
        self.assertEqual(updated_obj["custom_metadata"]["rating"], 5)
        self.assertEqual(updated_obj["custom_metadata"]["album"], "vacation_2026")

        # 5. Apply OP_DELETE_OBJECT
        self.standby_db.apply_edit("OP_DELETE_OBJECT", {"object_id": "test-obj"})
        self.assertIsNone(self.standby_db.get_object("test-obj"))
        self.assertEqual(len(self.standby_db.get_chunks_for_object("test-obj")), 0)

    def test_fsimage_snapshot_and_restore(self):
        # 1. Populate DB
        self.standby_db.apply_edit(
            "OP_CREATE_OBJECT",
            {
                "object_id": "snap-obj",
                "name": "report.pdf",
                "size": 10000,
                "content_type": "application/pdf",
                "created_at": "2026-11-23T08:00:00Z",
                "checksum": "hashpdf123",
                "total_chunks": 1,
                "custom_metadata": {"department": "Cloud"},
            },
        )
        self.standby_db.apply_edit(
            "OP_ADD_CHUNK",
            {
                "chunk_id": "snap-obj_chunk_0",
                "object_id": "snap-obj",
                "chunk_index": 0,
                "size": 10000,
                "checksum": "hashpdf123",
                "replicas": ["worker1", "worker2"],
            },
        )

        # 2. Create FSImage snapshot
        snapshot = self.standby_db.create_snapshot()
        self.assertEqual(len(snapshot["objects"]), 1)
        self.assertEqual(len(snapshot["chunks"]), 1)
        self.assertEqual(len(snapshot["chunk_replicas"]), 2)

        # 3. Save FSImage to disk
        fsimage_path = self.fsimage_mgr.save_fsimage(checkpoint_txid=10, snapshot=snapshot)
        self.assertTrue(os.path.exists(fsimage_path))

        # 4. Load FSImage into a fresh Standby DB
        fresh_db_path = os.path.join(self.temp_dir, "fresh_standby.db")
        fresh_db = StandbyDatabase(fresh_db_path)
        loaded = self.fsimage_mgr.load_latest_local_fsimage()
        self.assertIsNotNone(loaded)
        txid, loaded_snapshot = loaded
        self.assertEqual(txid, 10)

        fresh_db.restore_from_snapshot(loaded_snapshot)
        restored_obj = fresh_db.get_object("snap-obj")
        self.assertIsNotNone(restored_obj)
        self.assertEqual(restored_obj["name"], "report.pdf")
        self.assertEqual(restored_obj["custom_metadata"]["department"], "Cloud")

        restored_chunks = fresh_db.get_chunks_for_object("snap-obj")
        self.assertEqual(len(restored_chunks), 1)
        self.assertEqual(set(restored_chunks[0]["replicas"]), {"worker1", "worker2"})


if __name__ == "__main__":
    unittest.main()

