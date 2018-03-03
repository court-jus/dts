import datetime
import os
import shutil
import tempfile
import unittest
from storage import DatetimeEventStore


class TestWrite(unittest.TestCase):

    def setUp(self):
        self.tmpfolder = tempfile.mkdtemp()
        self.folder = os.path.join(self.tmpfolder, "dts")
        self.dts = DatetimeEventStore(storage_folder=self.folder)

    def test_init_storage(self):
        self.assertTrue(os.path.exists(self.folder))

    def test_write_one(self):
        self.dts.store_event(
            at=datetime.datetime(1980, 5, 25, 0, 15),
            data='Birth',
        )
        target = os.path.join(self.folder, "1980", "05", "25", "00:15:00-01")
        self.assertTrue(os.path.exists(target))
        with open(target, "r") as fp:
            data = fp.read()
        self.assertEqual(data, "Birth")

    def test_write_many(self):
        at = datetime.datetime(2018, 3, 3, 19, 0)
        for idx in range(10):
            data = str(idx)
            self.dts.store_event(at=at, data=data)
            target = os.path.join(
                self.folder, "2018", "03", "03",
                "19:00:00-{:0>2d}".format(idx + 1),
            )
            with open(target, "r") as fp:
                read_data = fp.read()
            self.assertEqual(read_data, data)

    def test_write_overflow(self):
        at = datetime.datetime(2018, 3, 3, 19, 0)
        for idx in range(99):
            data = str(idx)
            self.dts.store_event(at=at, data=data)
            target = os.path.join(
                self.folder, "2018", "03", "03",
                "19:00:00-{:0>2d}".format(idx + 1),
            )
            with open(target, "r") as fp:
                read_data = fp.read()
            self.assertEqual(read_data, data)

        data = "should overflow"
        with self.assertRaises(RuntimeError) as exc:
            self.dts.store_event(at=at, data=data)

    def tearDown(self):
        shutil.rmtree(self.tmpfolder)
