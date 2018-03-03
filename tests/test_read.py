import datetime
import os
import shutil
import tempfile
import unittest
from storage import DatetimeEventStore


class TestRead(unittest.TestCase):

    def setUp(self):
        self.tmpfolder = tempfile.mkdtemp()
        self.folder = os.path.join(self.tmpfolder, "dts")
        self.dts = DatetimeEventStore(storage_folder=self.folder)

    def test_init_storage(self):
        self.assertTrue(os.path.exists(self.folder))

    def test_read_one(self):
        target = os.path.join(self.folder, "1980", "05", "25", "00:15:00-01")
        os.makedirs(os.path.dirname(target))
        with open(target, "w") as fp:
            fp.write("Birth")

        events = list(self.dts.get_events(
            start=datetime.datetime(1980, 1, 1),
            end=datetime.datetime(1980, 12, 31),
        ))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0], "Birth")

    def test_read_many(self):
        at = datetime.datetime(2018, 3, 3, 19, 0)
        os.makedirs(os.path.join(
            self.folder, "2018", "03", "03",
        ))
        for idx in range(10):
            data = str(idx)
            target = os.path.join(
                self.folder, "2018", "03", "03",
                "19:00:00-{:0>2d}".format(idx + 1),
            )
            with open(target, "w") as fp:
                fp.write(data)

        events = list(self.dts.get_events(
            start=at,
            end=datetime.datetime(2018, 3, 3, 19, 0, 1),
        ))
        self.assertEqual(len(events), 10)
        self.assertTrue(all([
            str(idx) in events
            for idx in range(10)
        ]))

    def tearDown(self):
        shutil.rmtree(self.tmpfolder)
