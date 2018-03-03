import datetime
import errno
import os


class DatetimeEventStore(object):
    """
    This class allows to store and retrieve time based events stored
    in the filesystem.
    """

    def __init__(self, storage_folder=None):
        self.storage_folder = storage_folder
        self.init_storage()

    def init_storage(self):
        if self.storage_folder is None:
            self.storage_folder = os.path.join(os.path.expanduser("~"), "dts")

        if not os.path.exists(self.storage_folder):
            os.makedirs(self.storage_folder)

    def store_event(self, at=None, data=None):
        if at is None:
            at = datetime.datetime.now()
        target = os.path.join(
            self.storage_folder,
            at.strftime("%Y"),
            at.strftime("%m"),
            at.strftime("%d"),
            at.strftime("%H:%M:%S"),
        )
        try:
            os.makedirs(os.path.dirname(target))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            # Else this just means the folder already exists
        idx = 1
        while os.path.exists(
            "{}-{:0>2d}".format(target, idx)
        ):
            idx += 1
        if idx == 100:
            raise RuntimeError("Too many events at {}".format(at))

        event_path = "{}-{:0>2d}".format(target, idx)
        with open(event_path, "w") as fp:
            fp.write(data)
        return event_path

    def get_events(self, start=None, end=None):
        return []
