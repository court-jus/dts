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
        if start is None:
            start = datetime.datetime.min
        if end is None:
            end = datetime.datetime.now()
        if start >= end:
            return []
        for year in range(start.year, end.year + 1):
            for event in self.get_year_events(year, start, end):
                yield event

    def get_year_events(self, year, start, end):
        start_month = start.month if start.year == year else 1
        end_month = end.month if end.year == year else 12
        for month in range(start_month, end_month + 1):
            month_folder = os.path.join(
                self.storage_folder,
                "{:0>4d}".format(year),
                "{:0>2d}".format(month),
            )
            if os.path.exists(month_folder):
                for name, folders, files in os.walk(month_folder):
                    for filename in files:
                        event = self.event_from_file(os.path.join(name, filename))
                        if event["at"] >= start and event["at"] <= end:
                            yield event["data"]

    def event_from_file(self, filename):
        if not os.path.exists(filename):
            return None
        if not filename.startswith(self.storage_folder):
            raise ValueError("This filename is not part of the storage")
        with open(filename, "r") as fp:
            data = fp.read()
        at = filename[len(self.storage_folder):].split("-")[0]
        if at[0] == "/":
            at = at[1:]
        at = datetime.datetime.strptime(at, "%Y/%m/%d/%H:%M:%S")
        return {
            "at": at,
            "data": data,
        }
