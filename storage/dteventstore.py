import os


class DatetimeEventStore(object):
    """
    This class allows to store and retrieve time based events stored
    in the filesystem.
    """

    def __init__(self, storage_folder=None):
        self.init_storage(storage_folder)

    def init_storage(self, storage_folder=None):
        if storage_folder is None:
            storage_folder = os.path.join(os.path.expanduser("~"), "dts")

        if not os.path.exists(storage_folder):
            os.makedirs(storage_folder)

        self.storage_folder = storage_folder

    def store_event(self, at=None, data=None):
        pass

    def get_events(self, start=None, end=None):
        return []
