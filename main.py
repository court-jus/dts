import datetime
import random

from storage import DatetimeEventStore


store = DatetimeEventStore()

# Generate a bunch of events to be stored in a period of 20 years.
start_ts = datetime.datetime(2000, 1, 1).timestamp()
end_ts = datetime.datetime(2020, 1, 1).timestamp()

for i in range(10000):
    dt = datetime.datetime.fromtimestamp(
        random.randint(start_ts, end_ts)
    )
    store.store_event(at=dt, data="Event number %d." % i)

for event in store.get_events(
    start=datetime.datetime(year=2018, month=1, day=1),
    end=datetime.datetime(year=2018, month=2, day=1)
):
    print(event)

