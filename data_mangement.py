import datetime as dt
from typing import Callable

from input_interface import DBConnection


class DataManager(list):
    def __init__(self, line, stop_id=833, n=None):
        with DBConnection() as c:
            super().__init__(c.get_data(line=line, stop_id=stop_id, n=n))

    def __add__(self, other):
        assert isinstance(other, DataManager), TypeError
        new = DataManager.__new__(DataManager)
        super(DataManager, new).__init__(list(self)+list(other))
        return new

    def __repr__(self):
        return 'DataManager(\n' + ',\n'.join([repr(x) for x in self]) + ')'

    def filter_times(self, time1: dt.time, time2: dt.time):
        """Deletes every register if its time is not between time1 and time2.

        Args:
            time1 (datetime.date): lower filter.
            time2 (datetime.date): upper filter.
        """
        o = []
        for register in self:
            if time1 <= register.time <= time2:
                o.append(register)

        super().__init__(o)

    def group(self, epsilon: int = 2, selector: Callable = max):
        """Groups the registers according to its time. Due to grouping problems, it will also
        filter registers by line and stop_id. The default stop_id is 833, which is the id of the
        hospital's bus stop.

        Args:
            epsilon (int): maximum time difference between registers of the same group.
            selector (Callable): funtion that selects which of the registers in the group will
                remain.

        Returns:
            DataManager
        """

        self.sort(key=lambda x: (x.date, x.time))

        groups = []
        temp_group = []
        i = 0

        while i < len(self):
            k = i
            while self[i].distance(self[k]) < epsilon:
                temp_group.append(self[k])
                k += 1
                if k == len(self):
                    break

            i = k
            groups.append(temp_group)
            temp_group = []

        super().__init__([selector(x) for x in groups])
        self.sort(key=lambda x: (x.date, x.time))
