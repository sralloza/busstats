import datetime as dt
import sqlite3
from dataclasses import dataclass, field
from typing import Union

DATABASE_PATH = 'D:/.database/sql/busstats.sqlite'


@dataclass
class BSRegister:
    """Reprsentation of a Bus Stats Register."""
    line: Union[str, int]
    _datetime: dt.datetime
    date: dt.date = field(init=False)
    time: dt.time = field(init=False)

    def __lt__(self, other):
        assert isinstance(other, BSRegister), TypeError
        return self._datetime < other._datetime

    def __post_init__(self):
        self.line = str(self.line)

        if isinstance(self._datetime, str):
            self._datetime = dt.datetime.strptime(self._datetime, '%Y-%m-%d %H:%M:%S')

        self._datetime = self._datetime.replace(second=0, microsecond=0)

        self.date = self._datetime.date()
        self.time = self._datetime.time()

    @property
    def hours(self):
        """Returns the time in hours."""
        return self.time.hour + self.time.minute / 60

    def distance(self, other):
        """Returns the time difference betwen self and other in minutes"""
        assert isinstance(other, BSRegister), TypeError
        return abs(self._datetime - other._datetime).total_seconds() / 60

    def __repr__(self):
        return f"BSRegister(line={self.line!r}, date='{self.date}', time='{self.time}')"


class DBConnection:
    """Handles the connection with the Bus Stats Database."""
    def __init__(self, path=None):
        self.con = sqlite3.connect(path or DATABASE_PATH)
        self.cur = self.con.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.commit()
        self.con.close()

    def get_data(self, line, stop_id=833, n=None):
        """Gets the bus stats data from the database.

        Args:
            line (str | int): bus line to filter the data.
            stop_id (int): bus stop identification to filter the data. Default is 833, corresponding
                to the hospital's stop.
            n (int): Maximum number of registers to retrieve. Set None to retrieve all registries.

        Returns:
            Tuple[BSRegister]
        """

        if not n:
            packed_data = (str(line), int(stop_id))
            self.cur.execute('select line, actual_datetime from busstats where'
                             ' delay_minutes=0 and line=? and stop_id=?', packed_data)
        else:
            packed_data = (str(line), int(stop_id), int(n))
            self.cur.execute('select line, actual_datetime from busstats where'
                             ' delay_minutes=0 and line=? and stop_id=? limit ?', packed_data)
        return tuple((BSRegister(*x) for x in self.cur.fetchall()))
