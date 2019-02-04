import datetime as dt
import os
from sqlite3 import OperationalError

import pytest

from input_interface import BSRegister, DBConnection

DATABASE_PATH = 'D:/.database/sql/busstats.sqlite'

def test_dbregister():
    one_day = dt.datetime(2019, 2, 4, 12, 15, 3)
    other = one_day.replace(hour=one_day.hour - 1, minute=one_day.minute + 1)

    reg1 = BSRegister(1, dt.datetime.min)
    reg2 = BSRegister(2, one_day)
    reg3 = BSRegister(3, other)
    reg4 = BSRegister(4, dt.datetime.max)

    # noinspection PyDataclass
    assert reg1 < reg2 < reg4

    assert reg2.hours == one_day.hour + one_day.minute / 60
    assert reg1.hours == dt.datetime.min.hour + dt.datetime.min.minute / 60
    assert reg4.hours == dt.datetime.max.hour + dt.datetime.max.minute / 60

    assert reg1.distance(reg4) == 5258964959.0
    assert reg4.distance(reg1) == 5258964959.0
    assert reg2.distance(reg3) == 59.0
    assert reg3.distance(reg2) == 59.0
    assert reg1.distance(reg1) == 0
    with pytest.raises(AssertionError):
        assert reg1.distance('not-a-register')

    assert repr(reg1) == "BSRegister(line='1', date='0001-01-01', time='00:00:00')"
    assert repr(reg2) == "BSRegister(line='2', date='2019-02-04', time='12:15:00')"
    assert repr(reg3) == "BSRegister(line='3', date='2019-02-04', time='11:16:00')"
    assert repr(reg4) == "BSRegister(line='4', date='9999-12-31', time='23:59:00')"


def test_db_connection():
    with pytest.raises(OperationalError):
        with DBConnection(path='peter.class') as c:
            (c.get_data(9))

    os.remove('peter.class')

    with DBConnection() as c:
        c1 = c.get_data(2)
        c2 = c.get_data(2, n=20)

    with DBConnection(path=DATABASE_PATH) as c:
        c3 = c.get_data(2)

    assert len(c1) > 0
    assert len(c2) > 0
    assert len(c2) == 20
    assert len(c1) == len(c3)
    assert isinstance(c1, tuple)
    assert isinstance(c2, tuple)


if __name__ == '__main__':
    # pytest.main()
    test_db_connection()
