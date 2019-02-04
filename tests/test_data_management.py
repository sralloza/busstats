import datetime as dt

import pytest

from data_mangement import DataManager


def test_data_manager():
    dm1 = DataManager(2)
    dm2 = DataManager(8, n=20)
    dm3: DataManager = dm1 + dm2

    assert len(dm2) == 20
    assert repr(dm1).startswith('DataManager(') and repr(dm1).endswith(')')
    assert repr(dm2).startswith('DataManager(') and repr(dm2).endswith(')')
    assert len(dm3) == len(dm1) + len(dm2)

    len_1 = len(dm1)
    len_2 = len(dm2)

    dm1.filter_times(dt.time(8, 30), dt.time(9, 0))
    dm2.group()

    assert len(dm1) < len_1
    assert len(dm2) < len_2


if __name__ == '__main__':
    pytest.main()
    # test_data_manager()
