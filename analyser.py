import datetime as dt

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np

from data_mangement import DataManager

LINE = 8

if __name__ == '__main__':
    dm: DataManager = DataManager(2) + DataManager(8)
    print(dm)
    dm.group()
    dm.filter_times(dt.time(8, 36), dt.time(8, 48))

    dates = [str(x.date) for x in dm]
    times = [x.hours for x in dm]

    mdates.datestr2num(dates)

    fig, ax = plt.subplots()
    fig.autofmt_xdate()

    ax.scatter(dates, times, marker='*', c='red')
    ax.set_xlabel('Dates')
    ax.set_ylabel('Arrival times')
    ax.set_title(f'Arrival times for the hospital\'s bus stop (line {dm[0].line})')

    # for reg in dm:
    #     ax.text(str(reg.date), reg.hours, str(reg.time)[:-3])

    ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
    start, end = ax.get_xlim()
    ax.xaxis.set_ticks(np.arange(start, end, 2.5))

    plt.show()

    data_set = dict()
    for reg in dm:
        if reg.date not in data_set:
            data_set[reg.date] = reg.time
        else:
            if isinstance(data_set[reg.date], dt.time):
                data_set[reg.date] = [data_set[reg.date]]
            data_set[reg.date].append(reg.time)

    for key in data_set.keys():
        try:
            _ = len(data_set[key])
            print(f'{key}: {", ".join([str(x) for x in data_set[key]])}')
        except TypeError:
            pass

    mean = np.mean([x.hours for x in dm])
    print('Media:', mean)
