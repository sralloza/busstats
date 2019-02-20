import argparse
import logging
import platform
from datetime import datetime, timedelta

from rpi.connections import Connections
from rpi.custom_logging import configure_logging
from rpi.managers.users_manager import UsersManager

from busdatagenerator import analyse_stop

CHOICES = ('GAMAZO', 'CLINICO')

if platform.system() == 'Linux':
    LINUX = True
    configure_logging(filename='/home/pi/busstats/busstats.log')

else:
    LINUX = False
    configure_logging(name='busstats', filename='D:/.scripts/busstats/busstats.log')


if __name__ == '__main__':
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser('BusWarner')
    parser.add_argument('choice', choices=CHOICES)
    parser.add_argument('notify', nargs='+', choices=UsersManager().usernames)

    opt = parser.parse_args()
    logger.debug('Options: %r', opt)

    if opt.choice == 'GAMAZO':
        data = analyse_stop(stop_number=686, lines=2)
    elif opt.choice == 'CLINICO':
        data = analyse_stop(stop_number=833, lines=(2, 8))
    else:
        raise RuntimeError(f'Invalid option {opt.choice!r}')

    message = ''
    for register in data:
        register_datetieme = datetime.strptime(register.actual_datetime, '%Y-%m-%d %H:%M:%S')
        arrival_time = (register_datetieme + timedelta(seconds=register.delay_minutes * 60)).time()
        message += f'{register.line} llegará a las {arrival_time.strftime("%H:%M")} ' \
            f'({register.delay_minutes} mins)\n'

    message = message.strip()
    Connections.notify(f'BusWarner - {opt.choice.capitalize()}', message, destinations=opt.notify,
                       force=True)
