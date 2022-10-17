import logging
import logging.config

logging.config.fileConfig('logging.conf')

from delsitelinks.tasks import main_power_touch

main_power_touch()