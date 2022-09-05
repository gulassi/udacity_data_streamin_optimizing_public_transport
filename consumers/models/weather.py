"""Contains functionality related to Weather"""
import logging
from enum import IntEnum


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        value = message.value()
        logger.debug(f"temp: {int(round(value['temperature'], 0))}, status: {value['status']}")
        self.temperature = int(round(value["temperature"], 0))
        self.status = value['status']
