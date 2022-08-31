"""Methods pertaining to weather data"""
from enum import IntEnum
from datetime import date, datetime
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        super().__init__(
            "weather_report",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=6, # For performance 6 partition should be enough, weather updates happen quite seldom. Can be increased if needed
            num_replicas=2, # There are three Kafka broker instances, so replication factor should be lower and at least 2 replicas are needed for high availability
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        logger.info("sending weather update")
        resp = requests.post(
           f"{Weather.rest_proxy_url}/topics/{Weather.topic_name}",
           headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
           data=json.dumps(
               {
                   "key_schema": Weather.key_schema,
                   "value_schema": Weather.value_schema,
                    "records": [
                        {
                            "key": datetime.timestamp(),
                            "value": {
                                "temperature": Weather.temp,
                                "status": Weather.status
                            }
                        }
                    ]
               }
           ),
        )
        resp.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
