"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)
table = app.Table(
   "stations",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
   value_type=TransformedStation
)


@app.agent(topic)
async def station_agent(stream):
    async for value in stream:
        ts = {
            "station_id": value.station_id,
            "station_name": value.station_name,
            "order": value.order,
            "line": ""
        }
        if value.red:
            ts['line'] = "red"
        elif value.blue:
            ts['line'] = "blue"
        elif value.green:
            ts['line'] = "green"
        table[value.station_id] = ts


if __name__ == "__main__":
    app.main()
