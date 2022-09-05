"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"
KSQL_STATEMENT = """
CREATE TABLE IF NOT EXISTS TURNSTILE (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    kafka_topic='TURNSTILE',
    value_format='avro',
    key='station_id'
);

CREATE TABLE IF NOT EXISTS TURNSTILE_SUMMARY
WITH (
    kafka_topic='TURNSTILE_SUMMARY',
    value_format='json',
    partitions = 12
) AS 
    SELECT COUNT(*) AS count, station_id FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
    except:
        logger.error(f"{resp.content}")


if __name__ == "__main__":
    execute_statement()
