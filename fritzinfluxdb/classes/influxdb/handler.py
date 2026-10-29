# -*- coding: utf-8 -*-
#  Copyright (c) 2022 - 2023 Ricardo Bartels. All rights reserved.
#
#  fritzinfluxdb.py
#
#  This work is licensed under the terms of the MIT license.
#  For a copy, see file LICENSE.txt included in this
#  repository or visit: <https://opensource.org/licenses/MIT>.

import asyncio
import pytz
from datetime import datetime, timedelta
from http.client import HTTPConnection
from logging import LogRecord

from influxdb_client import InfluxDBClient, BucketRetentionRules, DBRPCreate, DBRPsService
from influxdb_client.rest import ApiException
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

from fritzinfluxdb.classes.influxdb.config import InfluxDBConfig
from fritzinfluxdb.log import get_logger
from fritzinfluxdb.classes.common import FritzMeasurement

from fritzinfluxdb.classes.fritzbox.config import FritzBoxConfig

log = get_logger()


class InfluxHandler:

    name = "InfluxDB"

    config = None
    session = None
    write_api = None

    # default InfluxDB connection timeout
    connection_timeout = 5

    # max size of message buffer before discarding old measurements
    max_measurements_buffer_size = 1_000_000

    # max number of measurements written with each InfluxDB write
    max_measurements_per_write = 1_000

    # percentage of filled buffer to start issue warnings
    max_measurements_buffer_warning = 80

    # the number of seconds to retry first unsuccessful write.
    # The next retry interval is doubled and so on
    retry_interval = 5

    # max interval between write retries
    max_retry_interval = 120

    # measurements closer than this to the retention period boundary are dropped as well,
    # as the boundary keeps moving while measurements wait in the buffer
    retention_period_margin = timedelta(minutes=5)

    # keep track if this instance was initiated successfully
    init_successful = False

    # set to true if connection to InfluxDB got lost
    connection_lost = False

    def __init__(self, config, user_agent=None):

        self.config = InfluxDBConfig(config)
        self.version = None
        self.init_successful = False

        self.buffer = list()

        # retention period of the configured bucket, None if unknown or infinite
        self.retention_period = None

        self.current_retry_interval = self.retry_interval
        self.last_write_retry = None
        self.current_max_measurements_buffer_warning = self.max_measurements_buffer_warning

        proto = "http"
        if self.config.tls_enabled:
            proto = "https"

        debug = False
        if HTTPConnection.debuglevel > 0:
            debug = True

        self.session = InfluxDBClient(
            url=f"{proto}://{self.config.hostname}:{self.config.port}",
            port=self.config.port,
            token=self.config.token,
            org=self.config.organisation,
            ssl=self.config.tls_enabled,
            verify_ssl=self.config.verify_tls,
            timeout=self.connection_timeout * 1000,
            debug=debug
        )

        if user_agent is not None:
            self.session.api_client.user_agent = user_agent

    def connect(self):

        if self.init_successful is True:
            return

        try:
            self.version = str(self.session.version().split(",")[0])

        except Exception as e:
            log.error(f"Failed to connect to InfluxDB: {e}")
            return

        # check status on influxdb buckets, if possible
        self.check_bucket_status()

        self.write_api = self.session.write_api(write_options=SYNCHRONOUS)

    def close(self):

        if self.session is not None:
            self.session.close()
            log.info("Closed InfluxDB session")

    def check_bucket_status(self):

        # get list of buckets and see if the configured bucket exists
        try:
            buckets_api = self.session.buckets_api()
            buckets = buckets_api.find_buckets().buckets
            dbrp_api = DBRPsService(api_client=self.session.api_client)
        except ApiException as e:
            log.error(f"Problem reading configured InfluxDB buckets: {e.status}: {e.reason}")
            return
        except Exception as e:
            log.error(f"Problem reading configured InfluxDB buckets: {e}")
            return

        if buckets is None or len(buckets) == 0:
            log.debug("Unable to read existing buckets. Assuming this is a token with "
                      "insufficient permissions to perform this action. Assuming the configured bucket is present.")
            log.info("Unable to determine the retention period of the InfluxDB bucket. "
                     "Measurements older than the retention period will be dropped by InfluxDB.")

            self.init_successful = True
            log.info("Connection to InfluxDB 2 established")
            return

        bucket_data = None
        for bucket in buckets or list():
            if self.config.bucket in [bucket.name, bucket.id]:
                log.debug(f"InfluxDB bucket '{self.config.bucket}' exists")
                bucket_data = bucket

        # create new bucket
        if bucket_data is None:
            log.info(f"InfluxDB bucket '{self.config.bucket}' not found, trying to create it")
            try:
                retention_rules = BucketRetentionRules(type="expire",
                                                       every_seconds=3600 * 24 * self.config.data_retention_days,
                                                       shard_group_duration_seconds=3600 * 24)

                bucket_data = buckets_api.create_bucket(bucket_name=self.config.bucket,
                                                        retention_rules=retention_rules,
                                                        org=self.config.organisation,
                                                        description="FritzInfluxDB bucket")

            except Exception as e:
                log.error(f"Problem creating InfluxDB bucket: {e}")
                return

            log.info(f"Successfully create InfluxDB bucket '{self.config.bucket}'")

        self.set_retention_period(bucket_data)

        # create new bucket
        if bucket_data is not None:
            log.debug(f"InfluxDB bucket '{self.config.bucket}' present, checking database name mapping")
            try:
                bucket_dbrp_data = dbrp_api.get_dbr_ps(bucket_id=bucket_data.id, org=self.config.organisation)
            except Exception as e:
                log.error(f"Problem requesting InfluxDB DBRP data: {e}")
                return

            create_retention_policy = False
            if "1year" not in [x.retention_policy for x in bucket_dbrp_data.content]:
                create_retention_policy = True

            if len(bucket_dbrp_data.content) == 0:
                create_retention_policy = True

            if create_retention_policy is False:
                log.debug(f"InfluxDB bucket '{self.config.bucket}' has a database mapping retention policy: "
                          f"{bucket_dbrp_data.content[0].retention_policy}")
            else:
                try:
                    dbrp_api.post_dbrp(DBRPCreate(
                        org=self.config.organisation,
                        bucket_id=bucket_data.id,
                        database=bucket_data.name,
                        retention_policy="1year",
                        default=True
                    ))
                except Exception as e:
                    log.error(f"Problem creating InfluxDB DBRP data: {e}")
                    return

                log.info(f"Successfully create InfluxDB database mapping '{self.config.bucket}'")

        log.info(f"Connection to InfluxDB {self.version} established and bucket is present")

        self.init_successful = True

    def set_retention_period(self, bucket_data):
        """
        read the retention period from the bucket's "expire" retention rule.
        A rule with 0 seconds means data never expires.
        """

        self.retention_period = None

        for rule in getattr(bucket_data, "retention_rules", None) or list():
            if rule.type == "expire" and isinstance(rule.every_seconds, int) and rule.every_seconds > 0:
                self.retention_period = timedelta(seconds=rule.every_seconds)

        if self.retention_period is None:
            log.info(f"InfluxDB bucket '{self.config.bucket}' has no retention period")
        else:
            log.info(f"InfluxDB bucket '{self.config.bucket}' has a retention period of "
                     f"{self.retention_period_days:g} days, "
                     f"older measurements will be dropped before writing")

    @property
    def retention_period_days(self):
        return self.retention_period.total_seconds() / 86400

    def drop_measurements_outside_retention_period(self):
        """
        remove all measurements from the buffer which InfluxDB would refuse to write
        as they are older than the retention period of the bucket
        """

        if self.retention_period is None or len(self.buffer) == 0:
            return

        oldest_permitted_timestamp = datetime.now(pytz.utc) - self.retention_period + self.retention_period_margin

        def is_outside(measurement):
            timestamp = measurement.timestamp
            if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
                timestamp = pytz.utc.localize(timestamp)
            return timestamp < oldest_permitted_timestamp

        dropped = [x for x in self.buffer if is_outside(x)]

        if len(dropped) == 0:
            return

        log.info(f"Dropping '{len(dropped)}' measurements which are older than the InfluxDB bucket "
                 f"retention period of {self.retention_period_days:g} days")
        for measurement in dropped:
            log.debug(f"Dropped measurement: {measurement}")

        self.buffer[:] = [x for x in self.buffer if not is_outside(x)]

    def convert_measurement(self, measurement):

        if not isinstance(measurement, FritzMeasurement):
            log.error(f"Measurement needs to be a 'FritzMeasurement' but got '{type(measurement)}'")
            return

        return {
            "measurement": self.config.measurement_name,
            "tags": measurement.tags,
            "time": measurement.timestamp,
            "fields": {measurement.name: measurement.value}
        }

    def permitted_to_write_data(self):

        # permit writing if no last write retry is known
        if self.last_write_retry is None:
            return True

        # limit max retry seconds
        if self.current_retry_interval > self.max_retry_interval:
            self.current_retry_interval = self.max_retry_interval

        # check if the next retry is permitted
        if (datetime.now(pytz.utc)-self.last_write_retry).total_seconds() >= self.current_retry_interval:
            return True

        return False

    async def write_data(self):

        if self.permitted_to_write_data() is False:
            return

        self.drop_measurements_outside_retention_period()

        if len(self.buffer) == 0:
            log.debug("InfluxDB data queue: No measurements found in queue")
            return

        # only use max amount of measurements to send to InfluxDB
        log.debug(f"Trying to write a maximum of '{self.max_measurements_per_write}' measurements to InfluxDB")
        local_buffer = self.buffer[0:self.max_measurements_per_write]

        # convert FritzMeasurement to list of dicts
        data = [self.convert_measurement(x) for x in local_buffer]

        write_successful = False
        self.last_write_retry = datetime.now(pytz.utc)
        try:
            self.write_api.write(bucket=self.config.bucket, record=data, write_precision=WritePrecision.US)
            write_successful = True
        except ApiException as e:

            exception_message = e.message
            if exception_message is None:
                exception_message = str(e)

            # InfluxDB wrote all measurements but the ones older than the retention period.
            # Retrying would only fail again, so the write is treated as successful.
            if e.status == 422 and "partial write" in exception_message and "retention policy" in exception_message:
                log.info(f"InfluxDB dropped measurements which are older than the bucket retention period: "
                         f"{exception_message}")
                write_successful = True
            else:
                log.error(f"Failed to write to InfluxDB '{self.config.hostname}': {e.status}: {exception_message}")

        except Exception as e:
            self.connection_lost = True
            log.error(f"Failed to write to InfluxDB '{self.config.hostname}': {e}")

        if write_successful is True:
            if self.connection_lost is True:
                log.info(f"Connection to influxDB '{self.config.hostname}' restored.")
                log.info(f"Flushing '{len(self.buffer)}' measurements to InfluxDB")

            log.debug(f"Successfully wrote {len(local_buffer)} measurements to InfluxDB")
            self.buffer[:] = [x for x in self.buffer if x not in local_buffer]

            self.connection_lost = False
            self.last_write_retry = None
            self.current_retry_interval = self.retry_interval

        else:
            if self.connection_lost is True:
                self.current_retry_interval *= 2

    async def check_buffer(self):

        length = len(self.buffer)
        max_length = self.max_measurements_buffer_size

        percent_buffer_usage = 100 / max_length * length

        buffer_warning_message = f"InfluxDB measurement buffer currently at {percent_buffer_usage:0.2f}% " \
                                 f"(current {length}/max {max_length}). If buffer is full the oldest " \
                                 f"messages will be discarded."

        if length > max_length:
            log.critical(f"InfluxDB measurement buffer length '{length}' "
                         f"exceeded the maximum of {max_length} items. "
                         f"Discarding oldest {length - max_length} measurements.")
            self.buffer[:] = self.buffer[0 - max_length:]

        elif percent_buffer_usage >= self.current_max_measurements_buffer_warning:
            log.warning(buffer_warning_message)
            self.current_max_measurements_buffer_warning += 5
        elif percent_buffer_usage < self.max_measurements_buffer_warning:
            self.current_max_measurements_buffer_warning = self.max_measurements_buffer_warning

    async def task_loop(self, queue):

        while True:

            # transfer items to instance buffer
            while queue.empty() is False:
                # add measurements to instance buffer
                self.buffer.append(await queue.get())

            # write data from buffer to InfluxDB
            await self.write_data()
            await self.check_buffer()

            log.debug(f"Current InfluxDB measurement buffer length: {len(self.buffer)}")
            await asyncio.sleep(1)


class InfluxLogAndConfigWriter:

    name = "InfluxLogAndConfigWriter"

    config = None

    # InfluxDB measurement name
    log_measurement_name = "log_entry"

    # log type of records for fritzinfluxdb instances
    log_record_type = "FritzInfluxDB"

    # measurement name of timezone setting record
    timezone_measurement_name = "fritzinfluxdb_setting_timezone"

    # The log queue to read the log records from
    log_queue = None

    # interval in which to write the timezone setting to InfluxDB
    timezone_setting_write_interval = 60 * 60 * 12  # every twelve hours

    # keep track if this instance was initiated successfully
    init_successful = False

    def __init__(self, config: FritzBoxConfig, log_queue: asyncio.Queue):
        """
        Handler to read log records from 'log_queue', format them to InfluxDB measurements
        and writers them to the output queue.
        Also writes the config option "timezone" to InfluxDB.

        Parameters
        ----------
        config: FritzBoxConfig
            the current FritzBoxConfig
        log_queue: asyncio.Queue
            queue object to read logs from which should be sent to InfluxDB

        Returns
        -------
        handler to use for formatting log entries and write timezone config to InfluxDB
        """

        if not isinstance(config, FritzBoxConfig):
            raise ValueError("param 'config' needs to be a 'FritzBoxConfig' object")

        if not isinstance(log_queue, asyncio.Queue):
            raise ValueError("param 'log_queue' needs to be a 'asyncio.Queue' object")

        self.config = config
        self.log_queue = log_queue

        self.last_timezone_setting_write = None

        self.init_successful = True

    def format_log_record(self, log_record):

        if not isinstance(log_record, LogRecord):
            return None

        log_timestamp = pytz.timezone("UTC").localize(datetime.utcfromtimestamp(log_record.created))

        log_msg = "{levelname}: {message}".format(**log_record.__dict__)

        return FritzMeasurement(self.log_measurement_name, log_msg,
                                box_tag=self.config.box_tag,
                                additional_tags={
                                    "log_type": self.log_record_type
                                },
                                data_type=str,
                                timestamp=log_timestamp,
                                timestamp_precision=WritePrecision.US)

    def get_timezone_setting_measurement(self):

        return FritzMeasurement(self.timezone_measurement_name, self.config.timezone,
                                box_tag=self.config.box_tag,
                                data_type=str)

    def is_time_to_write_timezone_setting(self):

        if self.last_timezone_setting_write is None:
            return True

        if (datetime.now(pytz.utc) - self.last_timezone_setting_write).total_seconds() >= \
                self.timezone_setting_write_interval:
            return True

        return False

    async def task_loop(self, output_queue: asyncio.Queue):

        while True:

            while self.log_queue.empty() is False:
                formatted_log_record = self.format_log_record(await self.log_queue.get())

                if formatted_log_record is None:
                    continue

                log.debug(formatted_log_record)

                await output_queue.put(formatted_log_record)

            # write timezone setting to influx queue
            if self.is_time_to_write_timezone_setting():
                timezone_measurement = self.get_timezone_setting_measurement()
                log.debug(timezone_measurement)
                await output_queue.put(timezone_measurement)
                self.last_timezone_setting_write = datetime.now(pytz.utc)

            await asyncio.sleep(1)

# EOF
