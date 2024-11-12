import json
import logging
from dataclasses import dataclass
from keboola.csvwriter import ElasticDictWriter
import csv

import dateparser
import pytz
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from legacy_client.ssh_client import SshClient
from legacy_client.result import Fetcher

KEY_INDEX_NAME = 'index_name'
KEY_REQUEST_BODY = 'request_body'
KEY_STORAGE_TABLE = 'storage_table'
KEY_INCREMENTAL = 'incremental'
KEY_PRIMARY_KEYS = 'primary_keys'

KEY_DATE = 'date'
KEY_DATE_APPEND = 'append_date'
KEY_DATE_FORMAT = 'format'
KEY_DATE_SHIFT = 'shift'
KEY_DATE_TZ = 'time_zone'

KEY_SSH = 'ssh'
KEY_SSH_USE = 'use_ssh'
KEY_SSH_HOST = 'hostname'
KEY_SSH_PORT = 'port'
KEY_SSH_USERNAME = 'username'
KEY_SSH_PKEY = '#private_key'

KEY_DB = 'db'
KEY_DB_HOST = 'hostname'
KEY_DB_PORT = 'port'

KEY_DEBUG = 'debug'

MANDATORY_PARAMS = [KEY_INDEX_NAME, KEY_DB, KEY_STORAGE_TABLE, KEY_SSH]


@dataclass
class SshTunnel:
    hostname: str
    port: int
    username: str
    key: str


@dataclass
class Database:
    host: str
    port: str


class LegacyClient(ComponentBase):
    BATCH_PROCESSING_SIZE = 100000

    def __init__(self):
        super().__init__()

        logging.info("Running legacy ssh client.")

        if self.configuration.parameters.get('debug', False) is True:
            logger = logging.getLogger()
            logger.setLevel(level='DEBUG')

        try:
            self.validate_configuration_parameters(MANDATORY_PARAMS)
        except ValueError as e:
            raise UserException(e)

        _db_object = self._parse_db_parameters()
        _ssh_object = self._parse_ssh_parameters()
        self.index, self.index_params = self._parse_index_parameters()

        self.client = SshClient(_ssh_object, _db_object)

        self.fetcher = Fetcher(self.tables_out_path, self.configuration.parameters[KEY_STORAGE_TABLE],
                               self.configuration.parameters.get(KEY_INCREMENTAL, True),
                               self.configuration.parameters.get(KEY_PRIMARY_KEYS, []))

    def _parse_ssh_parameters(self):

        ssh_config = self.configuration.parameters.get(KEY_SSH, {})

        if ssh_config == {}:  # or ssh_config.get(KEY_SSH_USE) is False:
            raise UserException("SSH configuration not specified.")
        else:
            try:
                ssh_object = SshTunnel(ssh_config[KEY_SSH_HOST], ssh_config[KEY_SSH_PORT],
                                       ssh_config[KEY_SSH_USERNAME], ssh_config[KEY_SSH_PKEY])
            except KeyError as e:
                raise UserException(f"Missing mandatory field {e} in SSH configuration.")

            return ssh_object

    def _parse_db_parameters(self):
        db_config = self.configuration.parameters[KEY_DB]
        try:
            db_object = Database(db_config[KEY_DB_HOST], db_config[KEY_DB_PORT])
        except KeyError as e:
            raise UserException(f"Missing mandatory field {e} in DB configuration.")
        return db_object

    def _parse_index_parameters(self):

        index = self.configuration.parameters[KEY_INDEX_NAME]
        date_config = self.configuration.parameters.get(KEY_DATE, {})

        _req_body = self.configuration.parameters.get(KEY_REQUEST_BODY, '{}').strip()
        request_body_string = _req_body if _req_body != '' else '{}'

        if '{{date}}' in index:
            _date = dateparser.parse(date_config.get(KEY_DATE_SHIFT, 'yesterday'))

            if _date is None:
                raise UserException(f"Could not parse value {date_config[KEY_DATE_SHIFT]} to date.")

            _date = _date.replace(tzinfo=pytz.UTC)

            _tz = date_config.get(KEY_DATE_TZ, 'UTC')

            if _tz not in pytz.all_timezones:
                raise UserException(f"Incorrect timezone {_tz} provided. Timezone must be a valid DB timezone name. "
                                    "See https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List.")

            _date_tz = pytz.timezone(_tz).normalize(_date)
            _date_formatted = _date_tz.strftime(date_config.get(KEY_DATE_FORMAT, '%Y-%m-%d'))

            index = index.replace('{{date}}', _date_formatted)
            logging.info(f"Replaced date placeholder with value {_date_formatted}. " +
                         f"Downloading data from index {index}.")

        else:
            logging.info(f"No date placeholder found in index name {index}.")

        try:
            request_body = json.loads(request_body_string)
        except ValueError:
            raise UserException("Could not parse request body string to JSON.")

        return index, request_body

    def parse_curl_stdout(self, stdout):
        stdout_split = stdout.split('\r\n\r\n')
        rsp_status = stdout_split[0].split(' ')[1]
        rsp_body = stdout_split[1]
        return rsp_status, rsp_body

    def parse_scroll(self, scroll_response):

        try:
            scroll_json = json.loads(scroll_response)
        except ValueError as e:
            raise UserException(f"Could not parse JSON response - {e}.")

        return scroll_json.get('_scroll_id'), scroll_json['hits']['total'], scroll_json['hits']['hits']

    def run(self):

        previous_state = self.get_state_file()
        if previous_state:
            columns = previous_state.get("columns", [])
            logging.info(f"Using table columns from state file: {columns}")
        else:
            columns = []
        is_complete = False

        _fp_out, _fp_err = self.client.get_first_page(self.index, self.index_params)

        if _fp_out == '' and _fp_err != '':
            raise UserException(f"Could not download data. Error: {_fp_err}")

        elif _fp_out == '' and _fp_err == '':
            raise UserException("No data returned.")

        else:
            pass

        logging.debug("Parsing first page.")
        stdout_sc, stdout_body = self.parse_curl_stdout(_fp_out)

        if stdout_sc != '200':
            raise UserException(f"Could not download data. Error: {stdout_body}.")

        else:
            _scroll_id, _nr_results, _results = self.parse_scroll(stdout_body)

        logging.info(f"{_nr_results} rows will be downloaded from index {self.index}.")
        all_results = [self.fetcher.flatten_json(r) for r in _results]

        already_written = 0
        with ElasticDictWriter(self.fetcher.get_table_path(), fieldnames=columns, restval='',
                               quoting=csv.QUOTE_ALL, quotechar='\"') as wr:
            for row in self.fetcher.fetch_results(all_results):
                wr.writerow(row)

            already_written += len(_results)

            if len(_results) < self.client._default_size:
                is_complete = True

            while not is_complete:

                _scroll_out, _scroll_err = self.client.get_scroll(_scroll_id)

                if _scroll_out == '':
                    raise UserException(f"Could not download data for scroll {_scroll_id}.\n" +
                                        f"STDERR: {_scroll_err}.")

                else:
                    pass

                stdout_sc, stdout_body = self.parse_curl_stdout(_scroll_out)

                if stdout_sc != '200':
                    raise UserException(f"Could not download data. Error: {stdout_body}.")

                else:
                    _scroll_id, _, _results = self.parse_scroll(stdout_body)

                all_results = [self.fetcher.flatten_json(r) for r in _results]

                if len(_results) < self.client._default_size:
                    is_complete = True

                for row in self.fetcher.fetch_results(all_results):
                    wr.writerow(row)
                already_written += len(_results)

                if already_written % self.BATCH_PROCESSING_SIZE == 0:
                    logging.info(f"Parsed {already_written} results so far.")

        logging.info(f"Downloaded all data for index {self.index}. Parsed {already_written} rows.")
        if already_written > 0:
            self.fetcher.create_manifest(wr.fieldnames, self.fetcher.incremental, self.fetcher.primary_keys)
            self.write_state_file({"columns": wr.fieldnames})

        logging.info("Component finished.")
