import io
import json
import logging
import socket
import sys
from typing import List, Tuple
from retry import retry
import paramiko
from furl import furl

Headers = List[Tuple[str, str]]

# Workaround for re-key timeout: https://github.com/paramiko/paramiko/issues/822
paramiko.packet.Packetizer.REKEY_BYTES = 1e12

SIZE_PARAM = 'size'
SCROLL_PARAM = 'scroll'

DEFAULT_SIZE = 2000
DEFAULT_SCROLL = '15m'
SSH_COMMAND_TIMEOUT = 60  # this is in seconds


class SshClient:

    def __init__(self, SshTunnel, Database):

        self.SshTunnel = SshTunnel

        pkey_file = io.StringIO(SshTunnel.key)
        self.pkey = self._parse_private_key(pkey_file)

        # Set up SSH paramiko client
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.connect_ssh()

        self.db = Database
        self._default_size = DEFAULT_SIZE

    def connect_ssh(self):
        try:
            self.ssh.connect(hostname=self.SshTunnel.hostname, port=self.SshTunnel.port,
                             username=self.SshTunnel.username, pkey=self.pkey)
        except (socket.gaierror, paramiko.ssh_exception.AuthenticationException):
            logging.exception("Could not establish SSH tunnel. Check that all SSH parameters are correct.")
            sys.exit(1)

    def _parse_private_key(self, keyfile):
        # try all versions of encryption keys
        pkey = None
        failed = False
        try:
            pkey = paramiko.RSAKey.from_private_key(keyfile)
        except paramiko.SSHException as e:
            logging.debug(e)
            logging.warning("RSS Private key invalid, trying DSS.")
            failed = True
        except IndexError:
            logging.exception("Could not read RSS Private Key - have you provided it correctly?")
            sys.exit(1)
        # DSS
        if failed:
            try:
                pkey = paramiko.DSSKey.from_private_key(keyfile)
                failed = False
            except paramiko.SSHException:
                logging.warning("DSS Private key invalid, trying ECDSAKey.")
                failed = True
            except IndexError:
                logging.exception("Could not read DSS Private Key - have you provided it correctly?")
                sys.exit(1)
        # ECDSAKey
        if failed:
            try:
                pkey = paramiko.ECDSAKey.from_private_key(keyfile)
                failed = False
            except paramiko.SSHException:
                logging.warning("ECDSAKey Private key invalid, trying Ed25519Key.")
                failed = True
            except IndexError:
                logging.exception("Could not read ECDSAKey Private Key - have you provided it correctly?")
                sys.exit(1)
        # Ed25519Key
        if failed:
            try:
                pkey = paramiko.Ed25519Key.from_private_key(keyfile)
            except paramiko.SSHException as e:
                logging.warning("Ed25519Key Private key invalid.")
                raise e
            except IndexError:
                logging.exception("Could not read Ed25519Key Private Key - have you provided it correctly?")
                sys.exit(1)

        return pkey

    def build_curl(self, url, request_type, headers=None, json_body=None):

        # Start of curl string
        curl = 'curl -i'
        curl += f' --request {request_type}'

        # Add headers
        _header_string = ''
        for header in headers:
            _header_string += f' -H "{header[0]}: {header[1]}"'

        curl += _header_string

        # Add JSON body
        if json_body is not None:
            curl += f' --data \'{json.dumps(json_body)}\''

        curl += f' {url}'

        logging.debug(f"Constructed cURL: {curl}.")
        return curl

    @retry(paramiko.ssh_exception.SSHException, delay=3, tries=5, backoff=3)
    def _execute_ssh_command(self, curl):
        """
        Wrapped func to execute ssh command with timeout defined in SSH_COMMAND_TIMEOUT
        """
        _, stdout, stderr = self.ssh.exec_command(command=curl, timeout=SSH_COMMAND_TIMEOUT)
        return _, stdout, stderr

    def execute_ssh_command(self, curl):
        """
        Executes ssh command with timeout defined in SSH_COMMAND_TIMEOUT
        """
        try:
            _, stdout, stderr = self._execute_ssh_command(curl)
        except paramiko.ssh_exception.SSHException:
            try:
                logging.info("Failed to execute SSH command, resetting connection and trying again...")
                self.connect_ssh()
                _, stdout, stderr = self._execute_ssh_command(curl)
            except paramiko.ssh_exception.SSHException:
                logging.exception(f"Maximum number of retries (3) reached when executing ssh_command {curl}")
                sys.exit(1)
            return _, stdout, stderr
        return _, stdout, stderr

    def get_first_page(self, index, body):

        db_url = furl(f'{self.db.host}:{self.db.port}')
        db_url /= f'{index}/_search'

        self._default_scroll = body.pop(SCROLL_PARAM, DEFAULT_SCROLL)
        db_url.args[SCROLL_PARAM] = self._default_scroll

        if SIZE_PARAM in body:
            self._default_size = body[SIZE_PARAM]
        else:
            body[SIZE_PARAM] = self._default_size

        logging.info(f"Default size: {self._default_size}")

        curl = self.build_curl(db_url, 'POST', [('Content-Type', 'application/json')], body)

        _, stdout, stderr = self.execute_ssh_command(curl)
        out, err = stdout.read(), stderr.read()

        return out.decode().strip(), err.decode().strip()

    def get_scroll(self, scroll_id):
        db_url = furl(f'{self.db.host}:{self.db.port}')
        db_url /= '_search/scroll'

        data = {'scroll': self._default_scroll, 'scroll_id': scroll_id}

        curl = self.build_curl(db_url, 'POST', [('Content-Type', 'application/json')], data)

        _, stdout, stderr = self.execute_ssh_command(curl)
        out, err = stdout.read(), stderr.read()

        return out.decode().strip(), err.decode().strip()
