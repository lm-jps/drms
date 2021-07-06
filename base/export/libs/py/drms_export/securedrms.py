'''
securedrms is a module that allows secure access, via ssh or Basic Access HTTP authentication, to a DRMS server's API methods; it is part of the drms Python package and requires the presence of the other modules in that package

here is a very basic example of its usage:

>>> from drms import securedrms

>>> # create a secure client factory
>>> factory = securedrms.SecureClientFactory()
>>> factory
<drms.securedrms.SecureClientFactory object at 0x10b2c24a8>
>>>
>>> # create an ssh client
>>> sshclient = factory.create_client(use_ssh=True)
>>> sshclient
<SSHClient "__JSOC">
>>>
>>> # list DRMS series whose names contain the string 'lev1'
>>> sshclient.series('lev1')
please enter password for arta@solarport
Password:
['aia.lev1', 'aia.lev1_euv_12s', 'aia.lev1_uv_24s', 'aia.lev1_vis_1h', 'aia_test.lev1_12s4arc', 'hmi.lev1_cal', 'hmi.lev1_dcon', 'iris.lev1', 'iris.lev1_nrt', 'mdi.fd_M_96m_lev182', 'mdi.fd_m_lev182']
>>>
>>> # create a Basic Access HTTP client
>>> httpclient = factory.create_client(use_ssh=False)
>>> httpclient
<BasicAccessClient "__JSOC">
>>> # list DRMS series whose names contain the string 'su_arta'; print a listing that contains series information
>>> httpclient.series('lev1', True)
                     name                                       note
0                aia.lev1                                AIA Level 1
1        aia.lev1_euv_12s             AIA Level 1, 12 second cadence
2         aia.lev1_uv_24s             AIA Level 1, 24 second cadence
3         aia.lev1_vis_1h           AIA Level 1, 3600 second cadence
4   aia_test.lev1_12s4arc                                AIA Level 1
5            hmi.lev1_cal             HMI Level 1 Calibration Series
6           hmi.lev1_dcon                                HMI Level 1
7               iris.lev1                 IRIS level 1 on-orbit data
8           iris.lev1_nrt  Near real time IRIS level 1 on-orbit data
9     mdi.fd_M_96m_lev182              MDI Full Disk 96m Magnetogram
10        mdi.fd_m_lev182             MDI Full Disk 01-m Magnetogram

>>> sshclient = factory.create_ssh_client(use_internal=True)
>>> sshclient
<SSHClient "__JSOC">
>>> res = sshclient.query('hmi.ic_720s[2017.1.8/60m]', key='TINTNUM,T_OBS', seg='continuum')
please enter a password for arta@solarport
Password:
>>> type(res)
<class 'tuple'>
>>> res[0]
   TINTNUM                    T_OBS
0      672  2017.01.07_23:59:52_TAI
1      672  2017.01.08_00:11:52_TAI
2      672  2017.01.08_00:23:52_TAI
3      672  2017.01.08_00:35:52_TAI
4      672  2017.01.08_00:47:52_TAI
>>> res[1]
                                 continuum
0  /SUM89/D990049682/S00000/continuum.fits
1  /SUM89/D990049682/S00001/continuum.fits
2  /SUM89/D990049682/S00002/continuum.fits
3  /SUM89/D990049682/S00003/continuum.fits
4  /SUM89/D990049682/S00004/continuum.fits
'''

# standard library imports
import base64
import getpass
import importlib
import inspect
import io
from json import load, loads, dumps, decoder
import os
import numpy
import pexpect
from queue import LifoQueue
import re
import shlex
import signal
import sys
import tarfile
import threading
import types
from typing import cast
from urllib.error import URLError
from urllib.parse import urlencode, urlparse, urlunparse, urljoin
from urllib.request import Request, urlopen
import uuid
from subprocess import check_output, CalledProcessError
import asyncio

# third party imports
import pandas
from six.moves.urllib import request as sixUrlRequest
from astropy.io import fits

# local imports
import drms
from drms import utils
from drms.client import Client as DRMSClient, ExportRequest, SeriesInfo
from drms.config import ServerConfig, register_server
from drms.exceptions import DrmsQueryError, DrmsExportError, DrmsOperationNotSupported
from drms.json import HttpJsonClient, HttpJsonRequest
from drms.utils import _split_arg


__all__ = [
    'BasicAccessClient',
    'BasicAccessHttpJsonClient',
    'BasicAccessHttpJsonRequest',
    'BasicAccessOnTheFlyDownloader',
    'OnTheFlyDownloader',
    'RuntimeEnvironment',
    'SecureClient',
    'SecureClientFactory',
    'SecureDRMSError',
    'SecureDRMSArgumentError',
    'SecureDRMSAuthorityFileError',
    'SecureDRMSConfigurationError',
    'SecureDRMSMethodError',
    'SecureDRMSResponseError',
    'SecureDRMSTimeOutError',
    'SecureDRMSUrlError',
    'SecureDRMSDeprecatedError',
    'SecureExportRequest',
    'SecureServerConfig',
    'SSHClient',
    'SSHJsonClient',
    'SSHJsonRequest',
    'SSHOnTheFlyNonstopDownloader',
    'SSHOnTheFlyOnestopDownloader'
]

DEFAULT_SSH_PORT=22
DEFAULT_SERVER_ENCODING='utf8'
PASSWORD_TIMEOUT=60

# Exception classes
class SecureDRMSError(Exception):
    '''
    base exception class for all Secure DRMS exceptions
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSArgumentError(SecureDRMSError):
    '''
    invalid or missing method argument
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSAuthorityFileError(SecureDRMSError):
    '''
    invalid or missing method argument
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSConfigurationError(SecureDRMSError):
    '''
    invalid or missing Secure DRMS server configuration attribute
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSMethodError(SecureDRMSError):
    '''
    method is not valid for the current secure-server configuration
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSResponseError(SecureDRMSError):
    '''
    time-out waiting for DRMS Server to respond
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSSystemError(SecureDRMSError):
    '''
    an OS error was encountered, such as a file IO error
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSTimeOutError(SecureDRMSError):
    '''
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSUrlError(SecureDRMSError):
    '''
    unable to open URL
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSDeprecatedError(SecureDRMSError):
    '''
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSChildProcessError(SecureDRMSError):
    '''
    error running child process
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class RuntimeEnvironment(object):
    '''
    a class to manage environment variables needed to initialize the remote-host runtime environment

    Attributes
    ----------
    <env var> : str
        For each environment variable <env var>, a str attribute with the same name exists.
    '''
    def __init__(self, env):
        '''
        Parameters
        ----------
        env : dict
            a dictionary where each key is the name of an environment variable and its value is the variable's string value

        The constructor iterates through the `env` dictionary, creating one RuntimeEnvironment instance attribute for each element.
        '''
        for env_var in env:
            setattr(self, env_var, env[env_var])

    def bash_cmd(self):
        '''
        returns a list containing one element per environment variable stored as an attribute in the instance; each element is a bash command that APPENDS a value to one existing variable (like [ "export VAR1=$VAR1'value1'", "export VAR2=$VAR2'value2" ]). If the environment variable does not already exist, it is created.

        Parameters
        ----------
        (none)

        Return Value
        ------------
        list
            an enumeration of bash export commands; each command sets and exports a single enviornment variable

        '''
        cmds = []

        if len(vars(self)) > 0:
            for env_var, val in vars(self).items():
                cmds.append('export ' + env_var + '=$' + env_var + "'" + val + "'")

        return cmds


class SecureServerConfig(ServerConfig):
    '''
    a class to create, maintain, and store DRMS server configurations for DRMS servers that require secure access

    Each instance describes the configuration of one NetDRMS server. After creating an instance, the instance should be passed to SecureServerConfig.register_server() so that this module can manage the instance.

    Attributes
    ----------
        connection_info : dict
            `connection_info` contains the host, port, role, and database name information needed to connect to the DRMS database
            {
              'dbhost' : <str - the database host>,
              'dbport' : <int - the port on the database host>,
              'dbuser' : <str - the database role>,
              'dbname' : <str - the database name>
            }
        download_directory : str
            `download_directory` contains the client path to which exported files are downloaded
        has_full_export_system : bool
            If `has_full_export_system` is True, the server has a fully-implemented export system. This implies the existence of an export-system-manager instance which monitors and manages export-system database tables and submits export requests to a queueing system for processing.
        http_download_baseurl : str
            For BasicAccessClient clients, `http_download_baseurl` contains the URL path to the Storage Units that will be downloaded via HTTP.
        ftp_download_baseurl : str
            For BasicAccessClient clients, `ftp_download_baseurl` contains the URL path to the Storage Units that will be downloaded via FTP.
        name : str
            `name` is a name for the NetDRMS server.
        server_tmp : str
            `server_tmp` contains the path to the remote-server temporary directory to which the SecureClient.export_fits() API downloads tar files.
        ssh_base_bin : str
            For SSHClient clients, `ssh_base_bin` contains the path to the remote-server directory that contains all binary executables referenced in this module.
        ssh_base_script : str
            For SSHClient clients, `ssh_base_script` contains the path to the remote-server directory that contains all scripts referenced in this module.
        ssh_check_email : str
            For SSHClient clients, `ssh_check_email` contains the remote-server script file that checks the registration status of export email addresses.
        ssh_check_email_addresstab : str
            For SSHClient clients, `ssh_check_email_addresstab` contains name of the remote-server database table that contains the registered local names.
        ssh_check_email_domaintab : str
            For SSHClient clients, `ssh_check_email_domaintab` contains name of the remote-server database table that contains the domains of the registered local names.
        ssh_export_fits : str
            For SSHClient clients, `ssh_export_fits` contains the remote-server program that synchronously exports FITS-protocol DRMS segment files.
        ssh_export_fits_args : dict
            For SSHClient clients, `ssh_export_fits_args` contains the command-line arguments for the `ssh_export_fits` program required for external/public database access:
                JSOC_DBHOST : str
                    the public database host (as seen from the SSH server)
                JSOC_DBUSER : str
                    the database account to access
                maxfilesize : int
                    the maximum size, expressed as the number of bytes, of the tar file that is allowed to be generated
        ssh_export_fits_internal_args : dict
            For SSHClient clients, `ssh_export_fits_internal_args` contains the command-line arguments for the `ssh_export_fits` program required for internal/private database access.
                JSOC_DBHOST : str
                    the private database host (as seen from the SSH server)
                JSOC_DBUSER : str
                    the database account to access
                maxfilesize : int
                    the maximum size, expressed as the number of bytes, of the tar file that is allowed to be generated
        ssh_jsoc_fetch : str
            For SSHClient clients, `ssh_jsoc_fetch` contains the remote-server program that initiates export requests, and reports the status on those requests. It accesses the external/public database.
        ssh_jsoc_fetch_args : dict
            For SSHClient clients, `ssh_jsoc_fetch_args` contains the command-line arguments for the `ssh_jsoc_fetch` program required for external/public database access:
                JSOC_DBHOST : str
                    the public database host (as seen from the SSH server)
                JSOC_DBUSER : str
                    the database account to access
        ssh_jsoc_fetch_internal_args : dict
            For SSHClient clients, `ssh_jsoc_fetch_internal_args` contains the command-line arguments for the `ssh_jsoc_fetch` program required for internal/private database access:
                JSOC_DBHOST : str
                    the private database host (as seen from the SSH server)
                JSOC_DBUSER : str
                    the database account to access
        ssh_jsoc_fetch_wrapper : str
            For SSHClient clients, `ssh_jsoc_fetch_wrapper` contains the remote-server program that initiates export requests, and reports the status on those requests. It accesses the external/public database, plus the internal/private database for pass-through series.
        ssh_jsoc_fetch_wrapper_args : dict
            For SSHClient clients, `ssh_jsoc_fetch_wrapper_args` contains the command-line arguments for the `ssh_jsoc_fetch_wrapper` program required for external/public database access:
                dbhost : str
                    the public database host (as seen from the SSH server)
        ssh_jsoc_info : str
            For SSHClient clients, `ssh_jsoc_info` contains the remote-server program that provides DRMS record-set information.  It accesses the external/public database.
        ssh_jsoc_info_args : dict
            For SSHClient clients, `ssh_jsoc_info_args` contains the command-line arguments for the `ssh_jsoc_info` program required for external/public database access:
                JSOC_DBHOST : str
                    the public database host (as seen from the SSH server)
                JSOC_DBUSER : str
                    the public database role (as seen from the SSH server)
        ssh_jsoc_info_internal_args : dict
            For SSHClient clients, `ssh_jsoc_info_internal_args` contains the command-line arguments for the `ssh_jsoc_info` program required for internal/private database access:
                JSOC_DBHOST : str
                    the private database host (as seen from the SSH server)
                JSOC_DBUSER : str
                    the private database role (as seen from the SSH server)
        ssh_jsoc_info_wrapper : str
            For SSHClient clients, `ssh_jsoc_info_wrapper` contains the remote-server program that provides DRMS record-set information. It accesses the external/public database, plus the internal/private database for pass-through series.
        ssh_jsoc_info_wrapper_args : dict
            For SSHClient clients, `ssh_jsoc_info_wrapper_args` contains the command-line arguments for the `ssh_jsoc_info_wrapper` program required for external/public database access.
                    dbhost : str
                        the public database host (as seen from the SSH server)
        ssh_parse_recset : str
            For SSHClient clients, `ssh_parse_recset` contains the remote-server program that parses DRMS record-set strings into parts (e.g., series name, filters, segment list, etc.).
        ssh_remote_env : str
            For SSHClient clients, `ssh_remote_env` contains a dict of environment variables to be passed along to the remote server.
        ssh_remote_host : str
            For SSHClient clients, `ssh_remote_host` contains the name of the remote host.
        ssh_remote_port : int
            For SSHClient clients, `ssh_remote_port` contains the port number of the remote host on which the SSH sevice listens.
        ssh_remote_user : str
            For SSHClient clients, `ssh_remote_user` contains the name of the user to run the remote command as.
        ssh_show_series : str
            For SSHClient clients, `ssh_show_series` contains the remote-server program that prints the series served.
        ssh_show_series_args : dict
            For SSHClient clients, `ssh_show_series_args` contains the command-line arguments for the `ssh_show_series` program required for external/public database access:
                JSOC_DBHOST : str
                    the external database host
        ssh_show_series_internal_args : dict
            For SSHClient clients, `ssh_show_series_internal_args` contains the command-line arguments for the `ssh_show_series` program required for internal/private database access :
                JSOC_DBHOST : str
                    the internal database host
        ssh_show_series_wrapper : str
            For SSHClient clients, `ssh_show_series_wrapper` contains the remote-server program that prints the public-accessible external data series PLUS the public-accessible internal data series.
        ssh_show_series_wrapper_args : dict
            For SSHClient clients, `ssh_show_series_wrapper_args` contains the command-line arguments for the `ssh_show_series_wrapper` program required for external/public database access:
                dbhost : str
                    the database host (as seen from the SSH server)
                --wlfile : str
                    the path to the remote-server DRMS series white-list file
        web_baseurl : str
            For BasicAccessClient clients, `web_baseurl` contains the URL path to the web-application directory.
        web_baseurl_authority : str
            For BasicAccessClient clients, `web_baseurl_authority` contains the clear-text HTTP Basic Access authentication name:password credentials.
        web_baseurl_authorityfile : str
            For BasicAccessClient clients, `web_baseurl_authorityfile` contains the path to a file that contains a function that returns the base64-encoded name:password credentials. The file must contain a single function named get_authority:

            def get_authority():
                return 'XXXXXXXXXXXXXXXXXXXX'

        web_baseurl_internal : str
            For BasicAccessClient clients, `web_baseurl_internal` contains the URL path that contains all CGI scripts for HTTP access to NetDRMS.
        web_check_address : str
            For BasicAccessClient clients, `web_check_address` contains the web-application script that verifies the export-registration of an email address.
        web_export_package : str
            For BasicAccessClient clients, `web_export_package` contains the web-applicaton script that synchronously exports FITS-protocol DRMS segments files.
        web_export_package_args : dict
            For BasicAccessClient clients, `web_export_package_args` contains the URL query arguments for the `web_export_package` program required for external/public database access:
                dbhost : str
                    the external database host
                webserver : str
                    the webserver host publicly accessible
        web_export_package_internal_args: dict
            For BasicAccessClient clients, `web_export_package_internal_args` contains the URL query arguments for the `web_export_package` program required for internal/private database access:
                dbhost : str
                    the internal database host
                webserver : str
                    the webserver host privately accessible
        web_jsoc_info : str
            For BasicAccessClient clients, `web_jsoc_info` contains the web-applicaton script that provides DRMS data series and record information; the dbhost cannot be specified as an argument
        web_jsoc_fetch : str
            For BasicAccessClient clients, `web_jsoc_fetch` contains the web-applicaton script that exports DRMS segment files; the dbhost cannot be specified as an argument
        web_parse_recset : str
            For BasicAccessClient clients, `web_parse_recset` contains the web-applicaton script used to parse record-set specification character strings into components (series, namespace, database table, filters, segments)
        web_show_series : str
            For BasicAccessClient clients, `web_show_series` contains the web-applicaton script that lists public series information.
        web_show_series_wrapper : str
            For BasicAccessClient clients, `web_show_series_wrapper` contains the  web-applicaton script that lists public and accessible private series information; the dbhost can be specified as an argument
        web_show_series_wrapper_args : dict
            For BasicAccessClient clients, `contains` contains the URL query arguments for the `web_show_series_wrapper` program required for external/public database access:
                dbhost : str
                    the external database host

    Class Variables
    ---------------
    __configs : dict (of SecureServerConfig instances)
        a container of all registered server configurations; the key is the server name, and the value is a SecureServerConfig instance
    __valid_keys : set
        a list of all valid supplemental (to ServerConfig._valid_keys) configuration properties
    __urls : set

    '''
    __configs = {}
    __valid_keys = set([
        'connection_info',
        'download_directory',
        'has_full_export_system',
        'http_download_baseurl',
        'ftp_download_baseurl',
        'name',
        'server_tmp',
        'ssh_base_bin',
        'ssh_base_script',
        'ssh_check_email',
        'ssh_check_email_addresstab',
        'ssh_check_email_domaintab',
        'ssh_export_fits',
        'ssh_export_fits_args',
        'ssh_export_fits_internal_args',
        'ssh_jsoc_fetch',
        'ssh_jsoc_fetch_args',
        'ssh_jsoc_fetch_internal_args',
        'ssh_jsoc_fetch_wrapper',
        'ssh_jsoc_fetch_wrapper_args',
        'ssh_jsoc_info',
        'ssh_jsoc_info_args',
        'ssh_jsoc_info_internal_args',
        'ssh_jsoc_info_wrapper',
        'ssh_jsoc_info_wrapper_args',
        'ssh_parse_recset',
        'ssh_remote_env',
        'ssh_remote_host',
        'ssh_remote_port',
        'ssh_remote_user',
        'ssh_show_series',
        'ssh_show_series_args',
        'ssh_show_series_internal_args',
        'ssh_show_series_wrapper',
        'ssh_show_series_wrapper_args',
        'web_baseurl',
        'web_baseurl_authority',
        'web_baseurl_authorityfile',
        'web_baseurl_internal',
        'web_check_address',
        'web_export_package',
        'web_export_package_args',
        'web_export_package_internal_args',
        'web_jsoc_info',
        'web_jsoc_fetch',
        'web_parse_recset',
        'web_show_series',
        'web_show_series_wrapper',
        'web_show_series_wrapper_args' ])
    __urls = set([
        'url_check_address',
        'url_export_package',
        'url_jsoc_info',
        'url_jsoc_fetch',
        'url_parse_recset',
        'url_show_series',
        'url_show_series_wrapper' ])


    def __init__(self, *, config=None, **kwargs):
        '''
        a new SecureServerConfig instance is optionally initialized by copying the attributes in `config`. The keyword arguments in `kwargs` are then added to the instance

        Parameters
        ----------
        [ config : object (SecureServerConfig) ]
            `config` is an existing SecureServerConfig used to initialize the new SecureServerConfig.
        kwargs : keyword-argument dict
            `kwargs` contains a dict of keyword-argument parameters that will become the SecureServerConfig properties; the keyword argument `name` must exist.
        '''
        # internal dictionary of all keys
        self._d = {}

        # the child has a different set of valid keys; all code checking actual key-name validity runs in SecureServerConfig, so we can discard the parent list of valid keys and replace it with child-valid keys
        self._valid_keys = []

        # add 'keys' specific to the SecureServerConfig class to the list of valid configuration parameters in parent
        for key in self.__valid_keys | self.__urls:
            self._valid_keys.append(key)

        if config is not None:
            # initialized the instance with `config`
            for key, val in config.to_dict().items():
                if key not in self._valid_keys:
                    raise SecureDRMSConfigurationError(f'[ SecureServerConfig.__init__ ] Invalid server config key `{key}` in `config` parameter')

                self._d[key] = val

        for key, val in kwargs.items():
            # add the parameters passed in as keyword arguments
            if key not in self._valid_keys:
                raise SecureDRMSConfigurationError(f'[ SecureServerConfig.__init__ ] Invalid server config key `{key}` in keyword arguments')

            self._d[key] = val

        if 'name' not in self._d:
            raise SecureDRMSConfigurationError(f'Server config entry `name` is missing')

        # default values
        if 'ssh_remote_port' not in self._d:
            self.ssh_remote_port = DEFAULT_SSH_PORT

        if 'encoding' not in self._d:
            self.encoding = DEFAULT_SERVER_ENCODING

        # if a Basic Access authority exists in an external file, read it into the web_baseurl_authority property
        if self.web_baseurl is not None  or self.web_baseurl_internal is not None:
            if self.web_baseurl_authority is None and self.web_baseurl_authorityfile is not None:
                # authority is in a file (specifying it as a keyword argument in the constructor takes precedence)
                if os.path.exists(self.web_baseurl_authorityfile):
                    try:
                        sys.path.append(os.path.dirname(self.web_baseurl_authorityfile))
                        spec = importlib.util.spec_from_file_location('auth', self.web_baseurl_authorityfile)
                        afile = spec.loader.load_module()
                        self.web_baseurl_authority = base64.b64decode(afile.get_authority().encode()).decode()
                    except ImportError:
                        raise SecureDRMSAuthorityFileError('authority file ' + self.web_baseurl_authorityfile + ' is invalid')
                    except NameError as exc:
                        if re.search(r'get_authority', str(exc)) is not None:
                            raise SecureDRMSAuthorityFileError('authority file ' + self.web_baseurl_authorityfile + ' does not contain get_authority() definition')
                        raise
                else:
                    raise SecureDRMSAuthorityFileError('authority file ' + self.web_baseurl_authorityfile + ' does not exist')

        # do not call parent constructor - the parent handles url parameters differently; the web-application URLs, if any, are managed by SecureServerConfig.set_urls()

    def __getattr__(self, name):
        '''
        if `name` is a valid attribute, return the value from self._d dict; otherwise return the instance attribute if it exists
        '''
        if name in self.__valid_keys:
            return self._d.get(name)
        else:
            return object.__getattribute__(self, name)

    def __repr__(self):
        return '<SecureServerConfig "{name}"'.format(name=self.name)

    def __setattr__(self, name, value):
        '''
        if `name` is a valid attribute, store its value in the self._d dict; otherwise store the value as an instance attribute
        '''
        if name in self.__valid_keys:
            self._d[name] = value
        else:
            object.__setattr__(self, name, value)

    # public methods
    def check_supported(self, op, use_ssh=None, use_internal=None):
        '''
        check if an operation is supported by the server; SecureServerConfiguration.check_supported() requires that
        use_ssh and use_internal be specified, but if SecureServerConfiguration.check_supported() is called from
        the parent Client class, those two arguments will not be provided - they will both default to None; to
        deal with that, we set two attributes, SecureServerConfiguration.use_ssh and SecureServerConfiguration.use_internal
        in SecureClient before calling into Client code (and then we remove them when done)

        Parameters
        ----------
        op : str
            the API method the client is invoking; valid values: check_mail, email, export, export_fits, export_from_id, info, keys, parse_spec, pkeys, query, series
        [ use_internal : bool ]
            if True, interface programs that access publicly accessible data series will be used; if False, programs that access privately accessible series will be used; if None (the default), the 'use_internal' property will be used
        [ use_ssh : bool ]
            if True, interface programs that use the SSH-access methods will be used; if False, programs that use the HTTP-access methods will be used; if None (the default), the 'use_ssh' property will be used

        Return Value
        ------------
        bool
            True if the operation is supported by the server, False otherwise

        '''
        if use_ssh is None:
            # passed by SecureClient API method call
            use_ssh = self.use_ssh

        if use_internal is None:
            # passed by SecureClient API method call
            use_internal = self.use_internal

        if self._debug:
            print('[ SecureServerConfig.check_supported ] use_ssh --> {use_ssh}, use_internal --> {use_internal}'.format(use_ssh=str(use_ssh), use_internal=str(use_internal)))

        if use_ssh:
            if use_internal:
                if op == 'check_email' or op == 'email':
                    return self.ssh_check_email is not None and self.ssh_check_email_addresstab is not None and self.ssh_check_email_domaintab is not None and self.ssh_base_script is not None
                elif op == 'export':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_jsoc_fetch is not None and self.ssh_base_bin is not None
                elif op == 'export_fits':
                    return self.ssh_export_fits is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'export_from_id':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_jsoc_fetch is not None and self.ssh_base_bin is not None
                elif op == 'export_package':
                    return self.ssh_export_fits is not None and self.ssh_parse_recset is not None and self.ssh_jsoc_fetch is not None and self.ssh_base_bin is not None
                elif op == 'info':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'keys':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'parse_spec':
                    return self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'pkeys':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'query':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'series':
                    return (self.ssh_show_series is not None and self.ssh_base_bin is not None) or (self.ssh_show_series_wrapper is not None and self.ssh_base_script is not None)
                else:
                    return False
            else:
                # external
                if op == 'check_email' or op == 'email':
                    return self.ssh_check_email is not None and self.ssh_check_email_addresstab is not None and self.ssh_check_email_domaintab is not None and self.ssh_base_script is not None
                elif op == 'export':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_jsoc_fetch is not None and self.ssh_base_bin is not None
                elif op == 'export_fits':
                    return self.ssh_export_fits is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'export_from_id':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_jsoc_fetch is not None and self.ssh_base_bin is not None
                elif op == 'export_package':
                    return self.ssh_export_fits is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'info':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'keys':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'parse_spec':
                    return self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'pkeys':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'query':
                    return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'series':
                    return (self.ssh_show_series is not None and self.ssh_base_bin is not None) or (self.ssh_show_series_wrapper is not None and self.ssh_base_script is not None)
                else:
                    return False
        else:
            # not using ssh
            if use_internal:
                show_series_wrapper_available = False

                if self.web_baseurl_internal is None or len(self.web_baseurl_internal) == 0:
                    return False
            else:
                # external
                show_series_wrapper_available = True

                if self.web_baseurl is None or len(self.web_baseurl) == 0:
                    return False

            if op == 'check_email' or op == 'email':
                return self.web_check_address is not None
            elif op == 'keys' or op == 'pkeys' or op == 'info':
                return self.web_jsoc_info is not None
            elif op == 'export' or op == 'export_from_id':
                return self.web_jsoc_info is not None and self.web_jsoc_fetch is not None
            elif op == 'export_package':
                return self.web_export_package is not None and self.web_parse_recset is not None
            elif op == 'parse_spec':
                return self.web_parse_recset is not None
            elif op == 'query':
                return self.web_jsoc_info is not None and self.web_parse_recset is not None
            elif op == 'series':
                return self.web_show_series is not None or (show_series_wrapper_available and self.web_show_series_wrapper is not None)
            else:
                # do not all parent check_supported() - the parent call may have called the child call - otherwise infinite recursion could occur
                return False

    def set_urls(self, use_internal=False, debug=False):
        '''
        if the server is internal, then generate internal web-application URLs, otherwise generate external web-application URLs; store the fully resolved URLs as SecureServerConfig attributes; the names of the SecureServerConfig attributes are derived from the existing 'web' attributes (attributes named 'web_*') by replacing 'web' with 'url' and the values are derived by joining the base URL attribute with the web-attribute value (a script name)

        Parameters
        ----------
        [ debug : bool ]
            if True, print debugging information (default is False)
        [ use_internal : bool ]
            if True, interface programs that access publicly accessible data series will be used; otherwise, programs that access privately accessible series will be used; (default is False)

        Return Value
        -----------
        None
        '''
        if use_internal and self.web_baseurl_internal is not None:
            for url_parameter in self.__urls:
                web_parameter = 'web' + url_parameter[3:]
                web_parameter_val = getattr(self, web_parameter, None)
                setattr(self, url_parameter, urljoin(self.web_baseurl_internal, web_parameter_val))
                if debug:
                    print('[ SecureServerConfig.set_urls ] set URL config parameter {param} to {url}'.format(param=url_parameter, url=urljoin(self.web_baseurl_internal, web_parameter_val)))
        else:
            # set external URLs
            for url_parameter in self.__urls:
                web_parameter = 'web' + url_parameter[3:]
                web_parameter_val = getattr(self, web_parameter, None)
                setattr(self, url_parameter, urljoin(self.web_baseurl, web_parameter_val))
                if debug:
                    print('[ SecureServerConfig.set_urls ] set URL config parameter {param} to {url}'.format(param=url_parameter, url=urljoin(self.web_baseurl, web_parameter_val)))

    @classmethod
    def get(cls, name='__JSOC'):
        '''
        return the SecureServerConfig instance named `name`; raises SecureDRMSArgumentError if there is no configuration named `name`

        Parameters
        ----------
        [ name : str ]
            the name of the server whose configuration is to be returned

        Return Value
        ------------
        object (SecureServerConfig)
        '''
        if len(name) == 0:
            return None

        try:
            return cls.__configs[name.lower()]
        except KeyError:
            raise SecureDRMSArgumentError('configuration ' + name + ' does not exist')

    @classmethod
    def register_server(cls, config):
        '''
        add the server configuration `config` to the dictionary of SecureServerConfigs (SecureServerConfig.__configs); add `config` to the global dictionary of ServerConfigs (config._server_configs)

        Parameters
        ----------
        config : object (SecureServerConfig)

        Return Value
        ------------
        None
        '''
        cls.set(config.name, config)

    @classmethod
    def set(cls, name, config):
        '''
        add a new SecureServerConfig instance, `config`, to cls.__configs, the class dict of configurations

        Parameters
        ----------
        name : str
            name of the server whose configuration is to be stored
        config : SecureServerConfig
            SecureServerConfig instance to be stored

        Return Values
        -------------
        None

        '''
        if isinstance(config, cls):
            cls.__configs[name.lower()] = config

            if isinstance(config, ServerConfig):
                # put this in the global used by the parent ServerConfig class too
                register_server(config)
        else:
            raise SecureDRMSArgumentError('config is of type ' + type(config) + '; must be of type ' + cls.__name__)
    # end public methods

    @property
    def debug(self):
        return self._debug

    @debug.setter
    def debug(self, value):
        self._debug = value

    @property
    def use_internal(self):
        '''
        use SecureServerConfig properties to pass the client's use_internal setting to the parent ServerConfig class; set by SecureClient code, and read by Client code
        '''
        return getattr(self, '_use_internal', None)

    @use_internal.setter
    def use_internal(self, value):
        self._use_internal = value

    @property
    def use_ssh(self):
        '''
        use SecureServerConfig properties to pass the client's use_internal setting to the parent ServerConfig class; set by SecureClient code, and read by Client code
        '''
        return getattr(self, '_use_ssh', None)

    @use_ssh.setter
    def use_ssh(self, value):
        self._use_ssh = value


class OnTheFlyDownloader(object):
    '''
    an abstract class to manage the download of synchronous exported data files; exported files are packaged into a tar file, which is then downloaded to a local directory; afterward, extraction occurs; inside the tar is a JSON status file, jsoc/file_list.json, that provides information about the export process, and the names of the files contained in the tar file; the contents of an example jsoc/file_list.json are:

    {
        "status" : 0,
        "requestid" : null,
        "method" : "url_direct",
        "protocol" : "FITS",
        "dir" : null,
        "wait" : 0,
        "data" :
        [
            {
                "record" : "hmi.M_720s[2017.12.03_00:00:00_TAI][3]{magnetogram}",
                "filename" : "hmi.M_720s.555172.magnetogram.fits"
            },
            {
                "record" : "hmi.M_720s[2017.12.03_00:12:00_TAI][3]{magnetogram}",
                "filename" : "hmi.M_720s.555176.magnetogram.fits"
            } ,
            {
                "record" : "hmi.M_720s[2017.12.03_00:24:00_TAI][3]{magnetogram}",
                "filename" : "hmi.M_720s.555180.magnetogram.fits"
            },
            {
                "record" : "hmi.M_720s[2017.12.03_00:36:00_TAI][3]{magnetogram}",
                "filename" : "hmi.M_720s.555184.magnetogram.fits"
            },
            {
                "record" : "hmi.M_720s[2017.12.03_00:48:00_TAI][3]{magnetogram}",
                "filename" : "hmi.M_720s.555188.magnetogram.fits"
            },
            {
                "record" : "hmi.M_720s[2017.12.03_01:00:00_TAI][3]{magnetogram}",
                "filename" : "hmi.M_720s.555192.magnetogram.fits",
            {
                "record" : "hmi.M_720s[2017.12.03_01:12:00_TAI][3]{magnetogram}",
                "filename":"hmi.M_720s.555196.magnetogram.fits"
            },
            {
                "record" : "hmi.M_720s[2017.12.03_01:24:00_TAI][3]{magnetogram}",
                "filename" : "hmi.M_720s.555200.magnetogram.fits"
            }
        ]
    }
    '''
    def __init__(self, secure_export_request, export_data, debug=False):
        '''
        Parameters
        ----------
        secure_export_request : object (SecureExportRequest)
            the SecureExportRequest object that created the OnTheFlyDownloader instance
        export_data : object (pandas.DataFrame)
            `export_data` is a (record, filename)-pandas.DataFrame; the record is a DRMS record-set specification for a single DRMS record, and the filename is the name of the record's data file as it appears in the downloaded tar file
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._secure_export_request = secure_export_request
        self._export_data = export_data # DataFrame
        self._debug = debug


class BasicAccessOnTheFlyDownloader(OnTheFlyDownloader):
    '''
    a class to manage the HTTP streaming of synchronous FITS exports; instances generate a web-application URL, which can then be used to download content to a specified local directory; the tar file exists as a stream, both on the server and locally - no physical file ever exists

    Attributes
    ----------
    raw_data : bytes
        a byte string of content (the tar file)
    urls : str
        the web-application URL; opening this URL will cause the tar-file data to stream to the client; an example web-application URL:
        http://jsoc.stanford.edu/cgi-bin/drms-export.sh?
            skiptar=true&
            spec=hmi.m_720s%5B2019.2.2%5D&
            filename=hmi.m_720s.%7BT_REC%3AA%7D.%7BCAMERA%7D.%7Bsegment%7D&
            compression=none&
            dbhost=hmidb2&
            webserver=jsoc.stanford.edu
    '''
    def __init__(self, secure_export_request, url, package, debug=False):
        '''
        Parameters
        ----------
        secure_export_request : object (SecureExportRequest)
            the SecureExportRequest object that created the BasicAccessOnTheFlyDownloader instance
        url : str
            the web-application that streams the synchronous export tar file to a specified directory
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._on_the_flyURL = url
        self._response = None
        self._content = None
        self._tar_file = package # absolute path to tar file on client; None for in-memory tar file
        self._tar_file_obj = None
        self._open_archive = None
        self._streams = [] # in-memory FITS files needed for HDUList code to work (it performs lazy loading) [in-memory mode]
        self._hduls = [] # HDUList instances that are opened when extracting data [in-memory mode]
        self._extracted_files = [] # on-disk extracted FITS files [on-disk mode]

        # export_data is None at the moment, since we have yet to download the tar file yet (and the export data
        # are in the jsoc/file_list.json file inside the archive); the export data are in a (record, filename) DataFrame
        # created from the 'data' attribute in the jsoc/file_list.json file
        super().__init__(secure_export_request, None, debug)

    def _extract_data(self, *, data_dir, index=None, fname_from_rec=False, remove_tar=False, verbose=None):
        # two columns, record and filename (inside the tar file)
        if index is not None:
            dl_data = self._export_data.iloc[index].copy()
        else:
            dl_data = self._export_data.copy()

        in_memory = data_dir is None

        if in_memory:
            out_hduls = [] # a list of HDUList objects (from astropy)
            for irec in range(len(dl_data)):
                hdu_list = None
                row = dl_data.iloc[irec]

                if verbose:
                    print('Extracting file {filenum} of {total}...'.format(filenum=str(irec + 1), total=str(len(dl_data))))
                    print('    record: ' + row.record)
                    print('  filename: ' + row.filename)

                try:
                    if self._debug:
                        print('[ BasicAccessOnTheFlyDownloader._extract_data ] extracting file {file} to HDUList'.format(file=row['filename']))

                    with self._open_archive.extractfile(row['filename']) as f_fits:
                        # we must copy the file to an in-memory structure because HDUList code does a lazy-load of
                        # file content
                        stream = io.BytesIO(f_fits.read())
                        self._streams.append(stream)
                        hdu_list = fits.open(stream)
                        out_hduls.append(hdu_list)
                except:
                    if verbose:
                        print('  -> Error: Could not extract file')

            # the tar file is in memory - no filename
            dl_data['filename'] = self._tar_file # may be on disk, or may reside in memory (in which case the value is None)
            dl_data['download'] = out_hduls
            self._hduls.extend(out_hduls)
        else:
            # extract FITS files to disk
            out_paths = []
            for irec in range(len(dl_data)):
                row = dl_data.iloc[irec]
                filename = None

                if fname_from_rec:
                    # if fname_from_rec is None or False, then use the filenames saved in the tar file; otherwise, generate names based upon the series and record prime-key values in the record name
                    filename = self._secure_export_request.client.filename_from_export_record(row.record, old_fname=row.filename)

                if filename is None:
                    # use the filename in the tar file
                    filename = row.filename

                dest_path_unique = self._secure_export_request._next_available_filename(os.path.join(data_dir, filename))
                dest_path_unique_tmp = os.path.join(data_dir, '.' + os.path.basename(dest_path_unique))

                if verbose:
                    print('Extracting file {filenum} of {total}...'.format(filenum=str(irec + 1), total=str(len(dl_data))))
                    print('    record: ' + row.record)
                    print('  filename: ' + row.filename)

                try:
                    if self._debug:
                        print('[ BasicAccessOnTheFlyDownloader.download ] extracting file {file} to {dest}'.format(file=row['filename'], dest=dest_path_unique))

                    with self._open_archive.extractfile(row['filename']) as fin, open(dest_path_unique_tmp, 'wb') as fout:
                        while True:
                            data_bytes = fin.read(8192)
                            if data_bytes == b'':
                                break

                            bytes_written = 0
                            while bytes_written < len(data_bytes):
                                bytes_written += fout.write(data_bytes)

                    # rename the temp file back to final destination (dest_path_unique)
                    os.rename(dest_path_unique_tmp, dest_path_unique)

                    # store path in case user wants to delete the file on close
                    self._extracted_files.append(dest_path_unique)
                except:
                    dest_path_unique = None

                    if verbose:
                        print('  -> Error: Could not extract file')

                out_paths.append(dest_path_unique)

            # the tar file is in memory - no filename
            dl_data['filename'] = None
            dl_data['download'] = out_paths

        if remove_tar:
            self._remove_tar_file(True)
            # the tar is in memory - release it (or remove all references to the memory buffer for async garbage-collection)

            self._content = None
            self._response = None

        return dl_data

    def _remove_tar_file(self, delete_files):
        if self._open_archive is not None:
            self._open_archive.close()
            self._open_archive = None

        if self._tar_file_obj is not None:
            self._tar_file_obj.close()
            self._tar_file_obj = None

        if self._tar_file is None:
            pass # self._tar_file_obj contained the byte stream, and we already closed it
        elif delete_files:
            if os.path.exists(self._tar_file):
                # on disk = remove the tar, locally
                os.remove(self._tar_file)

    def _close(self, delete_files):
        for stream in self._streams:
            stream.close()

        for hdul in self._hduls:
            hdul.close()

        if delete_files:
            for file in self._extracted_files:
                os.remove(file)

        self._remove_tar_file(delete_files)

        self._content = None
        self._response = None

    # public methods
    def download(self, *, data_dir, index=None, fname_from_rec=False, remove_package=True, verbose=None):
        '''
        stream the tar file into memory, and then extract individual data files to `data_dir`; `index` can be used to select the set of files to extract; if `index` is an integer value, then the data file for the record indexed by the value in the self._export_data pandas.DataFrame is extracted; multiple files can be extracted by providing a list of integer values; a value of None causes all files to be extracted

        Parameters
        ----------
        data_dir : str
            the directory to which data files are to be extracted
        [ index : int or list ]
            either an integer value, or a list of integer values, or None; the integer values refer to the indexes of the data files in the tar file that are to be extracted; if None (default) then all data files are extracted
        [ fname_from_rec : bool ]
            if True then the name of each extracted file is generated from the record-set specification for that record
        [ remove_package : bool ]
            if True (default), then the in-memory package file is deallocated, and if the package was saved to disk, it is deleted; if False, then the caller could call download() multiple times
        [ verbose : bool ]
            if True, print export status; if False do not print export status; if None (default), use the verbose attribute from SecureClient

        Return Value
        ------------
        object (('record', 'filename', 'download')-pandas.DataFrame):
            `record` - the record set specification for a single DRMS record
            `filename` - None (normally this would be the URL to the tar file on the server, but no such tar file was ever created)
            `download` - local path to the extracted data file
        '''
        in_memory = self._tar_file is None

        if self._response is None:
            # open the URL
            urldf = self.urls
            if urldf.shape[0] != 1:
                raise SecureDRMSArgumentError('[ BasicAccessOnTheFlyDownloader.download() ] unexpected number of rows in url DataFrame')

            url = urldf.at[0, 'url']

            if verbose is None:
                verbose = self._secure_export_request.client.verbose

            try:
                if self._debug:
                    print('[ BasicAccessOnTheFlyDownloader.download ] opening URL ' + url)

                request = Request(url)

                if self._secure_export_request.client.use_internal:
                    # assume that 'external' (public) websites do not require any kind of authorization
                    if self._debug:
                        print('[ BasicAccessOnTheFlyDownloader.download ] adding Basic Access authorization header to {url}'.format(url=url))

                    try:
                        # self._server is the SecureServerConfig that has the server authority information
                        pass_phrase = self._secure_export_request.client.json_client.server.web_baseurl_authority
                        if pass_phrase is None:
                            pass_phrase = getpass.getpass()
                        request.add_header("Authorization", "Basic " + base64.b64encode(pass_phrase.encode()).decode())
                    except AttributeError:
                        # the user did not provide a pass_phrase
                        if self._debug:
                            print('[ BasicAccessOnTheFlyDownloader.download ] no Basic Access authority provided')

                        pass

                self._response = urlopen(request) # self._response is a http.client.HTTPResponse
                self._content = None # the tar file content; refresh

                # get filename HTTP header; the tarfile that drms-export.sh would like to create appears in the Content-Disposition header
                # (although at this point it is hard-coded as data.tar)
                # with response.info() as info:
                #   local_tarfile = os.path.join(data_dir, os.path.basename(info.get_filename()))
            except URLError as exc:
                raise SecureDRMSUrlError('[ BasicAccessOnTheFlyDownloader.download() ] troubles opening URL ' + url)

        if self._tar_file_obj is None:
            # the tarfile is actually in memory - use self.data and stick into a fileobj
            self._tar_file_obj = io.BytesIO(self.raw_data) # a binary-stream implementaton; reads response tar-file content

        if not in_memory:
            # save on disk
            with open(self._tar_file, 'wb') as f_package:
                f_package.write(self._tar_file_obj.read())

        if self._open_archive is None:
            # file obj must be open for this to work; so if we want to keep the archive open between calls to
            # download() we have to keep both self._tar_file_obj and self._open_archive open (and keep them outside
            # of the
            self._tar_file_obj.seek(0)
            self._open_archive = tarfile.open(fileobj=self._tar_file_obj, mode='r')

        if self._export_data is None:
            fin = self._open_archive.extractfile('jsoc/file_list.json')
            json_dict = load(fin)

            columns = [ 'record', 'filename' ]
            self._export_data = pandas.DataFrame(json_dict['data'], columns=columns)

            if self._debug:
                print('[ BasicAccessOnTheFlyDownloader.download ] extracted (record, filename) export data from file_list.json inside streamed archive')

        dl_data = self._extract_data(data_dir=data_dir, index=index, fname_from_rec=fname_from_rec, remove_tar=remove_package, verbose=verbose)

        # return (single_record_spec, None, local_path_of_data_file) - in the parent class, the second column would normally be URL of the tarfile; however, no such file exists since the tarfile was generated synchronous (no Storage Unit was created)
        return dl_data

    def extract(self, *, data_dir, index=None, fname_from_rec=False, remove_package=False, verbose=None):
        dl_data = None
        try:
            dl_data = self._extract_data(data_dir=data_dir, index=index, fname_from_rec=fname_from_rec, remove_tar=remove_package, verbose=verbose)
        except OSError as error:
            raise SecureDRMSSystemError(error.strerror)

        return dl_data

    def generate_download_url(self):
        '''
        return a pandas.DataFrame that contains information needed to download the tar file of exported data files

        Parameters
        ----------
        None

        Return Value
        ------------
        object (('record', 'filename', 'url')-pandas.DataFrame):
            `record` - normally a record-set specification for a single DRMS record, but in this case, None since the tar file encompasses many DRMS records, so there is no single record-set specification to put here
            `filename` - normally the basename of the record's data file (a tar file), but in this case None since no tar file will ever exist
            `url` - normally a URL path to the data file on the DRMS file server, but in this case the web-application URL that generates the streamed tar file

        this DataFrame is designed to be compatible in format with the parent class; normally this is a table where each row contains the record-set specification of a single DRMS record, the filename of the data file as it resides on the server, and the URL to this server file; however, in this case a tar file, which applies to many DRMS records, is being created, so the table contains a single row of information about the tar file as described above
        '''
        # should return a DataFrame with 3 columns: record, filename, URL; in this case, we have only a single
        # tar file that encapsulates data from many records, so there should be 1 row, and the record column
        # should be None; the tar filename is generated in drms-export.py - so we don't know that either; the url
        # is the CGI url that causes a tar file to be streamed to stdout
        return pandas.DataFrame([ (None, None, self._on_the_flyURL) ], columns=[ 'record', 'filename', 'url' ])

    def remove_package(self):
        # the package is in memory
        self._remove_tar_file(True)

    def close(self, delete_files=True):
        self._close(delete_files)

    # end public methods

    @property
    def raw_data(self):
        '''
        Return Value
        ------------
        object (bytes):
            a byte string of content (the tar file)
        '''
        # binary data
        if self._content is None:
            self._content = self._response.read() # a bytes object (self._response must have some stream implementation in it); CANNOT seek()

        return self._content

    @property
    def urls(self):
        '''
        Return Value
        ------------
        str:
            the web-application URL that, when opened, generates a stream containing the tar datafile containing DRMS-record data files
        '''
        # this is the web-application URL that causes the drms-export-to-stdout code to run and produce a tar containing FITS files;
        # ok, ready for this; this calls the parent urls() method, which calls SecureExportRequest._generate_download_urls()
        # which calls BasicAccessOnTheFlyDownloader.generate_download_url() which returns self._on_the_flyURL in essence
        return super(SecureExportRequest, self._secure_export_request).urls


class SSHOnTheFlyOnestopDownloader(OnTheFlyDownloader):
    '''
    a class to manage the SSH download of synchronous exports; instances can be used to download content to a specified local directory

    Attributes
    ----------
    urls
        not implemented for SSHClients
    '''
    def __init__(self, secure_export_request, export_data, tarfile, local_package, remote_user, remote_host, remote_port, debug=False):
        '''
        Parameters
        ----------
        secure_export_request : object (SecureExportRequest)
            the SecureExportRequest object that created the SSHOnTheFlyOnestopDownloader instance
        export_data : object (pandas.DataFrame)
            a ('record', 'filename')-pandas.DataFrame with one row per DRMS record:
                `record` - the record-set specification for a single DRMS record
                `filename` - the name of the associated data file inside the tar file
        tarfile : str
            absolute server path to the synchronous tar file created on the server
        remote_user : str
            the unix account the ssh command runs as
        remote_host : str
            the server accepting ssh requests
        remote_port : int
            the port of the server to which ssh requests are sent
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._tarfile = tarfile # absolute path on server; None if server tar file has been removed
        self._local_tar_file = local_package
        self._remote_user = remote_user
        self._remote_host = remote_host
        self._remote_port = remote_port
        self._open_archive = None

        super().__init__(secure_export_request, export_data, debug)

    def _remove_sever_tar_file(self):
        if self._tarfile is None:
            raise SecureDRMSArgumentError('[ SSHOnTheFlyOnestopDownloader._remove_sever_tar_file ] server package file already removed')

        cmds =[ '/bin/rm', self._tarfile ]
        ssh_cmd_list = [ '/usr/bin/ssh', '-p', str(self._remote_port), self._remote_user + '@' + self._remote_host, shlex.quote('/bin/bash -c ' + shlex.quote(' '.join(cmds))) ]

        try:
            if self._debug:
                print('[ SSHOnTheFlyOnestopDownloader._extract_data ] running ssh command: {cmd}'.format(cmd=' '.join(ssh_cmd_list)))

            child = pexpect.spawn(' '.join(ssh_cmd_list))
            password_attempted = False
            password_failed = False
            while True:
                # calling rm to remove the tar file from the server; do not increase timeout; the default of 30 seconds should be
                # plenty for the execution of a rm command
                choice = child.expect([ 'password:', pexpect.EOF ])
                if choice == 0:
                    if password_attempted:
                        if self._debug:
                            print('[ SSHOnTheFlyOnestopDownloader._extract_data ] ssh password failed; requesting user re-try')
                        password_failed = True
                    # user was prompted to enter password
                    password = self._secure_export_request.client.json_client.get_password(user_and_host=self._remote_user + '@' + self._remote_host, first_try=(not password_failed))
                    child.sendline(password.encode('UTF8'))
                    password_attempted = True
                else:
                    # no password was required (because the SSH keys and ssh-agent were properly set up)
                    resp = child.before
                    break
            self._tarfile = None
        except AttributeError:
            # a configuration parameter is missing (like ssh_remote_user or ssh_remote_host or ssh_remote_port
            import traceback
            raise SecureDRMSConfigurationError(traceback.format_exc(1))
        except pexpect.exceptions.TIMEOUT:
            raise SecureDRMSTimeOutError('time-out waiting server to respond')

    # data_dir not determined in SecureClient.exp_fits(), so it is required here
    def _download_tar_file(self, *, data_dir):
        if self._tarfile is None:
            raise SecureDRMSArgumentError('[ SSHOnTheFlyOnestopDownloader._remove_sever_tar_file ] server package file has been removed')

        # download tarfile to `self._local_tar_file`; self._tarfile is the server path to the tarfile (as seen from the server)
        if not self._open_archive and not os.path.exists(self._local_tar_file):
            try:
                # scp the tarfile from the server to the local `data_dir` directory
                scp_cmd_list = [ '/usr/bin/scp', '-q', '-P', str(self._remote_port), self._remote_user + '@' + self._remote_host + ':' + self._tarfile, self._local_tar_file ]

                if self._debug:
                    print('[ SSHOnTheFlyOnestopDownloader._download_tar_file ] running scp command: {cmd}'.format(cmd=' '.join(scp_cmd_list)))

                child = pexpect.spawn(' '.join(scp_cmd_list))
                password_attempted = False
                password_failed = False
                while True:
                    # big timeout for potential slow or large download
                    choice = child.expect([ 'password:', pexpect.EOF ], timeout=1024)
                    if choice == 0:
                        if password_attempted:
                            if self._debug:
                                print('[ SSHOnTheFlyOnestopDownloader._download_tar_file ] scp password failed; requesting user re-try')
                            password_failed = True
                        # user was prompted to enter password
                        password = self._secure_export_request.client.json_client.get_password(user_and_host=self._remote_user + '@' + self._remote_host, first_try=(not password_failed))
                        child.sendline(password.encode('UTF8'))
                        password_attempted = True
                    else:
                        # no password was required (because the SSH keys and ssh-agent were properly set up)
                        resp = child.before
                        break
            except AttributeError:
                # a configuration parameter is missing (like ssh_remote_user or ssh_remote_host or ssh_remote_port
                import traceback
                raise SecureDRMSConfigurationError(traceback.format_exc(1))
            except pexpect.exceptions.TIMEOUT:
                raise SecureDRMSTimeOutError('[ SSHOnTheFlyOnestopDownloader._download_tar_file ] time-out waiting server to respond')
            finally:
                # always remove server tar file
                self._remove_sever_tar_file()

    def _remove_tar_file(self):
        # remove local, downloaded copy of tar file
        if self._debug:
            print('[ SSHOnTheFlyOnestopDownloader._remove_tar_file ] closing and removing tar file')

        if os.path.exists(self._local_tar_file):
            # the tar is in memory - release it (or remove all references to the memory buffer for async garbage-collection)
            if self._open_archive is not None:
                self._open_archive.close()
                self._open_archive = None

            # remove the local tar file
            os.remove(self._local_tar_file)

    def _download_and_open_tar_file(self, *, data_dir):
        if not os.path.exists(self._local_tar_file):
            self._download_tar_file(data_dir=data_dir)

        if not self._open_archive:
            self._open_archive = tarfile.open(name=self._local_tar_file, mode='r')

    def _extract_data(self, *, data_dir, index=None, fname_from_rec=False, remove_tar=False, verbose=None):
        if self._export_data is None:
            # this should have come from the json response when the server synchornous export call was made
            raise SecureDRMSArgumentError('missing drms-server-generated export data ')

        # two columns, record and filename (inside the tar file)
        if index is not None:
            dl_data = self._export_data.iloc[index].copy()
        else:
            dl_data = self._export_data.copy()

        out_paths = []
        for irec in range(len(dl_data)):
            row = dl_data.iloc[irec]
            filename = None

            if fname_from_rec:
                # if fname_from_rec is None or False, then use the filenames saved in the tar file; otherwise, generate names based upon the series and record prime-key values in the record name
                filename = self._secure_export_request.client.filename_from_export_record(row.record, old_fname=row.filename)

            if filename is None:
                # use the filename in the tar file
                filename = row.filename

            dest_path_unique = self._secure_export_request._next_available_filename(os.path.join(data_dir, filename))
            dest_path_unique_tmp = os.path.join(data_dir, '.' + os.path.basename(dest_path_unique))

            if verbose:
                print('Extracting file {filenum} of {total}...'.format(filenum=str(irec + 1), total=str(len(dl_data))))
                print('    record: ' + row.record)
                print('  filename: ' + row.filename)

            try:
                if self._debug:
                    print('[ SSHOnTheFlyOnestopDownloader._extract_data ] extracting file {file} to {dest}'.format(file=row['filename'], dest=dest_path_unique))

                with self._open_archive.extractfile(row['filename']) as fin, open(dest_path_unique_tmp, 'wb') as fout:
                    while True:
                        data_bytes = fin.read(8192)
                        if data_bytes == b'':
                            break

                        bytes_written = 0
                        while bytes_written < len(data_bytes):
                            bytes_written += fout.write(data_bytes)

                # rename the temp file back to final destination (dest_path_unique)
                os.rename(dest_path_unique_tmp, dest_path_unique)
            except:
                dest_path_unique = None

                if verbose:
                    print('  -> Error: Could not extract file')

            out_paths.append(dest_path_unique)

        if remove_tar:
            self._remove_tar_file()

        # we no longer need row.filename -> replace with local_tarfile so this can be returned to user (in case
        # they want to examine the downloaded tar file)
        dl_data['filename'] = self._local_tar_file
        dl_data['download'] = out_paths

        # return (record_spec, basename_local_tarfile, local_path), one row per DRMS record - in the parent class, the second column would normally be URL of the tar file (for tarfile exports); in this class, the tarfile exists locally, if it has not been removed, and its name is local_tarfile (if the tarfile has been removed, then local_tarfile is None)
        return dl_data

    # public methods
    def download(self, *, data_dir, index=None, fname_from_rec=False, remove_package=False, verbose=None):
        '''
        download the tar file from the server to `data_dir`, and then extract individual data files to `data_dir`; `index` can be used to select the set of files to extract; if `index` is an integer value, then the data file for the record indexed by the value in the self._export_data pandas.DataFrame is extracted; multiple files can be extracted by providing a list of integer values; a value of None causes all files to be extracted

        Parameters
        ----------
        data_dir : str
            the directory to which data files are to be extracted
        [ index : int or list ]
            either an integer value, or a list of integer values, or None; the integer values refer to the indexes of the data files in the tar file that are to be extracted; if None (default) then all data files are extracted
        [ fname_from_rec : bool ]
            if True then the name of each extracted file is generated from the record-set specification for that record
        [ remove_package : bool ]
            if True, then the in-memory tar file is deallocated and the local package file is deleted; if False (the default), then the caller could call download() multiple times
        [ verbose : bool ]
            if True, print export status; if False do not print export status; if None (default), use the verbose attribute from SecureClient

        Return Value
        ------------
        object (('record', 'filename', 'download')-pandas.DataFrame):
            `record` - the record set specification for a single DRMS record
            `filename` - the absolute path to the downloaded tar file (all rows have the same path)
            `download` - local path to the extracted data file
        '''
        if data_dir is None:
            raise SecureDRMSArgumentError('missing argument `data_dir`')

        if verbose is None:
            verbose = self._secure_export_request.client.verbose

        dl_data = None
        try:
            self._download_and_open_tar_file(data_dir=data_dir)
            dl_data = self._extract_data(data_dir=data_dir, index=index, fname_from_rec=fname_from_rec, remove_tar=remove_package, verbose=verbose)
        except OSError as error:
            raise SecureDRMSSystemError(error.strerror)

        return dl_data

    def extract(self, *, data_dir, index=None, fname_from_rec=False, remove_package=False, verbose=None):
        '''

        '''
        if verbose is None:
            verbose = self._secure_export_request.client.verbose

        if type(index) == list and len(index) == 0:
            # prohibited - cannot extract no data files (makes no sense)
            raise SecureDRMSArgumentError('must specify at least one record for extraction')

        if self._local_tar_file is None or self._open_archive is None:
            raise SecureDRMSArgumentError('cannot extract data before package has been downloaded and opened')

        if self._debug:
            print('[ SSHOnTheFlyOnestopDownloader.extract ] extracting data from open package file')

        dl_data = None
        try:
            dl_data = self._extract_data(data_dir=data_dir, index=index, fname_from_rec=fname_from_rec, remove_tar=remove_package, verbose=verbose)
        except OSError as error:
            raise SecureDRMSSystemError(error.strerror)

        return dl_data

    def remove_package(self):
        '''
        close in-memory data and remove the package from local storage
        '''
        if self._debug:
            print('[ SSHOnTheFlyOnestopDownloader.remove_package ] closing and removing package file')

        try:
            self._remove_tar_file()
        except OSError as error:
            raise SecureDRMSSystemError(error.strerror)

    def close(self):
        self._remove_tar_file()

    def generate_download_url(self):
        '''
        not supported in SSHClients
        '''
        # in the parent, this creates one url per DRMS data-series record
        raise SecureDRMSMethodError('[ _generate_download_url ] cannot generate urls for an SSH configuration')

    # end public methods

    @property
    def urls(self):
        raise SecureDRMSMethodError('[ urls ] urls not relevant to an SSH configuration')

class SSHOnTheFlyNonstopDownloader(OnTheFlyDownloader):
    '''
    a subclass that both generates the export package and streams it back to the client; there is no intermediate step of creating a server-side package file
    '''
    def __init__(self, secure_export_request, server_response, remote_user, remote_host, remote_port, debug=False):
        '''
        Parameters
        ----------
        secure_export_request : object (SecureExportRequest)
            the SecureExportRequest object that created the SSHOnTheFlyNonstopDownloader instance
        server_response : obj
            the data returned to the client in response to the client.export_package() call; it looks like this
            {
                "status" : 0,
                "on-the-fly-command" : cmd_list,
                "package" : download_path,
                "json_response" : "jsoc/file_list.json"
            }
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._drms_server_response_file = server_response['json_response'] # file in tar with json response
        self._on_the_fly_command = server_response['on-the-fly-command']
        self._tar_file = server_response['package'] # absolute path to tar file on client
        self._remote_user = remote_user
        self._remote_host = remote_host
        self._remote_port = remote_port
        self._tar_file_obj = None # file object; either an open self._tar_file (on-disk), or byte stream (in-memory)
        self._open_archive = None # tarfile obj representing an open archive
        self._streams = [] # in-memory FITS files needed for HDUList code to work (it performs lazy loading) [in-memory mode]
        self._hduls = [] # HDUList instances that are opened when extracting data [in-memory mode]
        self._extracted_files = [] # on-disk extracted FITS files [on-disk mode]

        # export_data is None at the moment, since we have yet to download the tar file yet (and the export data
        # are in the jsoc/file_list.json file inside the archive); the export data are in a (record, filename) DataFrame
        # created from the 'data' attribute in the jsoc/file_list.json file
        super().__init__(secure_export_request, None, debug) # second arg is export_data, it does not yet exist

    async def _write_tar_file(self, command_string):
        if self._tar_file_obj is None:
            raise SecureDRMSArgumentError('must first open tar file before attempting to write to it')

        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader._write_tar_file ] starting child process')

        proc = await asyncio.subprocess.create_subprocess_shell(command_string, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)

        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader._write_tar_file ] interacting with child process')

        self._tar_file_obj.write(await proc.stdout.read()) # disk file
        await proc.wait()

        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader._write_tar_file ] child process terminated')

    def _download_tar_file(self):
        # this really executes a program on the DRMS server; theoretically, it could be called more than once
        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader._download_tar_file ] downloading tar file')

        do_download = False

        if self._tar_file is None:
            # create an in-memory file (no disk file)
            in_memory = True
            if self._tar_file_obj is None:
                do_download = True
                self._tar_file_obj = io.BytesIO()
        else:
            # download to disk
            in_memory = False
            if not os.path.exists(self._tar_file):
                do_download = True
                self._tar_file_obj = open(self._tar_file, mode='w+b') # tar file will reside in memory and on disk

        if do_download:
            ssh_cmd_list = [ '/usr/bin/ssh', '-p', str(self._remote_port), self._remote_user + '@' + self._remote_host, shlex.quote('/bin/bash -c ' + shlex.quote(' '.join(self._on_the_fly_command))) ]

            if self._debug:
                print('[ SSHOnTheFlyNonstopDownloader._download_tar_file ] running ssh command: {cmd}'.format(cmd=' '.join(ssh_cmd_list)))

            # use asyncio
            asyncio.run(self._write_tar_file(' '.join(ssh_cmd_list)))

            if self._debug:
                print('[ SSHOnTheFlyNonstopDownloader._download_tar_file ] DONE reading data from child')

    def _remove_tar_file(self, delete_files):
        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader._remove_tar_file ] closing and removing tar file')

        # the tar is in memory - release it (or remove all references to the memory buffer for async garbage-collection)
        if self._open_archive is not None:
            self._open_archive.close()
            self._open_archive = None

        if self._tar_file_obj is not None:
            self._tar_file_obj.close()
            self._tar_file_obj = None

        if self._tar_file is None:
            pass # self._tar_file_obj contained the byte stream, and we already closed it
        elif delete_files:
            if os.path.exists(self._tar_file):
                # on disk = remove the tar, locally
                os.remove(self._tar_file)

    def _close(self, delete_files):
        for stream in self._streams:
            stream.close()

        for hdul in self._hduls:
            hdul.close()

        if delete_files:
            for file in self._extracted_files:
                os.remove(file)

        self._remove_tar_file(delete_files)

    def _download_and_open_tar_file(self):
        if self._export_data is not None:
            raise SecureDRMSArgumentError('tar file has already been downloaded and opened')

        if self._tar_file_obj is None:
            self._download_tar_file()

        # seek to beginning of file
        self._tar_file_obj.seek(0)

        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader._download_and_open_tar_file ] opening tar file')

        # the package has been successfully downloaded to the client

        if self._open_archive is None:
            # file obj must be open for this to work; so if we want to keep the archive open between calls to
            # download() we have to keep both self._tar_file_obj and self._open_archive open (and keep them outside
            # of the
            self._open_archive = tarfile.open(fileobj=self._tar_file_obj, mode='r')

        if self._export_data is None:
            with self._open_archive.extractfile(self._drms_server_response_file) as f_response:
                json_dict = load(f_response)

        columns = [ 'record', 'filename' ]
        self._export_data = pandas.DataFrame(json_dict['data'], columns=columns)

        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader._download_and_open_tar_file ] extracted (record, filename) export data from file_list.json inside streamed archive')

    def _extract_data(self, *, data_dir=None, index=None, fname_from_rec=False, remove_tar=False, verbose=None):
        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader._extract_data ] extracting data from open tar file')

        in_memory = data_dir is None

        if self._export_data is None:
            raise SecureDRMSArgumentError('tar file has has not been downloaded')

        # two columns, record and filename (inside the tar file)
        if index is not None:
            dl_data = self._export_data.iloc[index].copy()
        else:
            dl_data = self._export_data.copy()

        if in_memory:
            out_hduls = [] # a list of HDUList objects (from astropy)
            for irec in range(len(dl_data)):
                hdu_list = None
                row = dl_data.iloc[irec]

                if verbose:
                    print('Extracting file {filenum} of {total}...'.format(filenum=str(irec + 1), total=str(len(dl_data))))
                    print('    record: ' + row.record)
                    print('  filename: ' + row.filename)

                try:
                    if self._debug:
                        print('[ SSHOnTheFlyNonstopDownloader._extract_data ] extracting file {file} to HDUList'.format(file=row['filename']))

                    with self._open_archive.extractfile(row['filename']) as f_fits:
                        # we must copy the file to an in-memory structure because HDUList code does a lazy-load of
                        # file content
                        stream = io.BytesIO(f_fits.read())
                        self._streams.append(stream)
                        hdu_list = fits.open(stream)
                        out_hduls.append(hdu_list)
                except:
                    if verbose:
                        print('  -> Error: Could not extract file')

            dl_data['filename'] = self._tar_file
            dl_data['download'] = out_hduls
            self._hduls.extend(out_hduls)
        else:
            out_paths = []
            for irec in range(len(dl_data)):
                row = dl_data.iloc[irec]
                filename = None

                if fname_from_rec:
                    # if fname_from_rec is None or False, then use the filenames saved in the tar file; otherwise, generate names based upon the series and record prime-key values in the record name
                    filename = self._secure_export_request.client.filename_from_export_record(row.record, old_fname=row.filename)

                if filename is None:
                    # use the filename in the tar file
                    filename = row.filename

                if self._debug:
                    print(f'[ SSHOnTheFlyNonstopDownloader._extract_data ] extracting to directory {data_dir}, filename {filename}')

                dest_path_unique = self._secure_export_request._next_available_filename(os.path.join(data_dir, filename))
                dest_path_unique_tmp = os.path.join(data_dir, '.' + os.path.basename(dest_path_unique))

                if verbose:
                    print('Extracting file {filenum} of {total}...'.format(filenum=str(irec + 1), total=str(len(dl_data))))
                    print('    record: ' + row.record)
                    print('  filename: ' + row.filename)

                try:
                    if self._debug:
                        print('[ SSHOnTheFlyNonstopDownloader._extract_data ] extracting file {file} to {dest}'.format(file=row['filename'], dest=dest_path_unique))

                    with self._open_archive.extractfile(row['filename']) as fin, open(dest_path_unique_tmp, 'wb') as fout:
                        while True:
                            data_bytes = fin.read(8192)
                            if data_bytes == b'':
                                break

                            bytes_written = 0
                            while bytes_written < len(data_bytes):
                                bytes_written += fout.write(data_bytes)

                    # rename the temp file back to final destination (dest_path_unique)
                    os.rename(dest_path_unique_tmp, dest_path_unique)

                    # store path in case user wants to delete the file on close
                    self._extracted_files.append(dest_path_unique)
                except:
                    dest_path_unique = None

                    if verbose:
                        print('  -> Error: Could not extract file')

                out_paths.append(dest_path_unique)

            # the tar file is in memory - no filename
            dl_data['filename'] = None if remove_tar else self._tar_file
            dl_data['download'] = out_paths

        if remove_tar:
            self._remove_tar_file(True)

        # return (single_record_spec, None, local_path_of_data_file) - in the parent class, the second column would normally be URL of the tarfile; however, no such file exists since the tarfile was generated synchronous (no Storage Unit was created)
        return dl_data

    # public methods
    def download(self, *, data_dir=None, index=None, fname_from_rec=False, remove_package=False, verbose=None):
        '''
        download the package file to local storage, optionally extracting data files; the local path to the package file was determined by SecureClient.export_package(), but that can be overriden with `data_dir`

        Parameters
        ----------
        [ data_dir : str ]
            the directory to which the data files are to be extracted; if None, then the data files are extracted into memory and no disk files are created
        [ index : int or list ]
            either an integer value, or a list of integer values, or None; the integer values refer to the indexes of the data files in the tar file that are to be extracted; if the value is None (default), then all data files are extracted; if the value is [], an empty list, then no data files are extracted
        [ fname_from_rec : bool ]
            if True then the name of each extracted file is generated from the record-set specification for that record
        [ remove_package : bool ]
            if True, then after the package is downloaded and any data files have been extracted, the in-memory tar file is deallocated and the local disk file is deleted; if False (the default), then additional data-file can be extracted with SecureClient.extract()
        [ verbose : bool ]
            if True, print export status; if False do not print export status; if None (default), use the verbose attribute from SecureClient

        Return Value
        ------------
        object (('record', 'filename', 'download')pandas.DataFrame):
            `record` - the record set specification for a single DRMS record
            `filename` - None (normally this would be the URL to the tar file on the server, but no such tar file was ever created)
            `download` - local path to the extracted data files
        '''
        if verbose is None:
            verbose = self._secure_export_request.client.verbose

        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader.download ] downloading and opening package file, extracting data')

        dl_data = None
        try:
            self._download_and_open_tar_file()
            dl_data = self._extract_data(data_dir=data_dir, index=index, fname_from_rec=fname_from_rec, remove_tar=remove_package, verbose=verbose)
        except OSError as error:
            raise SecureDRMSSystemError(error.strerror)

        return dl_data

    def extract(self, *, data_dir=None, index=None, fname_from_rec=False, remove_package=False, verbose=None):
        '''
        extract one or more fits files from the downloaded, and possibly in memory, archive

        Parameters
        ----------
        [ data_dir : str ]
            the directory to which the data files are to be extracted; if None, then data files are in-memory
        [ index : int or list ]
            either an integer value, or a list of integer values, or None; the integer values refer to the indexes of the data files in the tar file that are to be extracted; if None (default) then all data files are extracted; the emtpy list [] is prohibited
        [ fname_from_rec : bool ]
            if True then the name of each extracted file is generated from the record-set specification for that record
        [ remove_package : bool ]
            if True, then after the package is downloaded and any data files have been extracted, the in-memory tar file is deallocated and the local disk file is deleted; if False (the default), then additional data-file can be extracted with SecureClient.extract()
        [ verbose : bool ]
            if True, print export status; if False do not print export status; if None (default), use the verbose attribute from SecureClient

        Return Value
        ------------
        object (('record', 'filename', 'download')pandas.DataFrame):
            `record` - the record set specification for a single DRMS record
            `filename` - None (normally this would be the URL to the tar file on the server, but no such tar file was ever created)
            `download` - local path to the extracted data files
        '''
        if verbose is None:
            verbose = self._secure_export_request.client.verbose

        if type(index) == list and len(index) == 0:
            # prohibited - cannot extract no data files (makes no sense)
            raise SecureDRMSArgumentError('must specify at least one record for extraction')

        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader.extract ] extracting data from open package file')

        dl_data = None
        try:
            dl_data = self._extract_data(data_dir=data_dir, index=index, fname_from_rec=fname_from_rec, remove_tar=remove_package, verbose=verbose)
        except OSError as error:
            raise SecureDRMSSystemError(error.strerror)

        return dl_data

    def remove_package(self):
        '''
        close in-memory data and remove the package from local storage
        '''
        if self._debug:
            print('[ SSHOnTheFlyNonstopDownloader.remove_package ] closing and removing package file')

        try:
            self._remove_tar_file(True)
        except OSError as error:
            raise SecureDRMSSystemError(error.strerror)

    def close(self, delete_files=True):
        # in-memory files
        self._close(delete_files)

class SecureExportRequest(ExportRequest):
    '''
    a class that manages submitted export requests; properties provide access to export-request information, and methods allow the caller to download exported files

    Attributes
    ----------
    reqid : str
        export request ID
    requestid : str
        export request ID (alternate synonymous to `reqid`)
    status : int
        export request status
    urls : object (pandas.DataFrame)
        URLs of all downloadable files
    request_url : string
        URL of the export request.
    method : string
        export method (url, url_quick, url-tar, url_direct, ftp, ftp-tar)
    protocol : str
        export protocol ()
    data : object (pandas.DataFrame)
        record-specification strings and data file filenames of the export request
    size: int
        total size in bytes of exported files
    error_msg: str
        if an error has occurred, this may contain a message
    contact: str
        if an error has occurred, this may contain a contact email address
    dir : str
        export-request Storage Unit directory
    tarfile : str
        tar filename (if export method is ftp or ftp-tar)
    keywords : str
        name of textfile in `dir` containing DRMS record keyword values

    not all attributes are always present; they are export-method-dependent
    '''
    def __init__(self, server_response, secure_client, remote_user=None, remote_host=None, remote_port=None, on_the_fly=True, defer_package=True, debug=False):
        '''
        Parameters
        ----------
        server_response : dict (Python representation of JSON)
            `server_response` contains the export-request information, in JSON format, returned by the server in response to an export request (request status, export method used, protocol used, tar file (if export method is ftp or ftp-tar), directory of exported files, suggested wait-time interval, exported record and data-file data); example `server_response` text (other attributes may be present, or some may be omitted, depending on the export method used):
            {
                "status" : 0,
                "requestid" : null,
                "method" : "url_direct",
                "protocol" : "FITS",
                "dir" : null,
                "wait" : 0,
                "tarfile" : "/tmp/.46680c4a-c004-4b67-9d7c-0d65412d8c94.tar"
                "data" :
                [
                    {
                        "record" : "hmi.M_720s[2017.12.03_00:00:00_TAI][3]{magnetogram}",
                        "filename" : "hmi.M_720s.555172.magnetogram.fits"
                    },
                    {
                        "record" : "hmi.M_720s[2017.12.03_00:12:00_TAI][3]{magnetogram}",
                        "filename" : "hmi.M_720s.555176.magnetogram.fits"
                    },
                    {
                        "record" : "hmi.M_720s[2017.12.03_00:24:00_TAI][3]{magnetogram}",
                        "filename" : "hmi.M_720s.555180.magnetogram.fits"
                    },
                    {
                        "record" : "hmi.M_720s[2017.12.03_00:36:00_TAI][3]{magnetogram}",
                        "filename" : "hmi.M_720s.555184.magnetogram.fits"
                    },
                    {
                        "record" : "hmi.M_720s[2017.12.03_00:48:00_TAI][3]{magnetogram}",
                        "filename" : "hmi.M_720s.555188.magnetogram.fits"
                    },
                    {
                        "record" : "hmi.M_720s[2017.12.03_01:00:00_TAI][3]{magnetogram}",
                        "filename" : "hmi.M_720s.555192.magnetogram.fits"
                    },
                    {
                        "record" : "hmi.M_720s[2017.12.03_01:12:00_TAI][3]{magnetogram}",
                        "filename":"hmi.M_720s.555196.magnetogram.fits"
                    },
                    {
                        "record" : "hmi.M_720s[2017.12.03_01:24:00_TAI][3]{magnetogram}",
                        "filename" : "hmi.M_720s.555200.magnetogram.fits"
                    }
                ]
            }

        secure_client : SecureClient
            `secure_client` is a reference back to the SecureClient that was used to initiate the export process
        [ remote_user : str ]
            the unix account the ssh command runs as; although this is indicated as optional, it is actually required for an synchronous export from a SSHClient secure_client
        [ remote_host : str ]
            the server accepting ssh requests; although this is indicated as optional, it is actually required for an synchronous export from a SSHClient secure_client
        [ on_the_fly : bool ]
            if True, then the export request is processed asynchronously
        [ defer_package : bool ]
            if True, then download() creates package and downloads it, otherwise, download() downloads
            the package that was previously created on the server
        [ remote_port : int ]
            the port of the server to which ssh requests are sent; although this is indicated as optional, it is actually required for an synchronous export from a SSHClient secure_client
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        # if the DRMS server supports formal exports (has_full_export_system == True) and the user has chosen an export method that makes use of the full export system (url, url_quick, url-tar, ftp, ftp-tar) or synchronous_export == False (in which case the method cannot be url_direct) then the parent-class (ExportRequest) code will be used to download data files; and if the user has chosen either the ftp or ftp-tar export method, then a tar file will be created on the server, and the ExportRequest.download() method will download the tar file to the local directory specified by the user; when a tar file is created, jsoc_export_make_index creates a 'tarfile' attribute that appears in the JSON response to the ExportRequest.export_from_id() call; the tarfile attribute specifies the location of the tar file on the server, and this attribute is used by ExportRequest.download() to create a URL path to the tar file
        # otherwise if the DRMS server does not support formal exports OR the user has specified synchronous_export == True in SecureExportRequest.export() OR the user called SecureExportRequest.export_fits(), then the server performs a synchronous export; if the user is employing an SSHClient, a tar file is created on the server whose path is stored in server_response['tarfile']; if the user is employing a BasicAccessClient, then no tar file is ever created on the server - instead a stream is created, over which the content of a tar file is sent directly into the securedrms.py memory; opening the url in server_response['url'] causing this tar-file streaming to occur

        # initialize parent first - needed for self.data; `server_response` must be a json dict with a 'data' attribute;
        # if server_response['data'] ==> 'data' not in self (the case for BasicAccessOnTheFlyDownloader and SSHOnTheFlyNonstopDownloader, but not SSHOnTheFlyOnestopDownloader)
        super().__init__(server_response, secure_client)

        self._on_the_fly = on_the_fly
        self._debug = debug
        if isinstance(secure_client, SSHClient):
            self._isssh = True
        else:
            self._isssh = False

        if self._on_the_fly:
            # self.data calls ExportRequest.data() - a (record, filename) DataFrame
            if self._isssh:
                if defer_package:
                    # the package does not yet exist; it will be created on the client during the execution of the download() method;
                    # at that time, the data attribute will also be created
                    self._downloader = SSHOnTheFlyNonstopDownloader(self, server_response, remote_user, remote_host, remote_port, debug)
                else:
                    # the package exists on the server; the data attribute exists (in parent)
                    self._downloader = SSHOnTheFlyOnestopDownloader(self, self.data, server_response['tarfile'], server_response['package'], remote_user, remote_host, remote_port, debug)
            else:
                # the package does not yet exist; it will be created on the client during the execution of the download() method;
                # at that time, the data attribute will also be created
                self._downloader = BasicAccessOnTheFlyDownloader(self, server_response['url'], server_response['package'], debug)
        else:
            self._downloader = None

    # private methods
    def _generate_download_urls(self):
        '''
        generate download URLs for the current request; for synchronous HTTP exports, this is a web-application URL that causes tar-file content to stream to client; for synchronous SSH exports, this is N/A; for non-synchronous exports, there is one URL per DRMS record data file
        '''
        # in the parent, this creates one url per DRMS data-series record
        if self._downloader is None:
            # use the parent code by default
            return super()._generate_download_urls()
        else:
            # some kind of synchronous download (so there is one url, if any)
            return self._downloader.generate_download_url()

    def _parse_export_recset(self, spec):
        '''
        return a series name, prime-key keywords, and segments tuple for each DRMS record
        '''
        parsed = self._client.parse_spec(spec)
        if parsed['nsubsets'] > 1:
            raise

        set = parsed['subsets'][0]
        sname = set['seriesname']
        pkeys = set['filter'].replace('[', ' ').replace(']', ' ').split()
        segs = set['segments'].replace('{', ' ').replace('}', ' ').strip().split(',')

        return (sname, pkeys, segs)
    # end private methods

    # public methods
    def download(self, *, directory=None, index=None, fname_from_rec=None, remove_package=False, verbose=None):
        '''
        download the data files or tar file of data files to a local directory

        Parameters
        ----------
        [ directory : str ]
            the directory to which data files are to be extracted; the package file local directory will be used by default
        [ index : int or list ]
            either an integer value, or a list of integer values, or None; the integer values refer to the indexes of the data files in the tar file that are to be extracted; if None (default) then all data files are extracted
        [ fname_from_rec : bool ]
            if True then the name of each extracted file is generated from the record-set specification for that record
        [ remove_package : bool ]
            if True, then the in-memory package file is deallocated; if False (the default), then the caller could call download() multiple times
        [ verbose : bool ]
            if True, print export status; if False do not print export status; if None (default), use the verbose attribute from SecureClient

        Return Value
        ------------
        object (('record', 'filename', 'download')pandas.DataFrame):
            the column values depend on the type of SecureClient used and are described in BasicAccessOnTheFlyDownloader.download(), SSHOnTheFlyOnestopDownloader.download(), SSHOnTheFlyNonstopDownloader.download()
        '''
        if self._downloader is None:
            return super().download(directory, index, fname_from_rec, verbose)

        # synchronous
        if directory is not None:
            data_dir = os.path.abspath(directory)
        else:
            data_dir = None

        # make a list out of index
        if numpy.isscalar(index):
            index = [ int(index) ]
        elif index is not None:
            index = list(index)

        if verbose is None:
            verbose = self._client.verbose

        return self._downloader.download(data_dir=data_dir, index=index, fname_from_rec=fname_from_rec, remove_package=remove_package, verbose=verbose)

    def extract(self, *, directory=None, index=None, fname_from_rec=None, remove_package=False, verbose=None):
        '''
        extract one or more fits files from the downloaded, and possibly in memory, archive

        Parameters
        ----------
        [ directory : str ]
            the directory to which data files are to be extracted; the package file local directory will be used by default
        [ index : int or list ]
            either an integer value, or a list of integer values, or None; the integer values refer to the indexes of the data files in the tar file that are to be extracted; if None (default) then all data files are extracted
        [ fname_from_rec : bool ]
            if True then the name of each extracted file is generated from the record-set specification for that record
        [ remove_package : bool ]
            if True, then the in-memory package file is deallocated; if False (the default), then the caller could call download() multiple times
        [ verbose : bool ]
            if True, print export status; if False do not print export status; if None (default), use the verbose attribute from SecureClient

        Return Value
        ------------
        object (('record', 'filename', 'download')pandas.DataFrame):
            the column values depend on the type of SecureClient used and are described in BasicAccessOnTheFlyDownloader.download(), SSHOnTheFlyOnestopDownloader.download(), SSHOnTheFlyNonstopDownloader.download()
        '''
        return self._downloader.extract(data_dir=directory, index=index, fname_from_rec=fname_from_rec, remove_package=remove_package, verbose=verbose)

    def remove_package(self):
        self._downloader.remove_package()

    def close(self):
        self._downloader.close()

    # end public methods

    @property
    def client(self):
        return self._client

    @property
    def request_url(self):
        '''
        return the URL to the export Storage Unit
        '''
        if self._downloader is None:
            # not an synchronous export
            return super().request_url

        # there is no export Storage Unit for synchronous exports
        raise SecureDRMSMethodError('[ SecureExportRequest.request_url ] no export SU directory is created for an synchronous export request')

    @property
    def urls(self):
        if self._downloader is None:
            super().urls

        return self._downloader.urls

    @property
    def size(self):
        return self._d.get('size')

    @property
    def error_msg(self):
        return self._d.get('error')

    @property
    def contact(self):
        return self._d.get('contact')


class BasicAccessHttpJsonRequest(HttpJsonRequest):
    '''
    a class to send web-server requests (HTTP URLs, with Basic Access authentication) to servers that return JSON to the DRMS client; for each request, the class processes the JSON response, decoding it into a dictionary which is then available to the calling JSON client

    Attributes
    ----------
    data : dict
        a dict representation of the JSON response to the server HTTP request
    raw_data : str
        the JSON response to the server HTTP request
    url : str
        the URL from the server http.client.HTTPResponse

    '''
    def __init__(self, request, encoding, debug=False):
        '''
        Parameters
        ----------
        request : object (urllib.request.Request)
            abstraction of an HTTP URL request; BasicAccessHttpJsonRequest._http is a http.client.HTTPResponse
        encoding : str
            the name of one of the following JSON encodings: UTF-8, UTF-16, or UTF-32
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._request = request
        self._encoding = encoding
        self._debug = debug
        self._http = sixUrlRequest.urlopen(request) # http.client.HTTPResponse
        self._data_str = None
        self._data = None
        # do not call parent's __init__() since that method calls urlopen without first making a Request; we need to make a Request so we can add the authentication header

    def __repr__(self):
        return '<BasicAccessHttpJsonRequest "{name}"'.format(name=self._request.full_url)

    @property
    def data(self):
        try:
            if self._debug:
                print('[ BasicAccessHttpJsonRequest.data ] JSON response: {json}'.format(json=self.raw_data.decode(self._encoding)))

            json_dict = super().data
        except decoder.JSONDecodeError:
            raise SecureDRMSResponseError('invalid JSON response: ' + self.raw_data.decode(self._encoding))

        return json_dict


class BasicAccessHttpJsonClient(HttpJsonClient):
    '''
    a class that provides AJAX access to the DRMS server; for each web-application, the class creates a BasicAccessHttpJsonRequest; it then collects the dictionary response from the BasicAccessHttpJsonRequest instance, processes it (if needed), and provides the result back to the BasicAccessClient

    Attributes
    ----------
    server : SecureServerConfig
        the configuration object for the DRMS server to which instances connect
    debug : bool
        if True, print debugging statements
    use_internal : bool
        if True, then access will be to privately accessible data series, otherwise access will be to publicly accessible series (external DRMS data series plus selectively exposed internal data series)
    use_ssh : bool
        if True, use the SSH-access methods supported by the secure server; otherwise use the HTTP-access methods; ALWAYS False
    '''
    def __init__(self, config, use_internal=False, debug=False):
        '''
        Parameters
        ----------
        config : SecureServerConfig
            a secure server configuration
        [ use_internal : bool ]
            if True, then access will be to privately accessible data series, otherwise access will be to publicly accessible series (external DRMS data series plus selectively exposed internal data series); (default is False)
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        super().__init__(config.name, debug) # internally, self._debug, externally BasicAccessHttpJsonClient.debug
        self._use_ssh = False
        self._use_internal = use_internal

    def __repr__(self):
        return '<BasicAccessHttpJsonClient "{name}"'.format(name=self._server.name)

    def _json_request(self, url):
        '''
        returns a JSON request appropriate for a web API (implemented as web-application URL `url`)
        '''
        if self._debug:
            print('[ BasicAccessHttpJsonClient._json_request ] JSON request {url}'.format(url=url))

        # we need to add the authority information
        request = sixUrlRequest.Request(url)

        if self._use_internal:
            # assume that 'external' (public) websites do not require any kind of authorization
            if self._debug:
                print('[ BasicAccessHttpJsonClient._json_request ] adding Basic Access authorization header to {url}'.format(url=url))

            try:
                # self._server is the SecureServerConfig that has the server authority information
                pass_phrase = self._server.web_baseurl_authority
                request.add_header("Authorization", "Basic " + base64.b64encode(pass_phrase.encode()).decode())
            except AttributeError:
                # the user did not provide a pass_phrase
                pass

        return BasicAccessHttpJsonRequest(request, self._server.encoding, self._debug)

    def _show_series(self, *, ds_filter=None, info=False):
        # we have to intercept calls to both show_series parent methods, show_series() and show_series_wrapper(), and then do the
        # right thing depending on configuration parameters
        arg_str_unencoded = {}

        if ds_filter is not None:
            arg_str_unencoded['filter'] = ds_filter

        if self._use_internal or self._server.web_show_series_wrapper is None:
            # do not use wrapper (use show_series)
            parsed = urlparse(self._server.url_show_series)
        else:
            # use wrapper (showextseries)
            if hasattr(self._server, 'web_show_series_wrapper_args') and self._server.web_show_series_wrapper_args is not None:
                arg_str_unencoded.update(self._server.web_show_series_wrapper_args)
                # use connection information override, if present
                if self._server.connection_info is not None:
                    if 'dbhost' in self._server.connection_info:
                        arg_str_unencoded['dbhost'] = self._server.connection_info['dbhost']
                    if 'dbport' in self._server.connection_info:
                        arg_str_unencoded['dbport'] = str(self._server.connection_info['dbport'])
                    if 'dbname' in self._server.connection_info:
                        arg_str_unencoded['dbname'] = self._server.connection_info['dbname']
                    if 'dbuser' in self._server.connection_info:
                        arg_str_unencoded['dbuser'] = self._server.connection_info['dbuser']
            if info:
                arg_str_unencoded['info'] = 1

            parsed = urlparse(self._server.url_show_series_wrapper)

        unparsed = urlunparse((parsed[0], parsed[1], parsed[2], None, urlencode(arg_str_unencoded), None))
        request = self._json_request(unparsed)
        return request.data

    def exp_package(self, spec, download_directory, filename_fmt=None):
        '''
        synchronously export FITS files contained by DRMS records specified by `spec`; unlike export(), this method does not require the presence of the formal export system and no export Storage Unit will be created on the server

        Parameters
        ----------
        spec : str
            a DRMS record-set specification such as "hmi.M_720s[2018.3.2_00:00_TAI]{magnetogram}"; the DRMS data segments to which `spec` refers must be of FITS protocol
        [ filename_fmt : str ]
            custom filename template string to use when generating the names for exported files that appear in the tar file; if None (the default), then '{seriesname}.{recnum:%%lld}.{segment}' is used


        Return Value
        ------------
        dict
            a dict containing a `url` attribute (the web-application URL which, when opened, initiates the streaming download of a tar file of synchronously exported FITS files); an example URL:
                http://jsoc.stanford.edu/cgi-bin/drms-export.sh?
                skiptar=true&
                spec=hmi.m_720s%5B2019.2.2%5D&
                filename=hmi.m_720s.%7BT_REC%3AA%7D.%7BCAMERA%7D.%7Bsegment%7D&
                compression=none&
                dbhost=hmidb2&
                webserver=jsoc.stanford.edu
        '''
        parsed = urlparse(self._server.url_export_package)

        arg_str_unencoded = { 'spec' : spec, 'compression' : 'rice', 'skiptar' : 'false' }

        if filename_fmt is not None and len(filename_fmt) > 0:
            arg_str_unencoded['filename'] = filename_fmt

        if self._use_internal:
            if hasattr(self._server, 'web_export_package_internal_args') and self._server.web_export_package_internal_args is not None:
                arg_str_unencoded.update(self._server.web_export_package_internal_args)
                if self._server.connection_info is not None:
                    if 'dbhost' in self._server.connection_info:
                        arg_str_unencoded['dbhost'] = self._server.connection_info['dbhost']
                    if 'dbport' in self._server.connection_info:
                        arg_str_unencoded['dbport'] = str(self._server.connection_info['dbport'])
                    if 'dbname' in self._server.connection_info:
                        arg_str_unencoded['dbname'] = self._server.connection_info['dbname']
                    if 'dbuser' in self._server.connection_info:
                        arg_str_unencoded['dbuser'] = self._server.connection_info['dbuser']
        else:
            if hasattr(self._server, 'web_export_package_args') and self._server.web_export_package_args is not None:
                arg_str_unencoded.update(self._server.web_export_package_args)
                if self._server.connection_info is not None:
                    if 'dbhost' in self._server.connection_info:
                        arg_str_unencoded['dbhost'] = self._server.connection_info['dbhost']
                    if 'dbport' in self._server.connection_info:
                        arg_str_unencoded['dbport'] = str(self._server.connection_info['dbport'])
                    if 'dbname' in self._server.connection_info:
                        arg_str_unencoded['dbname'] = self._server.connection_info['dbname']
                    if 'dbuser' in self._server.connection_info:
                        arg_str_unencoded['dbuser'] = self._server.connection_info['dbuser']

        unparsed = urlunparse((parsed[0], parsed[1], parsed[2], None, urlencode(arg_str_unencoded), None))

        if download_directory is None:
            download_path = None
        else:
            tar_file = str(uuid.uuid4()) + '.tar'
            download_path = os.path.join(download_directory, tar_file)

        # do not call self._json_request() - we want to defer the actual download until the user calls download(); must
        # return a dict
        return { 'url' : unparsed, 'package' : download_path, 'status' : 0 }

    def parse_recset(self, recset):
        '''
        parses a record-set specification into parts; no semantic checking is performed; only the syntax is verified;
        returns a dict that represents JSON:

        {
            "spec": "hmi.m_720s[2018.3.2]",
            "atfile": false,
            "hasfilts": true,
            "nsubsets": 1,
            "subsets": [
                {
                    "spec": "hmi.m_720s[2018.3.2]",
                    "settype": "drms",
                    "seriesname": "hmi.m_720s",
                    "seriesns": "hmi",
                    "seriestab": "m_720s",
                    "filter": "[2018.3.2]",
                    "segments": null,
                    "autobang": false
                }
            ],
            "errMsg": null
        }

        Parameters
        ----------
        recset : str
            a DRMS record-set specification such as "hmi.M_720s[2018.3.2_00:00_TAI]{magnetogram}"

        Return Value
        ------------
        dict
            a dict representing the JSON response to the show_series web-application

        '''
        parsed = urlparse(self._server.url_parse_recset)

        arg_str_unencoded = { 'spec' : recset }
        unparsed = urlunparse((parsed[0], parsed[1], parsed[2], None, urlencode(arg_str_unencoded), None))
        request = self._json_request(unparsed)
        return request.data

    def show_series(self, ds_filter=None):
        '''
        return a list of accessible data series

        Parameters
        ----------
        [ ds_filter : str ]
            name of filtering POSIX Extended Regular Expression; the list returned will contain only DRMS data series whose names match the regexp; if None (the default) then no filtering is performed

        Return Value
        ------------
        dict
            a dict representing the JSON response to the show_series web-application and containing the accessible series
        '''
        return self._show_series(ds_filter=ds_filter)

    def show_series_wrapper(self, ds_filter=None, info=False):
        '''
        return a list of accessible data series

        an alternative to show_series that, in addition to listing DRMS 'external' data series, lists 'internal' series (ones that reside in the internal, private database) that the server makes publicly accessible

        Parameters
        ----------
        [ ds_filter : str ]
            name of filtering POSIX Extended Regular Expression; the list returned will contain only DRMS data series whose names match the regexp; if None (the default) then no filtering is performed
        [ info : bool ]
            if True, the list returned includes a description of each data series (default is False)

        Return Value
        ------------
        dict
            a dict representing the JSON response to the show_series-wrapper web-application and containing the accessible series
        '''
        return self._show_series(ds_filter=ds_filter, info=info)

    # additional methods provided by the parent class are:


    # end public methods

    @property
    def use_internal(self):
        return self._use_internal

    @property
    def use_ssh(self):
        return self._use_ssh


class SSHJsonRequest(object):
    '''
    a class to send server requests (SSH commands) to the server, which then returns a JSON byte string; for each request, the class processes the JSON response, decoding it and intializing a dict with the decoded string; the resulting dict is returned to the calling JSON client

    Attributes
    ----------
    data : dict
        a dict representation of the JSON response to the server SSH request
    raw_data : str
        the JSON byte-string response to the server SSH request
    '''
    def __init__(self, cmds, json_client, encoding, remote_user, remote_host, remote_port, debug=False):
        '''
        Parameters
        ----------
        cmds : list
            a list of command-line arguments appropriate for the server web-application
        json_client:
            the SSHJsonClient client that created the instance
        encoding : str
            the name of one of the following JSON encodings: UTF-8, UTF-16, or UTF-32
        remote_user : str
            the unix account the ssh command runs as
        remote_host : str
            the server accepting ssh requests
        remote_port : int
            the port of the server to which ssh requests are sent
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._cmds = cmds
        self._json_client = json_client
        self._encoding = encoding
        self._remote_user = remote_user
        self._remote_host = remote_host
        self._remote_port = remote_port
        self._debug = debug
        self._data_str = None
        self._data = None

    def __repr__(self):
        return '<SSHJsonRequest "{name}"'.format(name=' '.join(self._cmds))

    def _run_cmd(self):
        '''
        executes the ssh command on the ssh server; returns a bytes object that represents an encoded JSON string
        '''
        try:
            ssh_cmd_list = [ '/usr/bin/ssh', '-p', str(self._remote_port), self._remote_user + '@' + self._remote_host, shlex.quote('/bin/bash -c ' + shlex.quote(' '.join(self._cmds))) ]

            if self._debug:
                print('[ SSHJsonRequest._run_cmd ] running ssh command: {cmd}'.format(cmd=' '.join(ssh_cmd_list)))

            child = pexpect.spawn(' '.join(ssh_cmd_list))
            password_attempted = False
            password_failed = False
            while True:
                # if we are running a synchronous export command, it could take a few minutes for it to complete - increase timeout from
                # the default value of 30 seconds
                index = child.expect([ 'password:', pexpect.EOF ], timeout=1024)
                if index == 0:
                    if password_attempted:
                        if self._debug:
                            print('[ SSHJsonRequest._run_cmd ] ssh password failed; requesting user re-try')
                        password_failed = True
                    # user was prompted to enter password
                    password = self._json_client.get_password(user_and_host=self._remote_user + '@' + self._remote_host, first_try=(not password_failed))
                    child.sendline(password.encode('UTF8'))
                    password_attempted = True
                else:
                    # no password was required (because the SSH keys and ssh-agent were properly set up)
                    resp = child.before
                    break
        except AttributeError:
            # a configuration parameter is missing (like ssh_remote_user or ssh_remote_host or ssh_remote_port
            import traceback
            raise SecureDRMSConfigurationError(traceback.format_exc(1))
        except pexpect.exceptions.TIMEOUT:
            raise SecureDRMSTimeOutError('time-out waiting server to respond')

        # resp contains the UTF8-encoded JSON response from the cmdList call
        return resp

    @property
    def data(self):
        if self._data is None:
            # assign dictionary to self._data
            json_str = self.raw_data.decode(self._encoding)

            if self._debug:
                print('[ SSHJsonRequest.data ] JSON response: {json}'.format(json=json_str))
            try:
                self._data = loads(json_str)
            except decoder.JSONDecodeError:
                raise SecureDRMSResponseError('invalid JSON response: ' + json_str)

        # returns a dict representation of JSON
        return self._data

    @property
    def raw_data(self):
        if self._data_str is None:
            self._data_str = self._run_cmd()
        return self._data_str


class SSHJsonClient(object):
    '''
    a class that contains one method per server ssh API call; for each API, the class creates an appropriate SSHJsonRequest; it then collects the dictionary response from the SSHJsonRequest instance, processes it (if needed), and provides the result back to the SSHClient

    Attributes
    ----------
    config : object (SecureServerConfig)
        a secure server configuration
    debug : bool
        if True, print debugging statements
    password : str
        a plain-text, memory-only, SSH-host password
    server : object (SecureServerConfig)
        a secure server configuration (a synonym for 'config')
    use_internal : bool
        if True, then access will be to privately accessible data series, otherwise access will be to publicly accessible series (external DRMS data series plus selectively exposed internal data series)
    use_ssh : bool
        if True, use the SSH-access methods supported by the secure server; otherwise use the HTTP-access methods; ALWAYS True
    '''
    def __init__(self, *, config, use_internal=False, debug=False):
        '''
        Parameters
        ----------
        config : object (SecureServerConfig)
            a secure server configuration
        [ use_internal : bool ]
            If True, then access will be to an internal server, otherwise access will be to an external server (external DRMS data series plus selectively exposed internal data series) (default is False)
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._server = config # SecureServerConfig
        self._use_ssh = True
        self._use_internal = use_internal
        self._debug = debug
        self._password = None
        self._password_timer = None

    def __repr__(self):
        return '<SSHJsonClient "{name}"'.format(name=self._server.name)

    def _clear_password(self):
        if self._debug:
            print('[ SSHJsonClient._clear_password ] clearing password')

        self._password_timer = None
        self._password = None

    def _clear_timer(self):
        '''
        if a timer is currently running, then this method cancels the timer
        '''
        if self._debug:
            print('[ SSHJsonClient._clear_timer ] clearing timer for json client {client}'.format(client=repr(self)))

        if self._password_timer is not None:
            self._password_timer.cancel()

        if self._debug:
            print('cleared\n')

    def _json_request(self, *, cmd_list):
        '''
        create the SSHJsonRequest object that contains the SSH interface call; the actual execution of the commands occurs in the _run_cmd() method when it 'gets' the 'data' property of the SSHJsonRequest object; `cmd_list` contains a list bash commands - commands to invoke the interface programs; these commands are appended to commands to initialize the server environment, and then a SSHJsonRequest object is constructed from the final list
        '''
        # prepend the cmd_list with the env var settings
        env_cmds = [ cmd + ';' for cmd in self._server.ssh_remote_env.bash_cmd() ]
        cmds = env_cmds + [ ' '.join(cmd_list) ]

        if self._debug:
            print('[ SSHJsonClient._json_request ] JSON request {cmd}'.format(cmd=' '.join(cmd_list)))

        return SSHJsonRequest(cmds, self, self._server.encoding, self._server.ssh_remote_user, self._server.ssh_remote_host, self._server.ssh_remote_port, self._debug)

    def _package_json_extractor(self, package_path):
        extraction_cmd = [ '/bin/tar', 'xOf', package_path, 'jsoc/file_list.json' ]
        try:
            resp = check_output(extraction_cmd)
            json_response = resp.decode('utf-8')
        except ValueError as exc:
            raise SecureDRMSChildProcessError('[ _package_json_extractor ] invalid command-line arguments: ' + ' '.join(extraction_cmd))
        except CalledProcessError as exc:
            raise SecureDRMSChildProcessError("[ _package_json_extractor ] command '" + ' '.join(extraction_cmd) + "' returned non-zero status code " + str(exc.returncode))

        return json_response

    def _run_cmd(self, *, cmd_list):
        '''
        create the SSHJsonRequest object and 'get' its 'data' property (which causes the interface SSH command to be execute)
        '''
        request = self._json_request(cmd_list=cmd_list)

        # runs the ssh command
        response = request.data
        return response

    def _show_series(self, *, ds_filter=None, info=False):
        # we have to intercept calls to both show_series parent methods, show_series() and show_series_wrapper(), and then do the
        # right thing depending on configuration parameters
        update_dict = {}

        if self._use_internal or self._server.ssh_show_series_wrapper is None:
            # binary executable
            cmd_list = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_show_series), '-qz' ]

            if self._server.encoding.lower() == 'utf8':
                cmd_list.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._use_internal:
                if self._server.ssh_show_series_internal_args is not None:
                    update_dict.update(self._server.ssh_show_series_internal_args)
            else:
                if self._server.ssh_show_series_args is not None:
                    update_dict.update(self.ssh_show_series_args)

            # override
            if self._server.connection_info is not None:
                if 'dbhost' in self._server.connection_info:
                    val = self._server.connection_info['dbhost']
                    if 'dbport' in self._server.connection_info:
                        val = f'{val}:{str(self._server.connection_info["dbport"])}'

                    update_dict['JSOC_DBHOST'] = val

                if 'dbuser' in self._server.connection_info:
                    update_dict['JSOC_DBUSER'] = self._server.connection_info['dbuser']

                if 'dbname' in self._server.connection_info:
                    update_dict['JSOC_DBNAME'] = self._server.connection_info['dbname']

            cmd_list.extend([ f'{key} = {str(val)}' for key, val in update_dict.items() ])

            if ds_filter is not None:
                cmd_list.append(ds_filter)
        else:
            # script (showextinfo.py)
            cmd_list = [ os.path.join(self._server.ssh_base_script, self._server.ssh_show_series_wrapper), '--json' ]

            if self._server.ssh_show_series_wrapper_args is not None:
                arg_str_unencoded.update(self._server.ssh_show_series_wrapper_args)

            # override
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                arg_str_unencoded['dbhost'] = val

            if ds_filter is not None:
                cmd_list.append('--filter=' + ds_filter)

            if info:
                cmd_list.append('--info' )

        return self._run_cmd(cmd_list=cmd_list)

    # public methods
    def clear_timer(self):
        '''
        if a timer is currently running, then this method cancels the timer

        Parameters
        ----------
        None

        Return Value
        ------------
        None
        '''
        return self._clear_timer()

    def get_password(self, *, user_and_host, first_try=True):
        '''
        Parameters
        ----------
        user_and_host : str
            the 'user@host' string presented to the user when prompted for a server SSH password
        [ first_try : bool]
            if True (the default), then the password has never been sent to the server for the current SSH command; otherwise, the password has been sent to the server, and authentication failed for that password

        Return Value
        ------------
        '''
        if not first_try:
            # this implies the existing password is bad
            self._password = None
            self._clear_timer()
            print('permission denied, please re-enter a password for {address}'.format(address=user_and_host))
            self._password = getpass.getpass()
        else:
            if self._password is not None:
                if self._debug:
                    print('[ SSHJsonClient._json_request ] renewing password' )

                self._clear_timer()
            else:
                print('please enter a password for {address}'.format(address=user_and_host))

                if self._debug:
                    print('[ SSHJsonClient._json_request ] storing password' )

                self._password = getpass.getpass()

        self._password_timer = threading.Timer(PASSWORD_TIMEOUT, self._clear_password)
        # IMPORTANT! making the timer threads daemons allows the interpreter to terminate in response to EOF (ctrl-d); otherwise,
        # the main thread blocks on a join() on the timer thread until the timer 'fires' (and calls self._clear_password()); normally
        # this isn't so cool, but there does not seem to be a way for this module to 'intercept' an EOF sent to the interpreter
        # interactively
        self._password_timer.daemon = True

        if self._debug:
            print('[ SSHJsonClient._json_request ] starting password timer' )

        self._password_timer.start()

        return self._password

    def check_address(self, address):
        '''
        verify that an email address is registered with the server's export system

        Parameters
        ----------
        address : str
            the email address to be verified

        Return Value
        ------------
        dict
            the dict contains 'status' and 'msg' properties:
                2: email address is registered
                4: email address has not been registered
               -2: invalid email address
        '''
        # external calls only (checkAddress.py)
        if address is None or not isinstance(address, str):
            raise SecureDRMSArgumentError('[ check_address ] missing or invalid argument email')

        # this check_email script accepts a URL argument string (like QUERY_STRING)
        arg_str_encoded = urlencode({ 'address' : address, 'addresstab' : self._server.ssh_check_email_addresstab, 'checkonly' : 1, 'domaintab' : self._server.ssh_check_email_domaintab })
        cmd_list = [ os.path.join(self._server.ssh_base_script, self._server.ssh_check_email), shlex.quote(arg_str_encoded) ]
        return self._run_cmd(cmd_list=cmd_list)

    def exp_fits(self, spec, download_directory, filename_fmt=None):
        '''
        synchronously export FITS files contained by DRMS records specified by `spec`; unlike export(), this method does not require the presence of the formal export system and no export Storage Unit will be created on the server; the FITS files are archived to a temporary tar file, which is placed on the server's temp directory

        Parameters
        ----------
        spec : str
            a DRMS record-set specification (e.g., 'hmi.M_720s[2018.3.2_00:00_TAI]{magnetogram}'); the DRMS data segments to which `spec` refers must be of FITS protocol
        [ filename_fmt : str ]
            custom filename template string to use when generating the names for exported files that appear in the tar file; if None (the default), then '{seriesname}.{recnum:%%lld}.{segment}' is used

        Return Value
        ------------
        dict
            a dict representing the JSON response to the export_fits web-application; the 'method' attribute will be set to 'url_direct',  'requestid' will be None since a format export request was never created, and 'dir' will be None since no export Storage Unit is created; to the server's response is added the 'tarfile' attribute, which is the path to the server's temporary tar file; example return value:
            {
                'status' : 0,
                'requestid' : None,
                'method' : 'url_direct',
                'protocol' : 'FITS',
                'dir' : None,
                'wait' : 0,
                'tarfile' : '/tmp/.46680c4a-c004-4b67-9d7c-0d65412d8c94.tar'
                'data' :
                [
                    {
                        'record' : 'hmi.M_720s[2017.12.03_00:00:00_TAI][3]{magnetogram}',
                        'filename' : 'hmi.M_720s.555172.magnetogram.fits'
                    },
                    {
                        'record' : 'hmi.M_720s[2017.12.03_00:12:00_TAI][3]{magnetogram}',
                        'filename' : 'hmi.M_720s.555176.magnetogram.fits'
                    },
                    {
                        'record' : 'hmi.M_720s[2017.12.03_00:24:00_TAI][3]{magnetogram}',
                        'filename' : 'hmi.M_720s.555180.magnetogram.fits'
                    },
                    {
                        'record' : 'hmi.M_720s[2017.12.03_00:36:00_TAI][3]{magnetogram}',
                        'filename' : 'hmi.M_720s.555184.magnetogram.fits'
                    },
                    {
                        'record' : 'hmi.M_720s[2017.12.03_00:48:00_TAI][3]{magnetogram}',
                        'filename' : 'hmi.M_720s.555188.magnetogram.fits'
                    },
                    {
                        'record' : 'hmi.M_720s[2017.12.03_01:00:00_TAI][3]{magnetogram}',
                        'filename' : 'hmi.M_720s.555192.magnetogram.fits'
                    },
                    {
                        'record' : 'hmi.M_720s[2017.12.03_01:12:00_TAI][3]{magnetogram}',
                        'filename':'hmi.M_720s.555196.magnetogram.fits'
                    },
                    {
                        'record' : 'hmi.M_720s[2017.12.03_01:24:00_TAI][3]{magnetogram}',
                        'filename' : 'hmi.M_720s.555200.magnetogram.fits'
                    }
                ]
            }
        '''
        cmd_list = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_export_fits), 'a=0', 's=0', 'e=1' ]

        cmd_list.append(shlex.quote('spec=' + spec))

        if filename_fmt is not None and len(filename_fmt) > 0:
            cmd_list.append(shlex.quote('ffmt=' + filename_fmt))

        if self._server.encoding.lower() == 'utf8':
            cmd_list.append('DRMS_DBUTF8CLIENTENCODING=1')

        update_dict = {}

        if self._use_internal:
            if self._server.ssh_export_fits_internal_args is not None:
                update_dict.update(self._server.ssh_export_fits_internal_args)
        else:
            if self._server.ssh_export_fits_args is not None:
                update_dict.update(self._server.ssh_export_fits_args)

        # override
        if self._server.connection_info is not None:
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                update_dict['JSOC_DBHOST'] = val

            if 'dbuser' in self._server.connection_info:
                update_dict['JSOC_DBUSER'] = self._server.connection_info['dbuser']

            if 'dbname' in self._server.connection_info:
                update_dict['JSOC_DBNAME'] = self._server.connection_info['dbname']

        cmd_list.extend([ f'{key} = {str(val)}' for key, val in update_dict.items() ])

        # we have to redirect the output to a tar file; use a UUID-inspired base file name; when the user calls
        # SecureExportRequest.download(dir), this file is scp'd back to the dir directory on the client host and exploded
        tar_file = os.path.join(self._server.server_tmp, '.' + str(uuid.uuid4()) + '.tar')
        cmd_list.append('>' + tar_file)

        # now, the JSON response is actually a file named jsoc/file_list.json inside the tar file; must extract that and
        # print to sdtout (which is that the 'O' flag to tar does)
        cmd_list.append(';')
        cmd_list.extend([ '/bin/tar', 'xOf', tar_file, 'jsoc/file_list.json' ])

        # the JSON in this status file looks like this:
        # {
        #     "status" : 0,
        #     "requestid" : null,
        #     "method" : "url_direct",
        #     "protocol" : "FITS",
        #     "dir" : null,
        #     "wait" : 0,
        #     "tarfile" : "/tmp/.46680c4a-c004-4b67-9d7c-0d65412d8c94.tar"
        #     "data" :
        #     [
        #         {
        #             "record" : "hmi.M_720s[2017.12.03_00:00:00_TAI][3]{magnetogram}",
        #             "filename" : "hmi.M_720s.555172.magnetogram.fits"
        #         },
        #         {
        #             "record" : "hmi.M_720s[2017.12.03_00:12:00_TAI][3]{magnetogram}",
        #             "filename" : "hmi.M_720s.555176.magnetogram.fits"
        #         },
        #         {
        #             "record" : "hmi.M_720s[2017.12.03_00:24:00_TAI][3]{magnetogram}",
        #             "filename" : "hmi.M_720s.555180.magnetogram.fits"
        #         },
        #         {
        #             "record" : "hmi.M_720s[2017.12.03_00:36:00_TAI][3]{magnetogram}",
        #             "filename" : "hmi.M_720s.555184.magnetogram.fits"
        #         },
        #         {
        #             "record" : "hmi.M_720s[2017.12.03_00:48:00_TAI][3]{magnetogram}",
        #             "filename" : "hmi.M_720s.555188.magnetogram.fits"
        #         },
        #         {
        #             "record" : "hmi.M_720s[2017.12.03_01:00:00_TAI][3]{magnetogram}",
        #             "filename" : "hmi.M_720s.555192.magnetogram.fits"
        #         },
        #         {
        #             "record" : "hmi.M_720s[2017.12.03_01:12:00_TAI][3]{magnetogram}",
        #             "filename":"hmi.M_720s.555196.magnetogram.fits"
        #         },
        #         {
        #             "record" : "hmi.M_720s[2017.12.03_01:24:00_TAI][3]{magnetogram}",
        #             "filename" : "hmi.M_720s.555200.magnetogram.fits"
        #         }
        #     ]
        # }

        response = self._run_cmd(cmd_list=cmd_list)

        # add the server tar-file name; response is a dict (made from the JSON-string server response)
        response['tarfile'] = tar_file
        response['package'] = os.path.join(download_directory, os.path.basename(tar_file)) # local package

        return response

    def exp_package(self, *, spec, download_directory=None, filename_fmt=None):
        '''
        synchronously export FITS files contained by DRMS records specified by `spec`; unlike export(), this method does not require the presence of the formal export system and no export Storage Unit will be created on the server; the FITS files are archived to a temporary tar file, which is placed on the server's temp directory

        Parameters
        ----------
        spec : str
            a DRMS record-set specification (e.g., 'hmi.M_720s[2018.3.2_00:00_TAI]{magnetogram}'); the DRMS data segments to which `spec` refers must be of FITS protocol
        download_directory : str, None
            the local directory to which the package will be downloaded; if the value is None, then the package will be streamed into memory and no disk file will be created
        [ filename_fmt : str ]
            custom filename template string to use when generating the names for exported files that appear in the tar file; if None (the default), then '{seriesname}.{recnum:%%lld}.{segment}' is used

        Return Value
        ------------
        dict
            a dict representing the JSON response to the export_fits web-application; the 'method' attribute will be set to 'url_direct',  'requestid' will be None since a format export request was never created, and 'dir' will be None since no export Storage Unit is created; to the server's response is added the 'tarfile' attribute, which is the path to the server's temporary tar file; example return value:
            {
                "status" : 0,
                "on-the-fly-command" : [ '/home/jsoc/cvs/Development/JSOC/bin/linux_avx/drms-export-to-stout', 'a=0', 's=0', 'e=1', spec='hmi.rdMAI_fd30[2231][210][255.0]', ffmt='hmi.rdMAI_fd30.{CarrRot}.{CMLon}.{LonHG}.{LatHG}.{LonCM}.{segment}', 'DRMS_DBUTF8CLIENTENCODING=1' ],
                "package" : "/tmp/.46680c4a-c004-4b67-9d7c-0d65412d8c94.tar",
                "json_response" : "jsoc/file_list.json"
            }
        '''
        # a=0 ==> compress all segments, s=0 ==> make tar file, e=1 ==> suppress stderr
        cmd_list = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_export_fits), 'a=0', 's=0', 'e=1' ]

        cmd_list.append(shlex.quote('spec=' + spec))

        if filename_fmt is not None and len(filename_fmt) > 0:
            cmd_list.append(shlex.quote('ffmt=' + filename_fmt))

        if self._server.encoding.lower() == 'utf8':
            cmd_list.append('DRMS_DBUTF8CLIENTENCODING=1')

        update_dict = {}

        if self._use_internal:
            if self._server.ssh_export_fits_internal_args is not None:
                update_dict.update(self._server.ssh_export_fits_internal_args)
        else:
            if self._server.ssh_export_fits_args is not None:
                update_dict.update(self._server.ssh_export_fits_args)

        # override
        if self._server.connection_info is not None:
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                update_dict['JSOC_DBHOST'] = val

            if 'dbuser' in self._server.connection_info:
                update_dict['JSOC_DBUSER'] = self._server.connection_info['dbuser']

            if 'dbname' in self._server.connection_info:
                update_dict['JSOC_DBNAME'] = self._server.connection_info['dbname']

        cmd_list.extend([ f'{key} = {str(val)}' for key, val in update_dict.items() ])

        if download_directory is None:
            download_path = None
        else:
            tar_file = str(uuid.uuid4()) + '.tar'
            download_path = os.path.join(download_directory, tar_file)

        # do not call self._run_cmd() - we are not executing ssh just yet; do that when the user calls
        # SSHClient.download()
        response = {
            "status" : 0,
            "on-the-fly-command" : cmd_list,
            "package" : download_path,
            "json_response" : "jsoc/file_list.json"
        }

        # the response json looks like:
        # {
        #      "status" : 0,
        #      "on-the-fly-command" : [ "/home/jsoc/cvs/Development/JSOC/bin/linux_avx/drms-export-to-stdout", "a=0", "s=0", "e=1", spec="hmi.M_720s[2017.12.03_01:12:00_TAI][3]{magnetogram}", "ffmt=hmi.M_720s.{T_REC:A}.{CAMERA}.{segment}", DRMS_DBUTF8CLIENTENCODING=1", JSOC_DBHOST" : "hmidb", "JSOC_DBUSER" : "production", "maxfilesize" : 4294967296 ],
        #      "package" : "/tmp/.46680c4a-c004-4b67-9d7c-0d65412d8c94.tar",
        #      "json_response" : "jsoc/file_list.json"
        # }

        return response

    def exp_request(self, ds, notify, method='url_quick', protocol='as-is', protocol_args=None, filenamefmt=None, n=None, requestor=None):
        '''
        submit a data-export request to the DRMS server

        Parameters
        ----------
        ds : str
            the record-set specification for the DRMS records to export (e.g., 'hmi.B_720s[2014.7.8/60m]{field}')
        notify : str
            an export-system registered email address
        [ method : str ]
            the export method, which is one of : 'url', 'url_direct', 'url_quick' (the default), 'url-tar', 'ftp', 'ftp-tar'
        [ protocol : str ]
            the export protocol, which is one of 'as-is' (default), 'fits', 'jpg', 'mpg' and 'mp4'
        [ protocol_args : dict ]
            protocol arguments for protocols 'jpg', 'mpg', and 'mp4', which include 'ct', 'scaling', 'min', 'max', and 'size'.
        [ filenamefmt : str ]
            custom filename template string to use when generating the names for exported files that appear in the tar file; if None (the default), then '{seriesname}.{recnum:%%lld}.{segment}' is used; for 'url_quick'/'as-is' data exports, this is ignored
        [ n : int ]
            a maximum of abs(`n`) records are exported; if `n` > 0 the first abs(`n`) records of the record set are returned, if `n` < 0 the last abs(`n`) records of the record set are returned, and `n` is None (the default), then no limit is applied
        [ requestor : str or False ]
            the export user name; if None (the default), then a name is automatically generated from the user's registered email address; if False, then no requestor argument will be submitted during the export request

        Return Value
        ------------
        dict
            a dict representing the JSON response returned by the export_request web-application
        '''
        # both external (jsocextfetch.py) and internal (jsoc_fetch) calls
        method = method.lower()
        method_list = [ 'url', 'url_direct', 'url_quick', 'url-tar', 'ftp', 'ftp-tar' ]
        if method not in method_list:
            raise ValueError("Method '%s' is not supported, valid methods are: %s" % (method, ', '.join("'%s'" % s for s in method_list)))

        protocol = protocol.lower()
        img_protocol_list = [ 'jpg', 'mpg', 'mp4' ]
        protocol_list = [ 'as-is', 'fits' ] + img_protocol_list
        if protocol not in protocol_list:
            raise ValueError("Protocol '%s' is not supported, valid protocols are: %s" % (protocol, ', '.join("'%s'" % s for s in protocol_list)))

        # method "url_quick" is meant to be used with "as-is", change method
        # to "url" if protocol is not "as-is"
        if method == 'url_quick' and protocol != 'as-is':
            method = 'url'

        if protocol in img_protocol_list:
            d = {'ct': 'grey.sao', 'scaling': 'MINMAX', 'size': 1}
            if protocol_args is not None:
                for k, v in protocol_args.items():
                    if k.lower() == 'ct':
                        d['ct'] = v
                    elif k == 'scaling':
                        d[k] = v
                    elif k == 'size':
                        d[k] = int(v)
                    elif k in ['min', 'max']:
                        d[k] = float(v)
                    else:
                        raise ValueError("Unknown protocol argument: '%s'" % k)

            protocol += ',CT={ct},scaling={scaling},size={size}'.format(**d)
            if 'min' in d:
                protocol += ',min=%g' % d['min']
            if 'max' in d:
                protocol += ',max=%g' % d['max']
        else:
            if protocol_args is not None:
                raise ValueError("protocol_args not supported for protocol '%s'" % protocol)

        update_dict = {}

        if self._use_internal or self._server.ssh_jsoc_fetch_wrapper is None:
            cmd_list = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_fetch), '-W', 'op=exp_request', 'format=json', shlex.quote('ds=' + ds), 'notify=' + notify, 'method=' + method, 'protocol=' + protocol ]

            if filenamefmt is not None:
                cmd_list.append(shlex.quote('filenamefmt=' + filenamefmt))
            if n is not None:
                cmd_list.append('process=n=' + str(n))
            if requestor is None:
                cmd_list.append('requestor=' + notify.split('@')[0])
            elif requestor is not False:
                cmd_list.append('requestor=' + requestor)

            if self._server.encoding.lower() == 'utf8':
                cmd_list.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._use_internal:
                if self._server.ssh_jsoc_fetch_internal_args is not None:
                    update_dict.update(self._server.ssh_jsoc_fetch_internal_args)
            else:
                if self._server.ssh_jsoc_fetch_args is not None:
                    update_dict.update(self._server.ssh_jsoc_fetch_args)

            # override
            if self._server.connection_info is not None:
                if 'dbhost' in self._server.connection_info:
                    val = self._server.connection_info['dbhost']
                    if 'dbport' in self._server.connection_info:
                        val = f'{val}:{str(self._server.connection_info["dbport"])}'

                    update_dict['JSOC_DBHOST'] = val

                if 'dbuser' in self._server.connection_info:
                    update_dict['JSOC_DBUSER'] = self._server.connection_info['dbuser']

                if 'dbname' in self._server.connection_info:
                    update_dict['JSOC_DBNAME'] = self._server.connection_info['dbname']

            cmd_list.extend([ f'{key} = {str(val)}' for key, val in update_dict.items() ])
        else:
            # this script (jsocextfetch.py) accepts a URL argument string (like QUERY_STRING)
            arg_str_unencoded = { 'op' : 'exp_request', 'format' : 'json', 'ds' : ds, 'notify' : notify, 'method' : method, 'protocol' : protocol, 'n' : 1 }

            if self._server.ssh_jsoc_fetch_wrapper_args is not None:
                arg_str_unencoded.update(self._server.ssh_jsoc_fetch_wrapper_args)

            # override
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                arg_str_unencoded['dbhost'] = val

            if filenamefmt is not None:
                arg_str_unencoded.update({ 'filenamefmt' : filenamefmt })
            if n is not None:
                arg_str_unencoded.update({ 'process=n=' : str(n) })
            if requestor is None:
                arg_str_unencoded.update({ 'requestor=' : notify.split('@')[0] })
            elif requestor is not False:
                arg_str_unencoded.update({ 'requestor=' : requestor })

            arg_str_encoded = urlencode(arg_str_unencoded)
            cmd_list = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_fetch), shlex.quote(arg_str_encoded) ]

        return self._run_cmd(cmd_list=cmd_list)

    def exp_status(self, requestid):
        '''
        returns information, including current status, about a pending export request
        Parameters
        ----------
        requestid : str
            a export request identification string, which was returned by the original exp_request() request

        Return Value
        ------------
        dict
            a dict representing the JSON response returned by the exp_status web-application that includes request information, including the status
        '''
        # both external (jsocextfetch.py) and internal (jsoc_fetch) calls
        if requestid is None or not isinstance(requestid, str):
            raise SecureDRMSArgumentError('[ exp_status ] missing or invalid argument requestid')

        update_dict = {}

        if self._use_internal or self._server.ssh_jsoc_fetch_wrapper is None:
            cmd_list = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_fetch), '-W', 'op=exp_status', 'requestid=' + requestid ]

            if self._server.encoding.lower() == 'utf8':
                cmd_list.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._use_internal:
                if self._server.ssh_jsoc_fetch_internal_args is not None:
                    update_dict.update(self._server.ssh_jsoc_fetch_internal_args)
            else:
                if self._server.ssh_jsoc_fetch_args is not None:
                    update_dict.update(self._server.ssh_jsoc_fetch_args)

            # override
            if self._server.connection_info is not None:
                if 'dbhost' in self._server.connection_info:
                    val = self._server.connection_info['dbhost']
                    if 'dbport' in self._server.connection_info:
                        val = f'{val}:{str(self._server.connection_info["dbport"])}'

                    update_dict['JSOC_DBHOST'] = val

                if 'dbuser' in self._server.connection_info:
                    update_dict['JSOC_DBUSER'] = self._server.connection_info['dbuser']

                if 'dbname' in self._server.connection_info:
                    update_dict['JSOC_DBNAME'] = self._server.connection_info['dbname']

            cmd_list.extend([ f'{key} = {str(val)}' for key, val in update_dict.items() ])
        else:
            # this script accepts a URL argument string (like QUERY_STRING)
            arg_str_unencoded = { 'op' : 'exp_status', 'requestid' : requestid, 'n' : 1 }

            if self._server.ssh_jsoc_fetch_wrapper_args is not None:
                arg_str_unencoded.update(self._server.ssh_jsoc_fetch_wrapper_args)

            # override
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                arg_str_unencoded['dbhost'] = val

            arg_str_encoded = urlencode(arg_str_unencoded)
            cmd_list = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_fetch), shlex.quote(arg_str_encoded) ]

        return self._run_cmd(cmd_list=cmd_list)

    def parse_recset(self, recset):
        '''
        parses a record-set specification into parts; no semantic checking is performed, only the syntax is verified

        Parameters
        ----------
        recset : str
            a DRMS record-set specification (e.g., 'hmi.M_720s[2018.3.2]{magnetogram}')

        Return Value
        ------------
        dict
            returns a dict that represents JSON; an example:
            {
                'spec' : 'hmi.m_720s[2018.3.2]',
                'atfile' : False,
                'hasfilts' : True,
                'nsubsets' : 1,
                'subsets' :
                [
                    {
                        'spec' : 'hmi.m_720s[2018.3.2]',
                        'settype' : 'drms',
                        'seriesname' : 'hmi.m_720s',
                        'seriesns' : 'hmi',
                        'seriestab' : 'm_720s',
                        'filter' : '[2018.3.2]',
                        'segments' : None,
                        'autobang' : False
                    }
                ],
                'errMsg': None
            }
        '''
        # external call only (drms_parserecset)
        if recset is None or not isinstance(recset, str):
            raise SecureDRMSArgumentError('[ parse_recset ] missing or invalid argument recset')

        cmd_list = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_parse_recset), shlex.quote('spec=' + recset) ]
        if self._server.encoding.lower() == 'utf8':
            cmd_list.append('DRMS_DBUTF8CLIENTENCODING=1')

        return self._run_cmd(cmd_list=cmd_list)

    def rs_list(self, ds, key=None, seg=None, link=None, recinfo=False, n=None, uid=None):
        '''
        return DRMS record-set information

        Parameters
        ----------
        ds : str
            DRMS record-set specification (e.g., 'hmi.B_720s[2014.7.8/60m]{field}')
        [ key : str or list ]
            a single DRMS keyword name, or a list of keyword names; if None (default), then return no keyword information
        [ seg : str or list ]
            a single DRMS segment name, or a list of segment names; if None (default), then return no segment information
        [ link : str or list ]
            a single DRMS link name, or a list of link names; if None (default), then return no link information
        [ recinfo : bool ]
            if True, then return the record-set specification for each record; (default is False)
        [ n : int ]
            a maximum of abs(`n`) records are exported; if `n` > 0 the first abs(`n`) records of the record set are returned, if `n` < 0 the last abs(`n`) records of the record set are returned, and `n` is None (the default), then no limit is applied
        [ uid : str ]
            session ID to pass to server; allows the caller to terminate the underlying application running on the server

        Return Value
        ------------
        dict
            returns a dict that represents JSON; an example:
            {
                'keywords':
                [
                    {'name': 'TINTNUM', 'values': ['672', '672', '672', '672', '672']},
                    {'name': 'T_OBS', 'values': ['2017.01.07_23:59:52_TAI', '2017.01.08_00:11:52_TAI', '2017.01.08_00:23:52_TAI', '2017.01.08_00:35:52_TAI', '2017.01.08_00:47:52_TAI']}
                ],
                'segments':
                [
                    {
                        'name': 'continuum',
                        'values': ['/SUM89/D990049682/S00000/continuum.fits', '/SUM89/D990049682/S00001/continuum.fits', '/SUM89/D990049682/S00002/continuum.fits', '/SUM89/D990049682/S00003/continuum.fits', '/SUM89/D990049682/S00004/continuum.fits'],
                        'dims': ['4096x4096', '4096x4096', '4096x4096', '4096x4096', '4096x4096'],
                        'cparms': ['compress Rice', 'compress Rice', 'compress Rice', 'compress Rice', 'compress Rice'],
                        'bzeros': ['97304', '97304', '97304', '97304', '97304'],
                        'bscales': ['3', '3', '3', '3', '3']}
                ],
                'links': [],
                'count': 5,
                'runtime': 0.037,
                'status': 0
            }
        '''
        # both external (jsocextinfo.py) and internal (jsoc_info) calls

        update_dict = {}

        if self._use_internal or self._server.ssh_jsoc_info_wrapper is None:
            cmd_list = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_info), '-s', 'op=rs_list', shlex.quote('ds=' + ds) ]

            if key is not None:
                cmd_list.append('key=' + ','.join(_split_arg(key)))
            if seg is not None:
                cmd_list.append('seg=' + ','.join(_split_arg(seg)))
            if link is not None:
                cmd_list.append('link=' + ','.join(_split_arg(link)))
            if recinfo:
                cmd_list.append('-R')
            if n is not None:
                cmd_list.append('n=' + str(n))
            if uid is not None:
                cmd_list.append('userhandle=' + uid)

            if self._server.encoding.lower() == 'utf8':
                cmd_list.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._use_internal:
                if self._server.ssh_jsoc_info_internal_args is not None:
                    update_dict.update(self._server.ssh_jsoc_info_internal_args)
            else:
                if self._server.ssh_jsoc_info_args is not None:
                    update_dict.update(self._server.ssh_jsoc_info_args)

            # override
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                update_dict['JSOC_DBHOST'] = val

            if 'dbuser' in self._server.connection_info:
                update_dict['JSOC_DBUSER'] = self._server.connection_info['dbuser']

            if 'dbname' in self._server.connection_info:
                update_dict['JSOC_DBNAME'] = self._server.connection_info['dbname']

            cmd_list.extend([ f'{key} = {str(val)}' for key, val in update_dict.items() ])
        else:
            # this script (jsocextinfo.py) accepts a URL argument string (like QUERY_STRING)
            arg_str_unencoded = { 'op' : 'rs_list', 'ds' : ds, 'N' : 1 }

            if key is not None:
                arg_str_unencoded.update({ 'key' : ','.join(_split_arg(key)) })
            if seg is not None:
                arg_str_unencoded.update({ 'seg' : ','.join(_split_arg(seg)) })
            if link is not None:
                arg_str_unencoded.update({ 'link' : ','.join(_split_arg(link)) })
            if recinfo:
                arg_str_unencoded.update({ 'R' : 1 })
            if n is not None:
                arg_str_unencoded.update({ 'n=' : str(n) })
            if uid is not None:
                arg_str_unencoded.update({ 'userhandle' : uid })

            if self._server.ssh_jsoc_info_wrapper_args is not None:
                update_dict.update(self._server.ssh_jsoc_info_wrapper_args)

            # override
            if self._server.connection_info is not None:
                if 'dbhost' in self._server.connection_info:
                    val = self._server.connection_info['dbhost']
                    if 'dbport' in self._server.connection_info:
                        val = f'{val}:{str(self._server.connection_info["dbport"])}'

                    update_dict['dbhost'] = val

            arg_str_unencoded.update(update_dict)
            arg_str_encoded = urlencode(arg_str_unencoded)
            cmd_list = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_info_wrapper), shlex.quote(arg_str_encoded) ]

        return self._run_cmd(cmd_list=cmd_list)

    def rs_summary(self, ds):
        '''
        return the count of the number of DRMS records in a record-set specification

        Parameters
        ----------
        ds : str
            DRMS record-set specification (e.g., 'hmi.B_720s[2014.7.8/60m]{field}')

        Return Value
        ------------
        dict
            returns a dict that represents JSON; an example:
            {
                'count' : 1,
                'runtime' : 0.056,
                'status' : 0
            }
        '''
        # both external (jsocextinfo.py) and internal (jsoc_info) calls
        update_dict = {}

        if self._use_internal or self._server.ssh_jsoc_info_wrapper is None:
            cmd_list = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_info), '-s', 'op=rs_summary', shlex.quote('ds=' + ds) ]

            if self._server.encoding.lower() == 'utf8':
                cmd_list.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._use_internal:
                if self._server.ssh_jsoc_info_internal_args is not None:
                    update_dict.update(self._server.ssh_jsoc_info_internal_args)
            else:
                if self._server.ssh_jsoc_info_args is not None:
                    update_dict.update(self._server.ssh_jsoc_info_args)

            # override
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                update_dict['JSOC_DBHOST'] = val

            if 'dbuser' in self._server.connection_info:
                update_dict['JSOC_DBUSER'] = self._server.connection_info['dbuser']

            if 'dbname' in self._server.connection_info:
                update_dict['JSOC_DBNAME'] = self._server.connection_info['dbname']

            cmd_list.extend([ f'{key} = {str(val)}' for key, val in update_dict.items() ])

        else:
            # jsocextfetch.py
            arg_str_unencoded = { 'op' : 'rs_summary', 'ds' : ds, 'N' : 1 }

            if self._server.ssh_jsoc_info_wrapper_args is not None:
                arg_str_unencoded.update(self._server.ssh_jsoc_info_wrapper_args)

            # override
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                arg_str_unencoded['dbhost'] = val

            arg_str_encoded = urlencode(arg_str_unencoded)
            cmd_list = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_info), shlex.quote(arg_str_encoded) ]

        return self._run_cmd(cmd_list=cmd_list)

    def series_struct(self, series):
        '''
        return a description of `series`

        Parameters
        ----------
        series : str
            DRMS data series (e.g., 'hmi.M_720s')

        Return Value
        ------------
        dict
            returns a dict that represents JSON; an example:
            {
                'note' : 'magnetograms with a cadence of 720 seconds.',
                'retention' : 10000,
                'stagingretention' : 3,
                'unitsize' : 32,
                'archive' : 1,
                'tapegroup' : 1001,
                'primekeys' : [ 'T_REC', 'CAMERA' ],
                'primekeysinfo' :
                [
                    {
                        'name' : 'T_REC',
                        'slotted' : 1,
                        'step' : '720.0'
                    },
                    {
                        'name' : 'CAMERA',
                        'slotted' : 0
                    }
                ],
                'dbindex' : [ 'T_REC', 'CAMERA' ],
                'keywords' :
                [
                    {
                        'name' : 'cparms_sg000',
                        'type' : 'string',
                        'recscope' : 'variable',
                        'defval' : 'compress Rice',
                        'units' : 'none',
                        'note' : ''
                    },
                    {
                        'name' : 'magnetogram_bzero',
                        'type' : 'double',
                        'recscope' : 'variable',
                        'defval' : '0',
                        'units' : 'none',
                        'note' : ''
                    },
                    ...
                ],
                segments' :
                [
                    {
                        'name' : 'magnetogram',
                        'type' : 'int',
                        'units' : 'Gauss',
                        'protocol' : 'fits',
                        'dims' : '4096x4096',
                        'note' : 'magnetogram'
                    }
                ],
                'links' : [ ],
                'Interval' :
                {
                    'FirstRecord' : 'hmi.M_720s[2009.04.13_21:48:00_TAI][1]',
                    'FirstRecnum' : 361508,
                    'LastRecord' : 'hmi.M_720s[2019.10.25_23:12:00_TAI][3]',
                    'LastRecnum' : 642236,
                    'MaxRecnum' : 642240
                },
                'runtime': 0.037,
                'status': 0
            }
        '''
        # both external (jsocextinfo.py) and internal (jsoc_info) calls
        if series is None or not isinstance(series, str):
            raise SecureDRMSArgumentError('[ series_struct ] missing or invalid argument series')

        update_dict = {}

        if self._use_internal or self._server.ssh_jsoc_info_wrapper is None:
            cmd_list = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_info), '-s', 'op=series_struct', 'ds=' +  series ]

            if self._server.encoding.lower() == 'utf8':
                cmd_list.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._use_internal:
                if self._server.ssh_jsoc_info_internal_args is not None:
                    update_dict.update(self._server.ssh_jsoc_info_internal_args)
            else:
                if self._server.ssh_jsoc_info_args is not None:
                    update_dict.update(self._server.ssh_jsoc_info_args)

            # override
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                update_dict['JSOC_DBHOST'] = val

            if 'dbuser' in self._server.connection_info:
                update_dict['JSOC_DBUSER'] = self._server.connection_info['dbuser']

            if 'dbname' in self._server.connection_info:
                update_dict['JSOC_DBNAME'] = self._server.connection_info['dbname']

            cmd_list.extend([ f'{key} = {str(val)}' for key, val in update_dict.items() ])
        else:
            # jsocextinfo.py
            arg_str_unencoded = { 'op' : 'series_struct', 'ds' : series, 'N' : 1 }

            if self._server.ssh_jsoc_info_wrapper_args is not None:
                arg_str_unencoded.update(self._server.ssh_jsoc_info_wrapper_args)

            # override
            if 'dbhost' in self._server.connection_info:
                val = self._server.connection_info['dbhost']
                if 'dbport' in self._server.connection_info:
                    val = f'{val}:{str(self._server.connection_info["dbport"])}'

                arg_str_unencoded['dbhost'] = val

            arg_str_encoded = urlencode(arg_str_unencoded)
            cmd_list = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_info), shlex.quote(arg_str_encoded) ]

        return self._run_cmd(cmd_list=cmd_list)

    def show_series(self, ds_filter=None):
        '''
        return a list of accessible data series

        Parameters
        ----------
        [ ds_filter : str ]
            name of filtering POSIX Extended Regular Expression; the list returned will contain only DRMS data series whose names match the regexp; if None (the default) then no filtering is performed

        Return Value
        ------------
        dict
            a dict representing the JSON response to the show_series web-application and containing the accessible series
        '''
        return self._show_series(ds_filter=ds_filter)

    def show_series_wrapper(self, ds_filter=None, info=False):
        '''
        return a list of accessible data series

        an alternative to show_series that, in addition to listing DRMS 'external' data series, lists 'internal' series (ones that reside in the internal, private database) that the server makes publicly accessible

        Parameters
        ----------
        [ ds_filter : str ]
            a filtering POSIX Extended Regular Expression; the list returned will contain only DRMS data series whose names match the regexp; if None (the default) then no filtering is performed
        [ info : bool ]
            if True, the list returned includes a description of each data series (default is False)

        Return Value
        ------------
        dict
            a dict representing the JSON response to the show_series-wrapper web-application and containing the accessible series
        '''
        return self._show_series(ds_filter=ds_filter, info=info)

    @property
    def config(self):
        return self._server

    @property
    def debug(self):
        return self._debug

    @debug.setter
    def debug(self, value):
        self._debug = value

    @property
    def password(self):
        return self._password

    @property
    def server(self):
        return self._server

    @server.setter
    def server(self, value):
        self._server = value

    @property
    def use_internal(self):
        return self._use_internal

    @property
    def use_ssh(self):
        return self._use_ssh


class SecureClient(DRMSClient):
    '''
    a class that provides secure access to DRMS API methods

    Attributes
    ----------
    config : object (SecureServerConfig)
        a secure server configuration
    debug : bool
        if True, print debugging statements
    json_client : BasicAccessHttpJsonClient or SSHJsonClient
        the associated JSON client used when communicating with the DRMS server
    use_internal : bool
        if True, then access will be to privately accessible data series, otherwise access will be to publicly accessible series (external DRMS data series plus selectively exposed internal data series)
    '''
    __lifo = LifoQueue()

    def __init__(self, config, use_internal=False, email=None, synchronous_export=True, verbose=False, debug=False):
        '''
        Parameters
        -----------
        config : object (SecureServerConfig)
            a secure server configuration
        [ use_internal : bool ]
            If True, then access will be to an internal server, otherwise access will be to an external server (external DRMS data series plus selectively exposed internal data series) (default is False)
        [ email : str ]
            default registered email address used when making an export request; used for the export() method if no email is provided to that method; (default is None)
        [ synchronous_export : bool ]
            if True (the default), then the SecureClient.export method will perform a synchronous export, provided the export method is 'url' or 'url_direct', the export protocol is FITS, and the segments being exported are of FITS protocol
        [ verbose : bool ]
            if True, print export status; if False (default) do not print export status
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._config = config
        self._use_internal = use_internal
        self._email = email     # use property for email validation
        self._synchronous_export = synchronous_export
        self._verbose = verbose # use property for conversion to bool
        self._debug = debug     # the parent sets self._json.debug, not self._debug, but we want to set _debug in this class as well
        self._info_cache = {}   # for the info() method

        # do not call parent's __init__() since that method creates an HttpJsonClient instance, but we need to create a BasicAccessHttpJsonClient/SSHJsonClient instead

    def __repr__(self):
        return '<SecureClient "{name}">'.format(name=self._config.name)

    def _execute(self, apimethod, **args):
        resp = None

        try:
            if 'api_name' in args:
                api_name = args['api_name']
                del args['api_name'] # API method might not handle this extraneous argument
            else:
                api_name = cast(types.FrameType, inspect.currentframe()).f_back.f_code.co_name

            if not self._server.check_supported(api_name, isinstance(self, SSHClient), self._use_internal):
                raise DrmsOperationNotSupported('Server does not support {api_name} access'.format(api_name=api_name))

            # set urls if this is an HTTP client
            if isinstance(self, BasicAccessClient):
                self._config.set_urls(self._use_internal, self._debug)

            # pass use_ssh and use_internal to SecureServerConfig.check_supported(), which will might be called by parent
            # method call

            # save old values
            self.__lifo.put((self.config.use_internal, self.config.use_ssh, self.config.debug))

            # set temporary values (for passing to config)
            self.config.use_internal = self._use_internal
            self.config.use_ssh = isinstance(self, SSHClient)
            self.config.debug = self._debug

            if self._debug:
                print('[ SecureClient._execute ] Executing DRMSClient API method "{method}"'.format(method=api_name))

            # this could call _execute() recursively, so a lower-level call could delete attributes, and then if we
            # try again after the api method call, we'll get an attribute error
            resp = apimethod(**args)

            use_internal, use_ssh, debug = self.__lifo.get()

            self.config.debug = debug
            self.config.use_ssh = use_ssh
            self.config.use_internal = use_internal
        except SecureDRMSError as exc:
            print(exc.msg, file=sys.stderr)

            import traceback
            print(traceback.format_exc(), file=sys.stderr)

            raise

        return resp

    def _extract_series_name(self, ds):
        parsed = self.parse_spec(ds)
        return parsed['subsets'][0]['seriesname'].lower()

    def _parse_spec(self, spec):
        return self._json.parse_recset(spec)

    def filename_from_export_record(self, rs, old_fname=None):
        '''
        return a file name for a export record-set DRMS record that is generated by applying an algorithm to the specification parts

        Parameters
        ----------
        rs : str
            a DRMS record-set specification such as "hmi.M_720s[2018.3.2_00:00_TAI]{magnetogram}"
        [ old_fname : str ]
            the original file name as it appears in the DRMS record

        Return Value
        ------------
        str
            the generated file name
        '''
        return self._filename_from_export_record(rs, old_fname)

    # Public Interface
    def check_email(self, address):
        '''
        verify that `address` is registered for use with the export system; an address can be registered by visiting http://jsoc.stanford.edu/ajax/register_email.html

        Parameters
        ----------
        email : str
            an email address

        Return Value
        ------------
        bool
            True if the email address is valid and registered for export, False otherwise
        '''
        args = { 'email' : address }

        # call the parent's check_email() method
        return self._execute(super().check_email, **args)

    def export(self, ds, method='url_quick', protocol='as-is', protocol_args=None, filename_fmt=None, n=None, email=None, requestor=None, synchronous_export=None):
        '''
        submit a data-export request to the DRMS server; if the export can be performed synchronously, and the user has not specifically said they do not want the export to be synchronous (by setting `synchronous_export` to False), then the export is a syncrhonous one

        Parameters
        ----------
        ds : str
            the record-set specification for the DRMS records to export (e.g., 'hmi.B_720s[2014.7.8/60m]{field}')
        [ method : str ]
            the export method, which is one of : 'url', 'url_direct', 'url_quick' (the default), 'url-tar', 'ftp', 'ftp-tar'
        [ protocol : str ]
            the export protocol, which is one of 'as-is' (the default), 'fits', 'jpg', 'mpg' and 'mp4'
        [ protocol_args : dict ]
            protocol arguments for protocols 'jpg', 'mpg', and 'mp4', which include 'ct', 'scaling', 'min', 'max', and 'size'
        [ filename_fmt : str ]
            custom filename template string to use when generating the names for exported files that appear in the tar file; if None (the default), then '{seriesname}.{recnum:%%lld}.{segment}' is used; for 'url_quick'/'as-is' data exports, this is ignored
        [ n : int ]
            a maximum of abs(`n`) records are exported; if `n` > 0 the first abs(`n`) records of the record set are returned, if `n` < 0 the last abs(`n`) records of the record set are returned, and `n` is None (the default), then no limit is applied
        [ email : str ]
            an export-system registered email address; if None (the default), then the client-wide email address, SecureClient.email, is used
        [ requestor : str or False ]
            the export user name; if None (the default), then a name is automatically generated from the user's registered email address; if False, then no requestor argument will be submitted during the export request
        [ synchronous_export : bool ]
            if True, then the SecureClient.export method will perform a synchronous export, provided the export method is 'url' or 'url_direct', the export protocol is FITS, and the segments being exported are of FITS protocol; if None (the default), then the client-wide synchronous_export flag, SecureClient.synchronous_export, is used

        Return Value
        ------------
        object (ExportRequest)
            an ExportRequest instance which can be used to check on the status of asynchronous exports, and to download exported data files
        '''
        # n argument is invalid (currently) for drms-export-to-stdout, and it doesn't really make any sense for jsoc_fetch either
        if filename_fmt is None:
            # override parent implementation
            series = self._extract_series_name(ds)
            filename_fmt = self._generate_filenamefmt(series)

        if synchronous_export is None:
            synchronous_export = self._synchronous_export

        # new feature - if the user is requesting FITS files via url_quick/FITS, then skip the export system altogether
        # and use drms-export-to-stdout; this module combines the image with the metadata into FITS files, wrapped in a
        # tar file, synchronous; a true export is avoided - drms-export-to-stdout reads the image files (bringing them
        # online if need be), and then combines the metadata, creating a tar file of all resulting FITS files; export_fits()
        # is the implementation of this feature, but export() makes use of this feature to avoid using export system; not all
        # NetDRMS sites have a full export system, but they all have drms-export-to-stdout

        if synchronous_export:
            # try running the new-feature method export_fits() - if the arguments are not suitable for export_fits(),
            # call the parent export() instead
            try:
                if self._server.check_supported('export_fits', isinstance(self, SSHClient), self._use_internal):
                    # exporting online SUs as fits with the new method only works if the method is url_quick (so
                    # there no true export request is performed), all segments have to be protocol fits

                    if method.lower() != 'url_direct' and method.lower() != 'url':
                        raise SecureDRMSArgumentError('[ SecureClient.export() ] method argument {method} not suitable for export_fits() shortcut'.format(method=method))

                    # call info() and make sure all segments are of fits protocol (drms-export-to-stdout does not support
                    # non-fits segments)
                    segments = self.info(series).segments

                    # here's how to see if any rows in the segment DataFrame contain anything other than segments of protocol fits:
                    if segments[segments.protocol != 'fits'].shape[0] > 0:
                        raise SecureDRMSArgumentError('[ SecureClient.export() ] at least one segment cannot be exported as fits - not suitable for export_fits() shortcut')

                    if protocol.lower() != 'fits':
                        raise SecureDRMSArgumentError('[ SecureClient.export() ] protocol argument {protocol} not suitable for export_fits() shortcut'.format(protocol=protocol))

                    if n is not None:
                        # drms-export-to-stdout does not support n (which is supposed by jsoc_fetch in the 'process=n' parameter)
                        raise SecureDRMSArgumentError('[ SecureClient.export() ] n argument {n} not suitable for export_fits() shortcut'.format(n=n))

                # OK to use export_fits() shortcut
                if self._debug:
                    print('[ SecureClient.export() ] executing export_fits() shortcut')

                # email is not needed for drms-export-to-stdout, so ignore it here (users will have authenticated already)
                args = { 'api_name' : 'export_fits', 'spec' : ds, 'filename_fmt' : filename_fmt }

                # returns a SecureExportRequest
                return self._execute(self._export_fits, **args)
            except SecureDRMSArgumentError as exc:
                if self._debug:
                    print(exc.args[0])
                pass
            except:
                # some problem calling self._export_fits, propagate to a handler
                raise

        args = { 'ds' : ds, 'method' : method, 'protocol' : protocol, 'protocol_args' : protocol_args, 'filenamefmt' : filename_fmt, 'n' : n, 'email' : email, 'requestor' : requestor }

        # call the parent's export() method
        if self._debug:
            print('[ SecureClient.export() ] calling parent export() with args ' + dumps(args))

        return self._execute(super().export, **args)

    def export_fits(self, spec, download_directory, filename_fmt=None):
        '''
        synchronously export FITS files contained by DRMS records specified by `spec`; unlike export(), this method does not require the presence of the formal export system and no export Storage Unit will be created on the server; attempts to export data segment files with a protocol other than FITS will fail
        [ DEPRECATED in SSH client - use SSHClient.export_package() ]

        Parameters
        ----------
        spec : str
            a DRMS record-set specification such as "hmi.M_720s[2018.3.2_00:00_TAI]{magnetogram}"; the DRMS data segments to which `spec` refers must be of FITS protocol
        [ filename_fmt : str ]
            custom filename template string to use when generating the names for exported files that appear in the tar file; if None (the default), then '{seriesname}.{recnum:%%lld}.{segment}' is used

        Return Value
        ------------
        object (SecureExportRequest)
            a SecureExportRequest instance which can be used to check on the status of the export, and to download exported data files
        '''
        if isinstance(self, SSHClient):
            # do not allow user to create the export package (tar file) on the server
            raise SecureDRMSDeprecatedError('[ SecureClient.export_fits() ] deprecated method; call export_package() instead')

        args = { 'api_name' : 'export_fits', 'spec' : spec, 'filename_fmt' : filename_fmt }

        # returns a SecureExportRequest
        return self._execute(self._export_fits, **args)

    def export_package(self, spec, download_directory, filename_fmt=None):
        args = { 'api_name' : 'export_package', 'download_directory' : download_directory, 'spec' : spec, 'filename_fmt' : filename_fmt }

        # returns a SecureExportRequest
        return self._execute(self._export_package, **args)

    def export_from_id(self, requestid):
        '''
        for asynchronous exports, obtain from the server request disposition information and create an ExportRequest instance, which can then be used to check on the pending export status, and to download files whose export has completed

        Parameters
        ----------
        requestid : str
            the ID string of the export request (e.g., 'JSOC_20191030_163')

        Return Value
        ------------
        object (ExportRequest)
            an ExportRequest instance which can be used to check on the status of the export, and to download data files whose export has completed
        '''
        args = { 'requestid' : requestid }

        # call the parent's export_from_id() method
        return self._execute(super().export_from_id, **args)

    def info(self, series):
        '''
        obtain DRMS data series information from the DRMS server

        Parameters
        ----------
        series : str
            a DRMS data series

        Return Value
        ------------
        object (SeriesInfo)
            a SeriesInfo instance which contains information about the data series, such as a description of the series and lists of DRMS keywords and segments and links
        '''
        args = { 'ds' : series }

        # override utils._extract_series_name
        saved = utils._extract_series_name
        utils._extract_series_name = self._extract_series_name

        response = self._execute(super().info, **args)

        # restore
        utils._extract_series_name = saved

        return response

    def keys(self, series):
        '''
        obtain from the DRMS server the list of DRMS keywords for `series`; use the SecureClient.info method for a more complete description of `series`

        Parameters
        ----------
        series : str
            a DRMS data series

        Return Value
        ------------
        list
            the keyword list for `series`
        '''
        args = { 'ds' : series }

        # call the parent's keys() method
        return self._execute(super().keys, **args)

    def parse_spec(self, spec):
        '''
        obtain from the DRMS server the DRMS elements (e.g., subsets, namespace, db tables, series, filter)  of `spec`

        Parameters
        ----------
        spec : str
            a DRMS record-set specification such as "hmi.M_720s[2018.3.2_00:00_TAI]{magnetogram}"

        Return Value
        ------------
        dict
            a dict that represents a JSON string of `spec` info; an example:
            {
                'spec' : 'hmi.m_45s[2015.2.2]{magnetogram}',
                'atfile' : False,
                'hasfilts' : True,
                'nsubsets' : 1,
                'subsets' :
                [
                    {
                        'spec' : 'hmi.m_45s[2015.2.2]{magnetogram}',
                        'settype' : 'drms'
                        'seriesname' : 'hmi.m_45s'
                        'seriesns' : 'hmi',
                        'seriestab' : 'm_45s',
                        'filter' : '[2015.2.2]',
                        'segments'v: '{magnetogram}',
                        'autobang'v: False
                    }
                ],
                'errMsg': None
            }
        '''
        args = { 'spec' : spec }

        return self._execute(self._parse_spec, **args)

    def pkeys(self, series):
        '''
        obtain from the DRMS server the list of DRMS keywords that compose the "prime key" of `series`; use the SecureClient.info method for a more complete description of `series`

        Parameters
        ----------
        series : str
            a DRMS data series

        Return Value
        ------------
        list
            the prime-key keyword list for `series`

        '''
        args = { 'ds' : series }

        # call the parent's pkeys() method
        return self._execute(super().pkeys, **args)

    def query(self, ds, key=None, seg=None, link=None, convert_numeric=True, skip_conversion=None, pkeys=False, rec_index=False, n=None):
        '''


        Parameters
        ----------
        ds : str
            DRMS record-set specification (e.g., 'hmi.B_720s[2014.7.8/60m]{field}')
        [ key : str or list ]
            a single DRMS keyword name, or a list of keyword names; if None (default), then return no keyword information
        [ seg : str or list ]
            a single DRMS segment name, or a list of segment names; if None (default), then return no segment information
        [ link : str or list ]
            a single DRMS link name, or a list of link names; if None (default), then return no link information
        [ convert_numeric : bool ]
            if True (the default), then numeric-datatype keywords not in the list `skip_conversion` are converted from string to numbers
        [ skip_conversion : list ]
            a list of keywords to be skipped when performing a numeric conversion; if None (the default) then no keywords are skipped
        [ pkeys : bool ]
            if True, then the set of keys specified by `key` is augmented with the prime-key keywords (default is False)
        [ rec_index : bool ]
            if True, then the rows in the resulting pandas.DataFrame are indexed by record specification; if False (the default) the rows are indexed by numeric index
        [ n : int ]
            a maximum of abs(`n`) records are returned; if `n` > 0 the first abs(`n`) records of the record set are returned, if `n` < 0 the last abs(`n`) records of the record set are returned, and `n` is None (the default), then no limit is applied

        Return Value
        ------------
        tuple
            a tuple of pandas.DataFrames:
                result[0] : keyword results; this DataFrame is not None if `key` is not None or `pkeys` is set to True
                result[1] : segment results; his DataFrame is not None if `seg` is not None
                result[3] : link results; this DataFrame is not None if `link` is not None
        '''
        args = { 'ds' : ds, 'key' : key, 'seg' : seg, 'link' : link, 'convert_numeric' : convert_numeric, 'skip_conversion' : skip_conversion, 'pkeys' : pkeys, 'rec_index' : rec_index, 'n' : n }

        # call the parent's query() method
        return self._execute(super().query, **args)

    def series(self, regex=None, full=False):
        '''
        return a list of accessible data series

        Parameters
        ----------
        [ regex : str ]
            a filtering POSIX Extended Regular Expression; the list returned will contain only DRMS data series whose names match the regexp; if None (the default) then no filtering is performed
        [ full : bool ]
            if True, the list returned includes the prime-key and a description of each data series (default is False)

        Return Value
        ------------
        list or object (pandas.DataFrame)
            if `full` is True, returns a pandas.DataFrame of series and information for series that match `regex`, otherwise returns a list of accessible series that match `regex`; if the server is configured for external access and web_show_series_wrapper/ssh_show_series_wrapper, then in addition to listing DRMS 'external' data series, lists 'internal' series (ones that reside in the internal, private database) that the server makes publicly accessible
        '''
        args = { 'api_name' : 'series', 'regex' : regex, 'full' : full }

        # call the parent's series() method [ the server configuration parameters in the parent method will be ignored; they will be used,
        # however, in the self._json.show_series*() methods ]
        return self._execute(self._series, **args)

    @property
    def config(self):
        return self._config

    @property
    def debug(self):
        return self._debug

    @debug.setter
    def debug(self, value):
        self._debug = value
        super().debug = value # call parent setter, which sets self._json._debug

    @property
    def json_client(self):
        return self._json

    @property
    def use_internal(self):
        return self._use_internal


class BasicAccessClient(SecureClient):
    '''
    a class that provides secure HTTP access (Basic Access authentication) to DRMS API methods
    '''
    def __init__(self, config, use_internal=False, email=None, synchronous_export=True, verbose=False, debug=False):
        '''
        Parameters
        ----------
        config : SecureServerConfig
            a secure server configuration
        [ use_internal : bool ]
            if True, then access will be to privately accessible data series, otherwise access will be to publicly accessible series (external DRMS data series plus selectively exposed internal data series); (default is False)
        [ email : str ]
            an export-system registered email address
        [ synchronous_export : bool ]
            if True (the default), then the SecureClient.export method will perform a synchronous export, provided the export method is 'url' or 'url_direct', the export protocol is FITS, and the segments being exported are of FITS protocol
        [ verbose : bool ]
            if True, print export status; if False (default) do not print export status
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._json = BasicAccessHttpJsonClient(config=config, use_internal=use_internal, debug=debug)
        super().__init__(config, use_internal=use_internal, email=email, synchronous_export=synchronous_export, verbose=verbose, debug=debug)

    def __repr__(self):
        return '<BasicAccessClient "{name}">'.format(name=self._config.name)

    def _export_fits(self, spec, download_directory, filename_fmt=None):
        # since a tar disk file is not created on the server, this method is synonymous with self._export_package()
        return self._export_package(spec, download_directory, filename_fmt)

    def _export_package(self, spec, download_directory, filename_fmt=None):
        # we are going to run drms-export-to-stdout on the server; `download_directory` refers a local directory to which the tar file will be downloaded - if None, then the tar file will be in-memory only
        resp = self._json.exp_package(spec, download_directory, filename_fmt)

        return SecureExportRequest(resp, self, remote_user=None, remote_host=None, remote_port=None, on_the_fly=True, defer_package=True, debug=self.debug)

    def _series(self, regex=None, full=False):
        if self._use_internal or self._server.web_show_series_wrapper is None:
            # do not use wrapper (use show_series)
            resp = self._json.show_series(regex)
            status = resp.get('status')
            if status != 0:
                self._raise_query_error(resp)

            if full:
                keys = ('name', 'primekeys', 'note')
                if not resp['names']:
                    return pandas.DataFrame(columns=keys)

                recs = [ (it['name'], _split_arg(it['primekeys']), it['note']) for it in resp['names'] ]
                return pandas.DataFrame(recs, columns=keys)
            else:
                if not resp['names']:
                    return []

                return [ it['name'] for it in resp['names'] ]
        else:
            # use wrapper (showextseries)
            resp = self._json.show_series_wrapper(regex, info=full)
            if full:
                keys = ('name', 'note')
                if not resp['seriesList']:
                    return pandas.DataFrame(columns=keys)

                recs = []
                for it in resp['seriesList']:
                    name, info = tuple(it.items())[0]
                    note = info.get('description', '')
                    recs.append((name, note))

                return pandas.DataFrame(recs, columns=keys)
            else:
                return resp['seriesList']


class SSHClient(SecureClient):
    '''
    a class that provides ssh access to DRMS API methods
    '''
    def __init__(self, config, use_internal=False, email=None, synchronous_export=True, verbose=False, debug=False):
        '''
        Parameters
        ----------
        config : SecureServerConfig
            a secure server configuration
        [ use_internal : bool ]
            if True, then access will be to privately accessible data series, otherwise access will be to publicly accessible series (external DRMS data series plus selectively exposed internal data series); (default is False)
        [ email : str ]
            an export-system registered email address
        [ synchronous_export : bool ]
            if True (the default), then the SecureClient.export method will perform a synchronous export, provided the export method is 'url' or 'url_direct', the export protocol is FITS, and the segments being exported are of FITS protocol
        [ verbose : bool ]
            if True, print export status; if False (default) do not print export status
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        self._json = SSHJsonClient(config=config, use_internal=use_internal, debug=debug)
        super().__init__(config, use_internal=use_internal, email=email, synchronous_export=synchronous_export, verbose=verbose, debug=debug)

    def __repr__(self):
        return '<SSHClient "{name}">'.format(name=self._config.name)

    # private methods
    def _export_fits(self, spec, download_directory, filename_fmt):
        # we are going to run drms-export-to-stdout on the server
        resp = self._json.exp_fits(spec, download_directory, filename_fmt)

        return SecureExportRequest(resp, self, remote_user=self._config.ssh_remote_user, remote_host=self._config.ssh_remote_host, remote_port=self._config.ssh_remote_port, on_the_fly=True, defer_package=False, debug=self.debug)

    def _export_package(self, spec, download_directory, filename_fmt):
        dummy_series_response = self._json.exp_package(spec=spec, download_directory=download_directory, filename_fmt=filename_fmt)

        return SecureExportRequest(dummy_series_response, self, remote_user=self._config.ssh_remote_user, remote_host=self._config.ssh_remote_host, remote_port=self._config.ssh_remote_port, on_the_fly=True, defer_package=True, debug=self.debug)

    def _series(self, regex=None, full=False):
        if self._use_internal or self._server.ssh_show_series_wrapper is None:
            # binary executable
            resp = self._json.show_series(regex)
            status = resp.get('status')
            if status != 0:
                self._raise_query_error(resp)

            if full:
                keys = ('name', 'primekeys', 'note')
                if not resp['names']:
                    return pandas.DataFrame(columns=keys)

                recs = [ (it['name'], _split_arg(it['primekeys']), it['note']) for it in resp['names'] ]
                return pandas.DataFrame(recs, columns=keys)
            else:
                if not resp['names']:
                    return []

                return [ it['name'] for it in resp['names'] ]
        else:
            # script
            resp = self._json.show_series_wrapper(regex, info=full)
            if full:
                keys = ('name', 'note')
                if not resp['seriesList']:
                    return pandas.DataFrame(columns=keys)

                recs = []
                for it in resp['seriesList']:
                    name, info = tuple(it.items())[0]
                    note = info.get('description', '')
                    recs.append((name, note))

                return pandas.DataFrame(recs, columns=keys)
            else:
                return resp['seriesList']

    # public methods
    def clear_timer(self):
        '''

        Parameters
        ----------
        None

        Return Value
        ------------
        None
        '''
        self._json.clear_timer()


class SecureClientFactory(object):
    '''
    a class to make a SecureClient factory, which can then be used to create a SecureClient instance
    '''
    __clients = []

    def __init__(self, *, server=None, email=None, verbose=False, debug=False):
        '''
        Parameters
        ----------
        [ server : str ]
            name of the secure-server configuration
        [ email : str ]
            an export-system registered email address
        [ verbose : bool ]
            if True, print export status; if False (the default) do not print export status
        [ debug : bool ]
            if True, print debugging statements (default is False)
        '''
        if server is not None:
            self._config = SecureServerConfig.get(server)
            self._config.debug = debug
        self._email = email
        self._verbose = verbose
        self._debug = debug

    def create_client(self, *, server=None, use_ssh=False, use_internal=False, connection_info=None, debug=None):
        '''
        return a SecureClient that can be used to access the DRMS server via the set of API methods in the SecureClient class

        Parameters
        ----------
        [ server : str ]
            name of the secure-server configuration
        [ use_ssh : bool ]
            if True, interface programs that use the SSH-access methods will be used; if False (the default), programs that use the HTTP-access methods will be used
        [ use_internal : bool ]
            if True, interface programs that access publicly accessible data series will be used; if False (the default), programs that access privately accessible series will be used
        [ connection_info : dict ]
            if not None, then use the database connection information contained within when connecting to the database, overriding the default connection arguments in the configuraton
            {
              'dbhost' : <str - the database host>,
              'dbport' : <int - the port on the database host>,
              'dbuser' : <str - the database role>,
              'dbname' : <str - the database name>
            }
        [ debug : bool ]
            if True, print debugging statements (default is False); if None (the default), then SecureClientFactory._debug is used

        Return Value
        ------------
        object (SecureClient)
            return an SSHClient if `use_ssh` is True, otherwise return a BasicAccessClient; the SecureClient returned can then be used to access the DRMS server
        '''
        client = None
        args = { 'email' : self._email, 'verbose' : self._verbose, 'debug' : self._debug if debug is None else debug, 'use_internal' : use_internal, 'connection_info' : connection_info }

        if server is not None:
            self._config = SecureServerConfig.get(server)
            self._config.debug = self._debug

        if not hasattr(self, '_config'):
            raise SecureDRMSArgumentError(f'[ {self.__class__.__name__}.create_client() ] no server configuration specified (must be specified either when creating factory or creating client)')

        # this will ensure that `connection_info` becomes an attribute of the configuration internal dict (`d`)
        self._config.connection_info = connection_info

        if use_ssh:
            client = SSHClient(self._config, **args)

            # add to list of clients whose timers might need to be canceled upon termination
            self.__class__._insert_client(client)
        elif False:
            # if we need additional clients, like https, make a new elsif statement immediately above this one
            pass
        else:
            # default to basic access
            client = BasicAccessClient(self._config, **args)

        return client

    def create_http_client(self, *, use_internal=False, connection_info=None, debug=None):
        '''
        return a BasicAccessClient that can be used to access the DRMS server via the set of API methods in the BasicAccessClient class

        Parameters
        ----------
        [ use_internal : bool ]
            if True, interface programs that access publicly accessible data series will be used; if False (the default), programs that access privately accessible series will be used
        [ connection_info : dict ]
            if not None, then use the database connection information contained within when connecting to the database, overriding the default connection arguments in the configuraton
            {
              'dbhost' : <str - the database host>,
              'dbport' : <int - the port on the database host>,
              'dbuser' : <str - the database role>,
              'dbname' : <str - the database name>
            }
        [ debug : bool ]
            if True, print debugging statements (default is False); if None (the default), then SecureClientFactory._debug is used

        Return Value
        ------------
        object (BasicAccessClient)
            return an BasicAccessClient, which can then be used to access the DRMS server
        '''
        return self.create_client(use_ssh=False, use_internal=use_internal, connection_info=connection_info, debug=debug)

    def create_ssh_client(self, *, use_internal=False, connection_info=None, debug=None):
        '''
        return an SSHClient that can be used to access the DRMS server via the set of API methods in the SSHClient class

        Parameters
        ----------
        [ use_internal : bool ]
            if True, interface programs that access publicly accessible data series will be used; if False (the default), programs that access privately accessible series will be used
        [ connection_info : dict ]
            if not None, then use the database connection information contained within when connecting to the database, overriding the default connection arguments in the configuraton
            {
              'dbhost' : <str - the database host>,
              'dbport' : <int - the port on the database host>,
              'dbuser' : <str - the database role>,
              'dbname' : <str - the database name>
            }
        [ debug : bool ]
            if True, print debugging statements (default is False); if None (the default), then SecureClientFactory._debug is used

        Return Value
        ------------
        object (SSHClient)
            return an SSHClient, which can then be used to access the DRMS server
        '''
        return self.create_client(use_ssh=True, use_internal=use_internal, connection_info=connection_info, debug=debug)

    @classmethod
    def _insert_client(cls, secure_client):
        cls.__clients.append(secure_client)

    @classmethod
    def terminator(cls, *args):
        for client in cls.__clients:
            # only SSHClients have a time to be cleared (if client is a BasicAccessClient, then this will raise)
            client.clear_timer()

# intercept ctrl-c and kill the child timer threads
signal.signal(signal.SIGINT, SecureClientFactory.terminator)
signal.signal(signal.SIGTERM, SecureClientFactory.terminator)
signal.signal(signal.SIGHUP, SecureClientFactory.terminator)

# register secure JSOC DRMS server

server_config_common = SecureServerConfig(
    name='common_do_not_use',
    download_directory='/tmp',
    has_full_export_system=True,
    server_tmp='/tmp',
    ssh_base_bin='/home/jsoc/cvs/Development/JSOC/bin/linux_avx',
    ssh_base_script='/home/jsoc/cvs/Development/JSOC/scripts',
    ssh_check_email='checkAddress.py',
    ssh_check_email_addresstab='jsoc.export_addresses',
    ssh_check_email_domaintab='jsoc.export_addressdomains',
    ssh_export_fits='drms-export-to-stdout',
    ssh_export_fits_args=
    {
        'JSOC_DBHOST' : 'hmidb2',
        'JSOC_DBUSER' : 'production',
        'maxfilesize' : 4294967296
    },
    ssh_export_fits_internal_args=
    {
        'JSOC_DBHOST' : 'hmidb',
        'JSOC_DBUSER' : 'production',
        'maxfilesize' : 4294967296
    },
    ssh_jsoc_info='jsoc_info',
    ssh_jsoc_info_args=
    {
        'dbhost' : 'hmidb2',
    },
    ssh_jsoc_info_internal_args=
    {
        'JSOC_DBHOST' : 'hmidb',
        'JSOC_DBUSER' : 'production'
    },
    ssh_jsoc_fetch='jsoc_fetch',
    ssh_jsoc_fetch_args=
    {
        'dbhost' : 'hmidb2'
    },
    ssh_jsoc_fetch_internal_args=
    {
        'JSOC_DBHOST' : 'hmidb',
        'JSOC_DBUSER' : 'production'
    },
    ssh_parse_recset='drms_parserecset',
    ssh_remote_env=RuntimeEnvironment(
        {
            'BOGUS_ENV' : 'bogus_value'
            # 'LD_LIBRARY_PATH' : '/nasa/intel/Compiler/2018.3.222/compilers_and_libraries_2018.3.222/linux/compiler/lib/intel64'
        }
    ),
    ssh_remote_user='arta',
    ssh_remote_host='solarport',
    ssh_remote_port=22,
    ssh_show_series='show_series',
    ssh_show_series_args=
    {
        'JSOC_DBHOST' : 'hmidb2',
        'JSOC_DBUSER' : 'production'
    },
    ssh_show_series_internal_args=
    {
        'JSOC_DBHOST' : 'hmidb',
        'JSOC_DBUSER' : 'production'
    },
    http_download_baseurl='http://jsoc.stanford.edu/',
    ftp_download_baseurl='ftp://pail.stanford.edu/export/')

server_config_external = SecureServerConfig(config=server_config_common)
server_config_external.name = 'jsoc_external'
server_config_internal = SecureServerConfig(config=server_config_common)
server_config_internal.name = 'jsoc_internal'

SecureServerConfig.register_server(server_config_external)
SecureServerConfig.register_server(server_config_internal)
