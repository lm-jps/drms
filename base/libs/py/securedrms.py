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
>>> # list DRMS series whose names contain the string 'su_arta'
>>> sshclient.series('lev1')
please enter password for arta@solarport
Password:
['aia.lev1', 'aia.lev1_euv_12s', 'aia.lev1_uv_24s', 'aia.lev1_vis_1h', 'aia_test.lev1_12s4arc', 'hmi.lev1_cal', 'hmi.lev1_dcon', 'iris.lev1', 'iris.lev1_nrt', 'mdi.fd_M_96m_lev182', 'mdi.fd_m_lev182']
>>>
>>> # create a Basic Access HTTP client
>>> httpclient = factory.create_client()
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

# third party imports
import pandas
from six.moves.urllib import request as sixUrlRequest

# local imports
import drms
from drms import utils
from drms.client import Client as DRMSClient, ExportRequest, SeriesInfo
from drms.config import ServerConfig, register_server
from drms.error import DrmsQueryError, DrmsExportError, DrmsOperationNotSupported
from drms.json import HttpJsonClient, HttpJsonRequest
from drms.utils import _split_arg


__all__ = [ 'SecureDRMSConfigurationError', 'SecureDRMSConfigurationError', 'SecureDRMSArgumentError', 'SecureDRMSTimeOutError', 'SecureDRMSResponseError', 'RuntimeEnvironment', 'SecureServerConfig', 'BasicAccessHttpJsonRequest', 'BasicAccessHttpJsonClient', 'SSHJsonRequest', 'SSHJsonClient', 'BasicAccessClient', 'SSHClient', 'SecureClientFactory' ]

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

class SecureDRMSConfigurationError(SecureDRMSError):
    '''
    invalid or missing Secure DRMS server configuration property
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

class SecureDRMSMethodError(SecureDRMSError):
    '''
    method is not valid for the current configuration
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

class SecureDRMSResponseError(SecureDRMSError):
    '''
    time-out waiting for DRMS Server to respond
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


class RuntimeEnvironment(object):
    '''
    a class to manage environment variables needed to initialize the remote-host runtime environment
    
    Constructor 
    -----------
        env : dict
            a dictionary where the key is an environment variable name, and the value is the variable's string value

        The constructor iterates through `env`, creating one RuntimeEnvironment instance attribute for each element (each element is for a single environment variable).
        
    Attributes
    ----------
    <env var> : str
        For each environment variable <env var>, a str attribute with the same name exists.
    
    Public Instance Methods
    -----------------------
    bash_cmd : [ None ] -> list
        Returns a list containing one element per environment variable stored as an attribute in the instance; each element is a bash command that APPENDS a value to one existing variable (like [ "export VAR1=$VAR1'value1'", "export VAR2=$VAR2'value2" ]). If the environment variable does not already exist, it is created.
    '''
    def __init__(self, env):
        for envVar in env:
            setattr(self, envVar, env[envVar])
                
    def bash_cmd(self):
        cmds = []

        if len(vars(self)) > 0:
            for envVar, val in vars(self).items():
                cmds.append('export ' + envVar + '=$' + envVar + "'" + val + "'")

        return cmds


class SecureServerConfig(ServerConfig):
    '''
    a class to create, maintain, and store DRMS server configurations
    
    Constructor
    -----------
        [ config : SecureServerConfig ]
            `config` is an existing SecureServerConfig used to initialize the new SecureServerConfig.
        kwargs : keyword-argument dict
            `kwargs` contains a dict of keyword-argument parameters that will act as a the SecureServerConfig attributes; the keyword argument `name` must exist.

        The new SecureServerConfig instance is optionally initialized by copying the attributes in `config`. The keyword arguments in `kwargs` are then added to the instance.
        
    Attributes
    ----------
        cgi_baseurl_authority : str
            For BasicAccessClient clients, `cgi_baseurl_authority` contains the clear-text HTTP Basic Access authentication name:password credentials.
        cgi_baseurl_authorityfile : str
            For BasicAccessClient clients, `cgi_baseurl_authorityfile` contains the path to a file that contains a function that returns the base64-encoded name:password credentials. The file must contain a single function named getAuthority:
            
            def getAuthority():
                return 'XXXXXXXXXXXXXXXXXXXX'
                
        cgi_baseurl_internal : str
            For BasicAccessClient clients, `cgi_baseurl_internal`
                
        ssh_base_bin : str
            For SSHClient clients, `ssh_base_bin` contains the path to the remote-server directory that contains all binary executables referenced in this module
        ssh_base_script : str
            For SSHClient clients, `ssh_base_script` contains the path to the remote-server directory that contains all scripts referenced in this module
        ssh_check_email : str
            For SSHClient clients, `ssh_check_email` contains the remote-server script file that checks the registration status of export email addresses.
        ssh_check_email_addresstab : str
            For SSHClient clients, `ssh_check_email_addresstab` contains name of the remote-server database table that contains the registered local names.
        ssh_check_email_domaintab : str
            For SSHClient clients, `ssh_check_email_domaintab` contains name of the remote-server database table that contains the domains of the registered local names.
        ssh_jsoc_fetch : str
            For SSHClient clients, `ssh_jsoc_fetch` contains the remote-server binary executable that initiates export requests, and reports the status on those requests.
        ssh_jsoc_info : str
            For SSHClient clients, `ssh_jsoc_info` contains the remote-server binary executable that provides DRMS record-set information.
        ssh_jsoc_info_args : dict
            For SSHClient clients, `ssh_jsoc_info_args`
        ssh_parse_recset : str
            For SSHClient clients, `ssh_parse_recset` contains the remote-server binary executable that parses DRMS record-set strings into parts (e.g., series name, filters, segment list, etc.).
        ssh_remote_env : str
            For SSHClient clients, `ssh_remote_env` contains a dict of environment variables to be passed along to the remote server.
        ssh_remote_host : str
            For SSHClient clients, `ssh_remote_host` contains the name of the remote host.
        ssh_remote_port : int
            For SSHClient clients, `ssh_remote_port` contains the port number of the remote host on which the SSH sevice listens.
        ssh_remote_user : str
            For SSHClient clients, `ssh_remote_user` contains the name of the user to run the remote command as.
        ssh_show_series : str
            For SSHClient clients, `ssh_show_series` contains the remote-server binary executable that prints the series served.
        ssh_show_series_args : dict
            `ssh_show_series_args` contains arguments for the `ssh_show_series` SSH call; these arguments are used when accessing an EXTERNAL series server:
                JSOC_DBHOST : str
                    the external database host
        ssh_show_series_internal_args : dict
            `ssh_show_series_internal_args` contains arguments for the `ssh_show_series` SSH call; these arguments are used when accessing an INTERNAL series server:
                JSOC_DBHOST : str
                    the internal database host
        ssh_show_series_wrapper : str
            For SSHClient clients, `ssh_show_series_wrapper` contains the remote-server script that prints the public-accessible external series served PLUS the public-accessible internal series.
        ssh_show_series_wrapper_args : dict
            `ssh_show_series_wrapper_args` contains arguments for the `ssh_show_series_wrapper` SSH call; for an external server, the arguments are:
                dbhost : str
                    the database host (as seen from the SSH server)
                --wlfile : str
                    the path to the remote-server DRMS series white-list file
    
    Class Variables
    ---------------
    __configs : dict
        a container of all registered server configurations
    __validKeys : list
        a list of all valid supplemental (to ServerConfig._valid_keys) configuration properties

    Public Class Methods
    --------------------
    register_server : config:object (SecureServerConfig) -> None
        Add the server configuration `config` to the dictionary of SecureServerConfigs (SecureServerConfig.__configs); add `config` to the global dictionary of ServerConfigs (config._server_configs)
    get : [ name:str ] -> SecureServerConfig
        Return the SecureServerConfig by the name of `name`
    set : name:str, config:SecureServerConfig -> None
        Add the server configuration `config` to the dictionary of SecureServerConfigs (SecureServerConfig.__configs)
        
    Public Instance Methods
    -----------------------
    check_supported : op:string -> bool
        Returns true of if the operation `op` is supported by the instance
    '''
    __configs = {}
    __validKeys = [ 'cgi_baseurl_internal', 'cgi_baseurl_authority', 'cgi_baseurl_authorityfile', 'cgi_export_fits', 'qcgi_export_fits_args', 'qcgi_export_fits_internal_args', 'cgi_parse_recset', 'has_full_export_system', 'url_baseurl_internal', 'url_baseurl_authority', 'server_tmp', 'ssh_base_bin', 'ssh_base_script', 'ssh_check_email', 'ssh_check_email_addresstab', 'ssh_check_email_domaintab', 'ssh_export_fits', 'ssh_export_fits_args', 'ssh_export_fits_internal_args', 'ssh_jsoc_fetch', 'ssh_jsoc_fetch_args', 'ssh_jsoc_fetch_internal', 'ssh_jsoc_fetch_internal_args', 'ssh_jsoc_info', 'ssh_jsoc_info_args', 'ssh_jsoc_info_internal', 'ssh_jsoc_info_internal_args', 'ssh_parse_recset', 'ssh_remote_env', 'ssh_remote_host', 'ssh_remote_port', 'ssh_remote_user', 'ssh_show_series', 'ssh_show_series_args', 'ssh_show_series_internal_args', 'ssh_show_series_wrapper', 'ssh_show_series_wrapper_args' ]


    def __init__(self, config=None, **kwargs):
        # add parameters for the login information
        for key in self.__validKeys:
            self._valid_keys.append(key)

        # add the authority to the base URL
        kwargsToParent = kwargs
        if 'cgi_baseurl' in kwargs or 'cgi_baseurl_internal' in kwargs:
            if 'cgi_baseurl_authority' not in kwargs and 'cgi_baseurl_authorityfile' in kwargs:
                # authority is on the command line (priority over providing authority in a file)
                if os.path.exists(kwargs['cgi_baseurl_authorityfile']):
                    kwargsToParent = kwargs.copy()

                    # get authority from file
                    try:
                        sys.path.append(os.path.dirname(kwargs['cgi_baseurl_authorityfile']))
                        spec = importlib.util.spec_from_file_location('auth', kwargs['cgi_baseurl_authorityfile'])
                        afile = spec.loader.load_module()
                        kwargsToParent['cgi_baseurl_authority'] = base64.b64decode(afile.getAuthority().encode()).decode()
                    except ImportError:
                        raise ValueError('authority file ' + kwargs['cgi_baseurl_authorityfile'] + ' is invalid')
                    except NameError as exc:
                        if re.search(r'getAuthority', str(exc)) is not None:
                            raise ValueError('authority file ' + kwargs['cgi_baseurl_authorityfile'] + ' does not contain getAuthority() definition')
                        raise
                else:
                    raise ValueError('authority file ' + kwargs['cgi_baseurl_authorityfile'] + ' does not exist')

        # default values
        if 'ssh_remote_port' not in kwargs:
            kwargsToParent['ssh_remote_port'] = DEFAULT_SSH_PORT
            
        if 'encoding' not in kwargs:
            kwargsToParent['encoding'] = DEFAULT_SERVER_ENCODING

        super().__init__(config, **kwargsToParent)
        
    def __repr__(self):
        return '<SecureServerConfig "{name}"'.format(name=self.name)
        
    def check_supported(self, op, use_ssh=None, use_internal=None):
        '''
        check if an operation is supported by the server; SecureServerConfiguration.check_supported() requires that
        use_ssh and use_internal be specified, but if SecureServerConfiguration.check_supported() is called from
        the parent Client class, those two arguments will not be provided - they will both default to None; to
        deal with that, we set two attributes, SecureServerConfiguration.use_ssh and SecureServerConfiguration.use_internal
        in SecureClient before calling into Client code (and then we remove them when done)
        '''            
        if use_ssh is None:
            # passed by SecureClient API method call
            use_ssh = self.use_ssh
            
        if use_internal is None:
            # passed by SecureClient API method call
            use_internal = self.use_internal
            
        if self._debug:
            print('[ SecureServerConfig.check_supported ] use_ssh --> {useSsh}, use_internal --> {useInternal}'.format(useSsh=str(use_ssh), useInternal=str(use_internal)))
        
        if use_ssh:
            if use_internal:
                if op == 'check_email' or op == 'email':
                    return self.ssh_check_email is not None and self.ssh_check_email_addresstab is not None and self.ssh_check_email_domaintab is not None and self.ssh_base_script is not None
                elif op == 'export':
                    return self.ssh_jsoc_info_internal is not None and self.ssh_parse_recset is not None and self.ssh_jsoc_fetch_internal is not None and self.ssh_base_bin is not None
                elif op == 'export_fits':
                    return self.ssh_export_fits is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'export_from_id':
                    return self.ssh_jsoc_info_internal is not None and self.ssh_parse_recset is not None and self.ssh_jsoc_fetch_internal is not None and self.ssh_base_bin is not None
                elif op == 'info':
                    return self.ssh_jsoc_info_internal is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'keys':
                    return self.ssh_jsoc_info_internal is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'parse_spec':
                    return self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'pkeys':
                    return self.ssh_jsoc_info_internal is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
                elif op == 'query':
                    return self.ssh_jsoc_info_internal is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
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
                if op == 'check_email' or op == 'email':
                    return self.cgi_check_address is not None and self.cgi_baseurl_internal is not None
                elif op == 'keys' or op == 'pkeys' or op == 'info':
                    return self.cgi_jsoc_info is not None and self.cgi_baseurl_internal is not None
                elif op == 'export' or op == 'export_from_id':
                    return self.cgi_jsoc_info is not None and self.cgi_jsoc_fetch is not None and self.cgi_baseurl_internal is not None
                elif op == 'export_fits':
                    return self.cgi_export_fits is not None and self.cgi_parse_recset is not None and self.cgi_baseurl_internal is not None
                elif op == 'parse_spec':
                    return self.cgi_parse_recset is not None and self.cgi_baseurl_internal is not None
                else:
                    return super().check_supported(op)
            else:
                # external
                if op == 'check_email' or op == 'email':
                    return self.cgi_check_address is not None and self.cgi_baseurl is not None
                elif op == 'keys' or op == 'pkeys' or op == 'info':
                    return self.cgi_jsoc_info is not None and self.cgi_baseurl is not None
                elif op == 'export' or op == 'export_from_id':
                    return self.cgi_jsoc_info is not None and self.cgi_jsoc_fetch is not None and self.cgi_baseurl is not None
                elif op == 'export_fits':
                    return self.cgi_export_fits is not None and self.cgi_parse_recset is not None and self.cgi_baseurl is not None
                elif op == 'parse_spec':
                    return self.cgi_parse_recset is not None and self.cgi_baseurl is not None
                else:
                    return super().check_supported(op)
            
    def set_urls(self, use_internal=False, debug=False):
        if use_internal and self.cgi_baseurl_internal is not None:
            for urlParameter, val in self.to_dict().items():
                if urlParameter.startswith('url'):
                    cgiParameter = 'cgi' + urlParameter[3:]
                    cgiParameterVal = getattr(self, cgiParameter, None)
                    setattr(self, urlParameter, urljoin(self.cgi_baseurl_internal, cgiParameterVal))
                    if debug:                        
                        print('set URL config parameter {param} to {url}'.format(param=urlParameter, url=urljoin(self.cgi_baseurl_internal, cgiParameterVal)))
        else:
            # set external URLs
            for urlParameter, val in self.to_dict().items():
                if urlParameter.startswith('url'):
                    cgiParameter = 'cgi' + urlParameter[3:]
                    cgiParameterVal = getattr(self, cgiParameter, None)
                    setattr(self, urlParameter, urljoin(self.cgi_baseurl, cgiParameterVal))
                    if debug:
                        print('set URL config parameter {param} to {url}'.format(param=urlParameter, url=urljoin(self.cgi_baseurl, cgiParameterVal)))

    @property
    def debug(self):
        return self._debug
  
    @debug.setter
    def debug(self, value):
        self._debug = value

    @property
    def use_internal(self):
        return getattr(self, '_use_internal', None)
    
    @use_internal.setter
    def use_internal(self, value):
        self._use_internal = value
        
    @property
    def use_ssh(self):
        return getattr(self, '_use_ssh', None)
    
    @use_ssh.setter
    def use_ssh(self, value):
        self._use_ssh = value
        
    @classmethod
    def register_server(cls, config):
        cls.set(config.name, config)

    @classmethod
    def get(cls, name='__JSOC'):
        if len(name) == 0:
            return None

        try:
            return cls.__configs[name.lower()]
        except KeyError:
            raise SecureDRMSArgumentError('configuration ' + name + ' does not exist')

    @classmethod
    def set(cls, name, config):
        if isinstance(config, cls):
            cls.__configs[name.lower()] = config

            if isinstance(config, ServerConfig):
                # put this in the global used by the parent ServerConfig class too
                register_server(config)
        else:
            raise SecureDRMSArgumentError('config is of type ' + type(config) + '; must be of type ' + cls.__name__)

class OnTheFlyDownloader(object):
    '''
    abstract
    '''
    def __init__(self, secure_export_request, export_data, debug=False):
        self._secureExportRequest = secure_export_request
        self._exportData = export_data # DataFrame
        self._debug = debug

    @property
    def request_url(self):
        raise SecureDRMSMethodError('[ request_url ] no export SU directory is created for an on-the-fly export request')


class BasicAccessOnTheFlyDownloader(OnTheFlyDownloader):
    '''
    jsoc/file_list.json contents:      
    {
        "data" : 
        [
            {
                "record" : "hmi.Ic_720s[2017.01.14_00:00:00_TAI][3]{continuum}",
                "filename" : "hmi.ic_720s.20170114_000000_TAI.3.continuum.fits"
            }
        ],
        "status" : 0,
        "requestid" : null,
        "method" : "url_direct",
        "protocol" : "FITS",
        "dir" : null,
        "wait" : 0
    }
    '''
    def __init__(self, secure_export_request, url, debug=False):

        self._onTheFlyURL = url
        self._content = None

        # export_data is None at the moment, since we have yet to download the tar file yet (and the export data
        # are in the jsoc/file_list.json file inside the archive); the export data are in a (record, filename) DataFrame 
        # created from the 'data' attribute in the jsoc/file_list.json file
        super().__init__(secure_export_request, None, debug)
        
    def generate_download_url(self):
        # should return a DataFrame with 3 columns: record, filename, URL; in this case, we have only a single
        # tar file that encapsulates data from many records, so there should be 1 row, and the record column
        # should be None; the tar filename is generated in drms-export.py - so we don't know that either; the url
        # is the CGI url that causes a tar file to be streamed to stdout
        return pandas.DataFrame([ (None, None, self._onTheFlyURL) ], columns=[ 'record', 'filename', 'url' ])

    def download(self, *, out_dir, index=None, fname_from_rec=False, remove_tar=True, verbose=None):
        '''
        make remove_tar True by default since the idea of this kind of export is to stream data back to the 
        user; the user opens a URL and the files inside the tar file should appear in `out_dir`; each time the 
        user calls download() a new tar HTTP download occurs
        '''
        # open the URL 
        urldf = self.urls
        if urldf.shape[0] != 1:
            raise SecureDRMSArgumentError('[ BasicAccessOnTheFlyDownloader.download() ] unexpected number of rows in url DataFrame')

        url = urldf.at[0, 'url']
        
        if verbose is None:
            verbose = self._secureExportRequest.client.verbose

        try:
            if self._debug:
                print('[ BasicAccessOnTheFlyDownloader.download() ] opening URL ' + url)
                
            request = Request(url)

            if self._secureExportRequest.client._use_internal:
                # assume that 'external' (public) websites do not require any kind of authorization
                if self._debug:
                    print('[ BasicAccessOnTheFlyDownloader.download() ] adding Basic Access authorization header to {url}'.format(url=url))

                try:
                    # self._server is the SecureServerConfig that has the server authority information
                    passPhrase = self._secureExportRequest.client.json_client.server.cgi_baseurl_authority
                    request.add_header("Authorization", "Basic " + base64.b64encode(passPhrase.encode()).decode())
                except AttributeError:
                    # the user did not provide a passPhrase
                    if self._debug:
                        print('[ BasicAccessOnTheFlyDownloader.download() ] no Basic Access authority provided')

                    pass

            self._response = urlopen(request)
            
            # get filename HTTP header; the tarfile that drms-export.sh would like to create appears in the Content-Disposition header
            # (although at this point it is hard-coded as data.tar)
            # with response.info() as info:
            #   localTarfile = os.path.join(out_dir, os.path.basename(info.get_filename()))
        except URLError as exc:
            raise SecureDRMSUrlError('[ BasicAccessOnTheFlyDownloader.download() ] troubles opening URL ' + url)

        # the tarfile is actually in memory - use self.data and stick into a fileobj
        fileobj = io.BytesIO(self.raw_data) # must be a binary stream

        outPaths = []

        with tarfile.open(fileobj=fileobj, mode='r') as openArchive:
            # we need to fetch the FITS file names from the archived file jsoc/file_list.json
            
            with openArchive.extractfile('jsoc/file_list.json') as fin:
                jsonDict = load(fin)
                
                columns = [ 'record', 'filename' ]
                self._exportData = pandas.DataFrame(jsonDict['data'], columns=columns)

                if self._debug:
                    print('[ BasicAccessOnTheFlyDownloader.download() ] extracted (record, filename) export data from file_list.json inside streamed archive')
                
                # two columns, record and filename (inside the tar file)
                if index is not None:
                    dlData = self._exportData.iloc[index].copy()
                else:
                    dlData = self._exportData.copy()
        
            for irec in range(len(dlData)):
                row = dlData.iloc[irec]
                filename = None
                
                if fname_from_rec:
                    # if fname_from_rec is None or False, then use the filenames saved in the tar file; otherwise, generate names based upon the series and record prime-key values in the record name
                    filename = self._secureExportRequest._client._filename_from_export_record(row.record, old_fname=row.filename)

                if filename is None:
                    # use the filename in the tar file
                    filename = row.filename

                destFileUnique = self._secureExportRequest._next_available_filename(filename)
                destPathUnique = os.path.join(out_dir, destFileUnique)
                destPathUniqueTmp = os.path.join(out_dir, '.' + destFileUnique)

                if verbose:
                    print('Extracting file {filenum} of {total}...'.format(filenum=str(irec + 1), total=str(len(dlData))))
                    print('    record: ' + row.record)
                    print('  filename: ' + row.filename)

                try:
                    if self._debug:
                        print('[ download ] extracting file {file} to {dest}'.format(file=row['filename'], dest=destPathUnique))

                    with openArchive.extractfile(row['filename']) as fin, open(destPathUniqueTmp, 'wb') as fout:
                        while True:
                            dataBytes = fin.read(8192)
                            if dataBytes == b'':
                                break

                            bytesWritten = 0
                            while bytesWritten < len(dataBytes):
                                bytesWritten += fout.write(dataBytes)
    
                    # rename the temp file back to final destination (destPathUnique)
                    os.rename(destPathUniqueTmp, destPathUnique)
                except:
                    destPathUnique = None
                    
                    if verbose:
                        print('  -> Error: Could not extract file')
                    
                outPaths.append(destPathUnique)
        
        if remove_tar:
            # the tar is in memory - release it (or remove all references to the memory buffer for async garbage-collection)
            self._content = None

        # the tar file is in memory - no filename
        dlData['filename'] = None
        dlData['downloads'] = outPaths
            
        # return (record, tar_file_on_server, local_path)
        return dlData

    @property
    def raw_data(self):
        # binary data
        if self._content is None:
            self._content = self._response.read()

        return self._content
    
    @property
    def urls(self):
        # this is the CGI URL that causes the drms-export-to-stdout code to run and produce a tar containing FITS files;
        # ok, ready for this; this calls the parent urls() method, which calls SecureExportRequest._generate_download_urls()
        # which calls BasicAccessOnTheFlyDownloader.generate_download_url() which returns self._onTheFlyURL in essence
        return super(SecureExportRequest, self._secureExportRequest).urls


class SSHOnTheFlyDownloader(OnTheFlyDownloader):
    def __init__(self, secure_export_request, export_data, tarfile, remote_user, remote_host, remote_port, debug=False):
        self._tarfile = tarfile # absolute path on server
        self._remoteUser = remote_user
        self._remoteHost = remote_host
        self._remotePort = remote_port
        
        super().__init__(secure_export_request, export_data, debug)

    def generate_download_url(self):
        # in the parent, this creates one url per DRMS data-series record
        raise SecureDRMSMethodError('[ _generate_download_url ] cannot generate urls for an SSH configuration')
        
    def download(self, *, out_dir, index=None, fname_from_rec=False, remove_tar=False, verbose=None):
        # download tarfile to `directory`; self._tarfile is the server path to the tarfile (as seen from the server)
        localTarfile = os.path.join(out_dir, os.path.basename(self._tarfile))
        scpCmdList = [ '/usr/bin/scp', '-q', '-P', str(self._remotePort), self._remoteUser + '@' + self._remoteHost + ':' + self._tarfile, localTarfile ]

        try:
            if self._debug:
                print('running scp command: {cmd}'.format(cmd=' '.join(scpCmdList)))

            child = pexpect.spawn(' '.join(scpCmdList))
            passwordAttempted = False
            passwordFailed = False
            while True:
                choice = child.expect([ 'password:', pexpect.EOF ], timeout=1024) # big timeout for potential slow or large download
                if choice == 0:
                    if passwordAttempted:
                        if self._debug:
                            print('scp password failed; requesting user re-try')
                        passwordFailed = True
                    # user was prompted to enter password
                    password = self._secureExportRequest.client.json_client.getPassword(user_and_host=self._remoteUser + '@' + self._remoteHost, first_try=(not passwordFailed))
                    child.sendline(password.encode('UTF8'))
                    passwordAttempted = True
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
        
        # two columns, record and filename (inside the tar file)
        if index is not None:
            dlData = self._exportData.iloc[index].copy()
        else:
            dlData = self._exportData.copy()
            
        if verbose is None:
            verbose = self._secureExportRequest.client.verbose

        outPaths = []

        with tarfile.open(name=localTarfile, mode='r') as openArchive:
            for irec in range(len(dlData)):
                row = dlData.iloc[irec]
                filename = None
                
                if fname_from_rec:
                    # if fname_from_rec is None or False, then use the filenames saved in the tar file; otherwise, generate names based upon the series and record prime-key values in the record name
                    filename = self._secureExportRequest._client._filename_from_export_record(row.record, old_fname=row.filename)

                if filename is None:
                    # use the filename in the tar file
                    filename = row.filename

                destFileUnique = self._secureExportRequest._next_available_filename(filename)
                destPathUnique = os.path.join(out_dir, destFileUnique)
                destPathUniqueTmp = os.path.join(out_dir, '.' + destFileUnique)

                if verbose:
                    print('Extracting file {filenum} of {total}...'.format(filenum=str(irec + 1), total=str(len(dlData))))
                    print('    record: ' + row.record)
                    print('  filename: ' + row.filename)

                try:
                    if self._debug:
                        print('[ download ] extracting file {file} to {dest}'.format(file=row['filename'], dest=destPathUnique))

                    with openArchive.extractfile(row['filename']) as fin, open(destPathUniqueTmp, 'wb') as fout:
                            
                        while True:
                            dataBytes = fin.read(8192)
                            if dataBytes == b'':
                                break

                            bytesWritten = 0
                            while bytesWritten < len(dataBytes):
                                bytesWritten += fout.write(dataBytes)
    
                    # rename the temp file back to final destination (destPathUnique)
                    os.rename(destPathUniqueTmp, destPathUnique)
                except:
                    destPathUnique = None
                    
                    if verbose:
                        print('  -> Error: Could not extract file')
                    
                outPaths.append(destPathUnique)
        
        if remove_tar:
            # remove the tar, locally
            os.remove(localTarfile)
        
        # remote the tar, remotely
        cmds =[ '/bin/rm', self._tarfile ]
        sshCmdList = [ '/usr/bin/ssh', '-p', str(self._remotePort), self._remoteUser + '@' + self._remoteHost, shlex.quote('/bin/bash -c ' + shlex.quote(' '.join(cmds))) ]
        
        try:
            if self._debug:
                print('running ssh command: {cmd}'.format(cmd=' '.join(sshCmdList)))

            child = pexpect.spawn(' '.join(sshCmdList))
            passwordAttempted = False
            passwordFailed = False
            while True:
                choice = child.expect([ 'password:', pexpect.EOF ])
                if choice == 0:
                    if passwordAttempted:
                        if self._debug:
                            print('ssh password failed; requesting user re-try')
                        passwordFailed = True
                    # user was prompted to enter password
                    password = self._secureExportRequest._client._json.getPassword(user_and_host=self._remoteUser + '@' + self._remoteHost, first_try=(not passwordFailed))
                    child.sendline(password.encode('UTF8'))
                    passwordAttempted = True
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

        # we no longer need row.filename -> replace with localTarfile so this can be returned to user (in case
        # they want to examine the downloaded tar file)
        dlData['filename'] = localTarfile
        dlData['downloads'] = outPaths
            
        # return (record, tar_file_on_server, local_path)
        return dlData

    @property
    def urls(self):
        raise SecureDRMSMethodError('[ urls ] urls not relevant to an SSH configuration')


class SecureExportRequest(ExportRequest):
    '''
    instead of making yet another subclass (i.e., SSHExportRequest and BasicAccessExportRequest), for now we have just SecureExportRequest with a bunch of if statements to choose between the SSH and HTTP functionality
    a typical SSHClient server_response looks like this:
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
    
    ExportRequest._d contains this dict (which was made from the the JSON string returned by the server export script/binary)
    
    '''
    
    '''
    Constructor
    -----------
        server_response : dict (containing JSON key-values)
            `server_response` is 
        secure_client : SecureClient
            `secure_client` 

    '''
    def __init__(self, server_response, secure_client, remote_user=None, remote_host=None, remote_port=None, on_the_fly=True, debug=False):
        # so jsoc_export_make_index will create a 'tarfile' property in the response JSON returned by jsoc_fetch?op=exp_status
        # if the method is 'url-tar'; this is path into the export SU directory, which is not created if self._onTheFly is
        # True; self._tarfile is the SSH-server path of the on-the-fly tarfile created by drms-export-to-stdout if an SSHClient 
        # is being used AND self._onTheFly is True; drms-export-to-stdout over HTTP will not create file on the server - it will
        # stream the tarfile over HTTP when the user calls SecureExportRequest.download(); in this on-the-fly HTTP case, 
        # self._onTheFlyURL will contain the CGI URL that is used to execute drms-export-to-stdout remotely, and the url looks like:
        # 
        # http://jsoc.stanford.edu/cgi-bin/drms-export.sh?skiptar=true&spec=hmi.m_720s%5B2019.2.2%5D&filename=hmi.m_720s.%7BT_REC%3AA%7D.%7BCAMERA%7D.%7Bsegment%7D&compression=none&dbhost=hmidb2&webserver=jsoc.stanford.edu
        
        # initialize parent first - needed for self.data; `server_response` must be a json dict with a 'data' attribute
        super().__init__(server_response, secure_client)
                
        self._onTheFly = on_the_fly
        self._debug = debug
        if isinstance(secure_client, SSHClient):
            self._isssh = True 
        else:
            self._isssh = False

        if self._onTheFly:
            # self.data calls ExportRequest.data() - a (record, filename) DataFrame 
            if self._isssh:
                self._downloader = SSHOnTheFlyDownloader(self, self.data, server_response['tarfile'], remote_user, remote_host, remote_port, debug)                
            else:
                # ugh - at this point, we have no response data attribute (we need to stream the tarfile down first)
                self._downloader = BasicAccessOnTheFlyDownloader(self, server_response['url'], debug)
        else:
            self._downloader = None

    def _parse_export_recset(self, spec):
        parsed = self._client.parse_spec(spec)
        if parsed['nsubsets'] > 1:
            raise
            
        set = parsed['subsets'][0]
        sname = set['seriesname']
        pkeys = set['filter'].replace('[', ' ').replace(']', ' ').split()        
        segs = set['segments'].replace('{', ' ').replace('}', ' ').strip().split(',')
        
        return (sname, pkeys, segs)
        
    def _generate_download_urls(self):
        # in the parent, this creates one url per DRMS data-series record
        if self._downloader is None:
            # use the parent code by default
            return super()._generate_download_urls()
        else:
            # some kind of on-the-fly download (so there is one url, if any)
            return self._downloader.generate_download_url()
        
    def download(self, directory, index=None, fname_from_rec=None, remove_tar=False, verbose=None):
        if self._downloader is None:
            return super().download(directory, index, fname_from_rec, verbose)
            
        # on-the-fly
        outDir = os.path.abspath(directory)

        # make a list out of index        
        if numpy.isscalar(index):
            index = [ int(index) ]
        elif index is not None:
            index = list(index)

        if verbose is None:
            verbose = self._client.verbose

        return self._downloader.download(out_dir=outDir, index=index, fname_from_rec=fname_from_rec, remove_tar=remove_tar, verbose=verbose)
        
    def filename_from_export_record(self, rs, old_fname=None):
        return super()._filename_from_export_record(rs, old_fname)
        
    @property
    def client(self):
        return self._client

    @property
    def request_url(self):
        if self._downloader is None:
            return super().request_url

        return self._downloader().request_url
            
    @property
    def urls(self):
        if self._downloader is None:
            super().urls
    
        return self._downloader().urls


class BasicAccessHttpJsonRequest(HttpJsonRequest):
    '''
    a class to send web-server requests (HTTP URLs, with Basic Access authentication) to servers that return JSON to the DRMS client; for each request, the class processes the JSON response, decoding it into a dictionary which is then available to the calling JSON client
    
    Constructor
    -----------
        parameters
        ----------
        request : object (urllib.request.Request)
            abstraction of an HTTP URL request; BasicAccessHttpJsonRequest._http is a http.client.HTTPResponse
        encoding : str
            the name of one of the following JSON encodings: UTF-8, UTF-16, or UTF-32
            
        attributes
        ----------
    '''
    def __init__(self, request, encoding, debug=False):
        self._request = request
        self._encoding = encoding
        self._debug = debug
        self._http = sixUrlRequest.urlopen(request)
        self._data_str = None
        self._data = None
        # do not call parent's __init__() since that method calls urlopen without first making a Request; we need to make a Request so we can add the authentication header
        
    def __repr__(self):
        return '<BasicAccessHttpJsonRequest "{name}"'.format(name=self._request.full_url)


class BasicAccessHttpJsonClient(HttpJsonClient):
    '''
    a class that contains one method per web API call; for each web API, the class creates an appropriate BasicAccessHttpJsonRequest; it then collects the dictionary response from the BasicAccessHttpJsonRequest instance, processes it (if needed), and provides the result back to the BasicAccessClient
    
    Constructor Parameters
    ----------------------
    config : SecureServerConfig
        a secure server configuration
    debug : bool
        if true, print debugging statements
        
    Private Instance Methods
    ------------------------
    _json_request() : url:string -> BasicAccessHttpJsonRequest
        returns a JSON request appropriate for a web API (implemented as web-application URL `url`)
       
    Public Instance Methods
    -----------------------
    all methods are defined in the parent class HttpJsonClient
    '''
    def __init__(self, config, use_internal=False, debug=False):
        super().__init__(config.name, debug) # internally, self._debug, externally BasicAccessHttpJsonClient.debug
        self._use_ssh = False
        self._use_internal = use_internal
        
    def __repr__(self):
        return '<BasicAccessHttpJsonClient "{name}"'.format(name=self._server.name)

    def _json_request(self, url):
        if self.debug:
            print('[ BasicAccessHttpJsonClient ] JSON request {url}'.format(url=url))

        # we need to add the authority information
        request = sixUrlRequest.Request(url)

        if self._use_internal:
            # assume that 'external' (public) websites do not require any kind of authorization
            if self.debug:
                print('[ BasicAccessHttpJsonClient ] adding Basic Access authorization header to {url}'.format(url=url))

            try:
                # self._server is the SecureServerConfig that has the server authority information
                passPhrase = self._server.cgi_baseurl_authority
                request.add_header("Authorization", "Basic " + base64.b64encode(passPhrase.encode()).decode())
            except AttributeError:
                # the user did not provide a passPhrase
                pass

        return BasicAccessHttpJsonRequest(request, self._server.encoding, self._debug)
        
    def _show_series(self, *, ds_filter=None, info=False):
        # we have to intercept calls to both show_series parent methods, show_series() and show_series_wrapper(), and then do the
        # right thing depending on configuration parameters
        query = {}        

        if ds_filter is not None:
            query['filter'] = ds_filter
        
        if self._use_internal or self._server.cgi_show_series_wrapper is None or self._server.show_series_wrapper_dbhost is None:
            # do not use wrapper (use show_series)
            parsed = urlparse(self._server.url_show_series)
        else:
            # use wrapper (showextseries)
            query['dbhost'] = self._server.show_series_wrapper_dbhost
            
            if info:
                query['info'] = 1
                
            parsed = urlparse(self._server.url_show_series_wrapper)
            
        unparsed = urlunparse((parsed[0], parsed[1], parsed[2], None, urlencode(query), None))
        request = self._json_request(unparsed)
        return request.data

    def exp_fits(self, spec, filenamefmt=None):
        parsed = urlparse(self._server.url_export_fits)
        
        argStrUnencoded = { 'spec' : spec, 'compression' : 'none', 'skiptar' : 'false' }
        
        if filenamefmt is not None and len(filenamefmt) > 0:
            argStrUnencoded['filename'] = filenamefmt
        
        if self._use_internal:
            if hasattr(self._server, 'qcgi_export_fits_internal_args') and self._server.qcgi_export_fits_internal_args is not None:
                argStrUnencoded.update(self._server.qcgi_export_fits_internal_args)            
        else:        
            if hasattr(self._server, 'qcgi_export_fits_args') and self._server.qcgi_export_fits_args is not None:
                argStrUnencoded.update(self._server.qcgi_export_fits_args)

        unparsed = urlunparse((parsed[0], parsed[1], parsed[2], None, urlencode(argStrUnencoded), None))
        
        # do not call self._json_request() - we want to defer the actual download until the user calls download(); must
        # return a dict
        return { 'url' : unparsed, 'status' : 0 }
        
    def parse_recset(self, recset):
        '''
        parses a record-set specification into parts; no semantic checking is performed, only the syntax is verified
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
        '''
        parsed = urlparse(self._server.url_parse_recset)
        
        argStrUnencoded = { 'spec' : recset }
        unparsed = urlunparse((parsed[0], parsed[1], parsed[2], None, urlencode(argStrUnencoded), None))
        request = self._json_request(unparsed)
        return request.data
        
    def show_series(self, ds_filter=None):
        return self._show_series(ds_filter=ds_filter)
        
    def show_series_wrapper(self, ds_filter=None, info=False):
        return self._show_series(ds_filter=ds_filter, info=info)
        
    @property
    def use_internal(self):
        return self._use_internal
        
    @property
    def use_ssh(self):
        return self._use_ssh


class SSHJsonRequest(object):
    '''
    a class to send server requests (ssh commands) to servers that return JSON to the DRMS client; for each request, the class processes the JSON response, decoding it into a dictionary which is then available to the calling JSON client
    
    Constructor Parameters
    ----------------------
    cmdList : list
        a list of command-line arguments appropriate for a server API
    encoding : str
        the name of one of the following JSON encodings: UTF-8, UTF-16, or UTF-32
    remote_user : str
        the unix account the ssh command runs as
    remote_host : str
        the server accepting ssh requests
    remote_port : int
        the port of the server to which ssh requests are sent
    debug : bool
        if true, print debugging statements
        
    Class Methods
    -------------
    raw_data : None -> bytes
        returns the server response bytes (obtained by the __runCmd() method operating ont the cmdList); the bytes contain an encoded JSON string
    data : None -> dict
        converts the encoded JSON string server response into a Python dict
        
    Private Instance Methods
    ------------------------
    __getPassword() : None -> str
        interactively obtains the ssh password from the user
    
    __runCmd : None -> bytes
        executes the ssh command on the ssh server; returns a bytes object that represents an encoded JSON string
    '''
    def __init__(self, cmds, json_client, encoding, remote_user, remote_host, remote_port, debug=False):
        self._cmds = cmds
        self._jsonClient = json_client
        self._encoding = encoding
        self._remoteUser = remote_user
        self._remoteHost = remote_host
        self._remotePort = remote_port
        self._debug = debug
        self._data_str = None
        self._data = None
        
    def __repr__(self):
        return '<SSHJsonRequest "{name}"'.format(name=' '.join(self._cmds))
                
    def __runCmd(self):
        try:            
            # sshCmdList = [ '/usr/bin/ssh', '-p', str(self._remotePort), self._remoteUser + '@' + self._remoteHost, shlex.quote('/bin/bash -c ' + shlex.quote(' '.join(self._cmdList))) ]
            
            sshCmdList = [ '/usr/bin/ssh', '-p', str(self._remotePort), self._remoteUser + '@' + self._remoteHost, shlex.quote('/bin/bash -c ' + shlex.quote(' '.join(self._cmds))) ]
            
            if self._debug:
                print('running ssh command: {cmd}'.format(cmd=' '.join(sshCmdList)))

            child = pexpect.spawn(' '.join(sshCmdList))
            passwordAttempted = False
            passwordFailed = False
            while True:
                index = child.expect([ 'password:', pexpect.EOF ])
                if index == 0:
                    if passwordAttempted:
                        if self._debug:
                            print('ssh password failed; requesting user re-try')
                        passwordFailed = True
                    # user was prompted to enter password
                    password = self._jsonClient.getPassword(user_and_host=self._remoteUser + '@' + self._remoteHost, first_try=(not passwordFailed))
                    child.sendline(password.encode('UTF8'))
                    passwordAttempted = True
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
    def raw_data(self):
        if self._data_str is None:
            self._data_str = self.__runCmd()
        return self._data_str

    @property
    def data(self):
        if self._data is None:
            # assign dictionary to self._data
            jsonStr = self.raw_data.decode(self._encoding)
            
            if self._debug:
                print('json response: {json}'.format(json=jsonStr))
            try:
                self._data = loads(jsonStr)
            except decoder.JSONDecodeError:
                raise SecureDRMSResponseError('invalid json response: ' + jsonStr)
                
        # returns a dict
        return self._data


class SSHJsonClient(object):
    '''
    a class that contains one method per server ssh API call; for each API, the class creates an appropriate SSHJsonRequest; it then collects the dictionary response from the SSHJsonRequest instance, processes it (if needed), and provides the result back to the SSHClient
    
    Constructor Parameters
    ----------------------
    config : SecureServerConfig
        a secure server configuration
    [ use_internal : bool ]
        If True, then access will be to an internal server, otherwise access will be to an external server (external DRMS data series plus selectively exposed internal data series)
    [ debug : bool ]
        if true, print debugging statements
        
    Private Attributes
    ------------------
    _server : object (SecureServerConfig)
        The server configuration contains parameter values needed to create SSHJsonRequests.
    _use_ssh : True
        If True, then this instance accesses the remove server via SSH
    _use_internal : bool
        If True, then access will be to an internal server, otherwise access will be to an external server (external DRMS data series plus selectively exposed internal data series)
    _debug : bool
        If True, debug statements are printed.
    _password : str
        The SSH password entered by the user is cached.
    _password_timer : object (threading.Timer)
        The cached user password times-out after an interval of time.
        
    Public Attributes
    -----------------
    None
        
    Private Instance Methods
    ------------------------
    __clearPassword  None -> None
    _json_request : cmdList:list -> object (SSHJsonRequest)
        This method generates a set of bash commands to set the runtime environment. To this set of commands, the method appends the SSH-interface bash commands contained in `cmdList`. The method then creates and returns an SSHJsonRequest instance that encapsulates these commands.

        
    Public Instance Methods
    -----------------------
    clearTimer : None -> None
        If a timer is currently running, then this method clears the timer.
    show_series : ds_filter:str -> object (json)
        This method executes the show_series command on the server; `ds_filter` is a POSIX Extended Regular Expression that filters-in the series that are returned in the returned list object
    show_series_wrapper() : ds_filter:str, info:bool -> object 
        executes the showintseries.py command on the server; `ds_filter` is a POSIX Extended Regular Expression that filters-in the series that are returned in the object; if `info` is True, then a description of each series is included in the returned pandas.DataFrame object
    '''
    def __init__(self, *, config, use_internal=False, debug=False):
        self._server = config
        self._use_ssh = True
        self._use_internal = use_internal
        self._debug = debug
        self._password = None
        self._password_timer = None
        
    def __repr__(self):
        return '<SSHJsonClient "{name}"'.format(name=self._server.name)        
        
    def __clearPassword(self):
        if self._debug:
            print('[ SSHJsonClient.__clearPassword() ] clearing password')

        self._password_timer = None
        self._password = None           

    def _json_request(self, *, cmd_list):
        # prepend the cmd_list with the env var settings
        envCmds = [ cmd + ';' for cmd in self._server.ssh_remote_env.bash_cmd() ]
        cmds = envCmds + [ ' '.join(cmd_list) ]
        
        if self._debug:
            print('[ SSHJsonClient ] JSON request {cmd}'.format(cmd=' '.join(cmd_list)))

        return SSHJsonRequest(cmds, self, self._server.encoding, self._server.ssh_remote_user, self._server.ssh_remote_host, self._server.ssh_remote_port, self._debug)
        
    def __runCmd(self, *, cmd_list):
        request = self._json_request(cmd_list=cmd_list)

        # runs the ssh command
        response = request.data
        return response

    def _show_series(self, *, ds_filter=None, info=False):
        # we have to intercept calls to both show_series parent methods, show_series() and show_series_wrapper(), and then do the
        # right thing depending on configuration parameters
        if self._use_internal or self._server.ssh_show_series_wrapper is None:
            # binary executable
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_show_series), '-qz' ]
            
            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')
            
            if self._use_internal:
                if self._server.ssh_show_series_internal_args is not None:
                    cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_show_series_internal_args.items() ])
            else:
                if self._server.ssh_show_series_args is not None:
                    cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_show_series_args.items() ])

            if ds_filter is not None:
                cmdList.append(ds_filter)
        else:
            # script
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_show_series_wrapper), '--json' ]
            
            if self._server.ssh_show_series_wrapper_args is not None:
                cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_show_series_wrapper_args.items() ])
            
            if ds_filter is not None:
                cmdList.append('--filter=' + ds_filter)

            if info:
                cmdList.append('--info' )

        return self.__runCmd(cmd_list=cmdList)
        
    # public methods
    def clearTimer(self):
        if self._debug:
            print('clearing timer for json client {client}'.format(client=repr(self)))

        if self._password_timer is not None:
            self._password_timer.cancel()
            
        if self._debug:
            print('cleared\n')
            
    def getPassword(self, *, user_and_host, first_try=True):
        if not first_try:
            # this implies the existing password is bad
            self._password = None
            self._password_timer.cancel()
            print('permission denied, please re-enter a password for {address}'.format(address=user_and_host))
            self._password = getpass.getpass()
        else:
            if self._password is not None:
                if self._debug:
                    print('[ SSHJsonClient._json_request ] renewing password' )

                self._password_timer.cancel()
            else:
                print('please enter a password for {address}'.format(address=user_and_host))

                if self._debug:
                    print('[ SSHJsonClient._json_request ] storing password' )

                self._password = getpass.getpass()

        self._password_timer = threading.Timer(PASSWORD_TIMEOUT, self.__clearPassword)
        # IMPORTANT! making the timer threads daemons allows the interpreter to terminate in response to EOF (ctrl-d); otherwise, 
        # the main thread blocks on a join() on the timer thread until the timer 'fires' (and calls self.__clearPassword()); normally
        # this isn't so cool, but there does not seem to be a way for this module to 'intercept' an EOF sent to the interpreter
        # interactively
        self._password_timer.daemon = True
        
        if self._debug:
            print('[ SSHJsonClient._json_request ] starting password timer' )

        self._password_timer.start()
        
        return self._password 
        
    def check_address(self, address):
        # external calls only (checkAddress.py)
        if address is None or not isinstance(address, str):
            raise SecureDRMSArgumentError('[ check_address ] missing or invalid argument email')
        
        # this check_email script accepts a URL argument string (like QUERY_STRING)
        argStr = urlencode({ 'address' : address, 'addresstab' : self._server.ssh_check_email_addresstab, 'checkonly' : 1, 'domaintab' : self._server.ssh_check_email_domaintab })
        cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_check_email), shlex.quote(argStr) ]
        return self.__runCmd(cmd_list=cmdList)
        
    def exp_fits(self, spec, filenamefmt=None):        
        cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_export_fits), 'a=0', 's=0' ]

        cmdList.append(shlex.quote('spec=' + spec))
        
        if filenamefmt is not None and len(filenamefmt) > 0:
            cmdList.append(shlex.quote('ffmt=' + filenamefmt))
        
        if self._server.encoding.lower() == 'utf8':
            cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')
        
        if self._use_internal:
            if self._server.ssh_export_fits_internal_args is not None:
                cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_export_fits_internal_args.items() ])
        else:
            if self._server.ssh_export_fits_args is not None:
                cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_export_fits_args.items() ])
                
        # we have to redirect the output to a tar file; use a UUID-inspired base file name; when the user calls
        # SecureExportRequest.download(dir), this file is scp'd back to the dir directory on the client host and exploded
        tarFile = os.path.join(self._server.server_tmp, '.' + str(uuid.uuid4()) + '.tar')
        cmdList.append('>' + tarFile)
        
        # now, the JSON response is actually a file named jsoc/file_list.json inside the tar file; must extract that and
        # print to sdtout (which is that the 'O' flag to tar does)
        cmdList.append(';')
        cmdList.extend([ '/bin/tar', 'xOf', tarFile, 'jsoc/file_list.json' ])
        
        # ART - we want to the the guid file name in the request, then send that request back to SSHClient.export_fits so
        # it can put the file name into the SecureExportRequest, do SecureExportRequest.download() knows the name of the 
        # tar file to explode
        
        response = self.__runCmd(cmd_list=cmdList)
        
        # add the server tar-file name; response is a dict (made from the JSON-string server response)
        response['tarfile'] = tarFile
            
        return response
        
    def exp_request(self, ds, notify, method='url_quick', protocol='as-is', protocol_args=None, filenamefmt=None, n=None, requestor=None):
        # both external (jsocextfetch.py) and internal (jsoc_fetch) calls
        method = method.lower()
        method_list = ['url_quick', 'url', 'url-tar', 'ftp', 'ftp-tar']
        if method not in method_list:
            raise ValueError("Method '%s' is not supported, valid methods are: %s" % (method, ', '.join("'%s'" % s for s in method_list)))

        protocol = protocol.lower()
        img_protocol_list = ['jpg', 'mpg', 'mp4']
        protocol_list = ['as-is', 'fits'] + img_protocol_list
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
            
        if self._use_internal:
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_fetch_internal), '-W', 'op=exp_request', 'format=json', shlex.quote('ds=' + ds), 'notify=' + notify, 'method=' + method, 'protocol=' + protocol ]

            if filenamefmt is not None:
                cmdList.append(shlex.quote('filenamefmt=' + filenamefmt))
            if n is not None:
                cmdList.append('process=n=' + str(n))
            if requestor is None:
                cmdList.append('requestor=' + notify.split('@')[0])
            elif requestor is not False:
                cmdList.append('requestor=' + requestor)

            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._server.ssh_jsoc_fetch_internal_args is not None:
                cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_jsoc_fetch_internal_args.items() ])            
        else:
            # this script accepts a URL argument string (like QUERY_STRING)
            argStrUnencoded = { 'op' : 'exp_request', 'format' : 'json', 'ds' : ds, 'notify' : notify, 'method' : method, 'protocol' : protocol }

            if self._server.ssh_jsoc_fetch_args is not None:            
                argStrUnencoded.update(self._server.ssh_jsoc_fetch_args)
                
            if filenamefmt is not None:
                argStrUnencoded.update({ 'filenamefmt' : filenamefmt })
            if n is not None:
                argStrUnencoded.update({ 'process=n=' : str(n) })
            if requestor is None:
                argStrUnencoded.update({ 'requestor=' : notify.split('@')[0] })
            elif requestor is not False:
                argStrUnencoded.update({ 'requestor=' : requestor })

            argStr = urlencode(argStrUnencoded)
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_fetch), shlex.quote(argStr) ]

        return self.__runCmd(cmd_list=cmdList)
        
    def exp_status(self, requestid):
        # both external (jsocextfetch.py) and internal (jsoc_fetch) calls
        if requestid is None or not isinstance(requestid, str):
            raise SecureDRMSArgumentError('[ exp_status ] missing or invalid argument requestid')
            
        if self._use_internal:
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_fetch_internal), '-W', 'op=exp_status', 'requestid=' + requestid ]

            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._server.ssh_jsoc_fetch_internal_args is not None:
                cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_jsoc_fetch_internal_args.items() ])
        else:
            # this script accepts a URL argument string (like QUERY_STRING)
            argStrUnencoded = { 'op' : 'exp_status', 'requestid' : requestid }
            
            if self._server.ssh_jsoc_fetch_args is not None:            
                argStrUnencoded.update(self._server.ssh_jsoc_fetch_args)

            argStr = urlencode(argStrUnencoded)
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_fetch), shlex.quote(argStr) ]
        
        return self.__runCmd(cmd_list=cmdList)

    def parse_recset(self, recset):
        '''
        parses a record-set specification into parts; no semantic checking is performed, only the syntax is verified
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
        '''

        # external call only (drms_parserecset)
        if recset is None or not isinstance(recset, str):
            raise SecureDRMSArgumentError('[ parse_recset ] missing or invalid argument recset')

        cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_parse_recset), shlex.quote('spec=' + recset) ]
        if self._server.encoding.lower() == 'utf8':
            cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')

        return self.__runCmd(cmd_list=cmdList)
        
    def rs_list(self, ds, key=None, seg=None, link=None, recinfo=False, n=None, uid=None):
        # both external (jsocextinfo.py) and internal (jsoc_info) calls

        if self._use_internal:
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_info_internal), '-s', 'op=rs_list', shlex.quote('ds=' + ds) ]

            if key is not None:
                cmdList.append('key=' + ','.join(_split_arg(key)))
            if seg is not None:
                cmdList.append('seg=' + ','.join(_split_arg(seg)))
            if link is not None:
                cmdList.append('link=' + ','.join(_split_arg(link)))
            if recinfo:
                cmdList.append('-R')
            if n is not None:
                cmdList.append('n=' + str(n))
            if uid is not None:
                cmdList.append('userhandle=' + uid)

            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._server.ssh_jsoc_info_internal_args is not None:
                cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_jsoc_info_internal_args.items() ])
        else:
            # this script accepts a URL argument string (like QUERY_STRING)
            argStrUnencoded = { 'op' : 'rs_list', 'ds' : ds }

            if key is not None:
                argStrUnencoded.update({ 'key' : ','.join(_split_arg(key)) })
            if seg is not None:
                argStrUnencoded.update({ 'seg' : ','.join(_split_arg(seg)) })
            if link is not None:
                argStrUnencoded.update({ 'link' : ','.join(_split_arg(link)) })
            if recinfo:
                argStrUnencoded.update({ 'R' : 1 })
            if n is not None:
                argStrUnencoded.update({ 'n=' : str(n) })
            if uid is not None:
                argStrUnencoded.update({ 'userhandle' : uid })
                
            if self._server.ssh_jsoc_info_args is not None:            
                argStrUnencoded.update(self._server.ssh_jsoc_info_args)
                
            argStr = urlencode(argStrUnencoded)
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_info), shlex.quote(argStr) ]
            
        return self.__runCmd(cmd_list=cmdList)
            
    def rs_summary(self, ds):
        # both external (jsocextinfo.py) and internal (jsoc_info) calls
        if self._use_internal:
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_info_internal), '-s', 'op=rs_summary', shlex.quote('ds=' + ds) ]
        
            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')
                
            if self._server.ssh_jsoc_info_internal_args is not None:
                cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_jsoc_info_internal_args.items() ])
        else:
            argStrUnencoded = { 'op' : 'rs_summary', 'ds' : ds }

            if self._server.ssh_jsoc_info_args is not None:            
                argStrUnencoded.update(self._server.ssh_jsoc_info_args)

            argStr = urlencode(argStrUnencoded)
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_info), shlex.quote(argStr) ]

        return self.__runCmd(cmd_list=cmdList)
    
    def series_struct(self, series):
        # both external (jsocextinfo.py) and internal (jsoc_info) calls
        if series is None or not isinstance(series, str):
            raise SecureDRMSArgumentError('[ series_struct ] missing or invalid argument series')

        if self._use_internal:
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_info_internal), '-s', 'op=series_struct', 'ds=' +  series ]

            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')            

            if self._server.ssh_jsoc_info_internal_args is not None:
                cmdList.extend([ key + '=' + str(val) for key, val in self._server.ssh_jsoc_info_internal_args.items() ])
        else:
            argStrUnencoded = { 'op' : 'series_struct', 'ds' : series }

            if self._server.ssh_jsoc_info_args is not None:
                argStrUnencoded.update(self._server.ssh_jsoc_info_args)

            argStr = urlencode(argStrUnencoded)
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_info), shlex.quote(argStr) ]

        
        return self.__runCmd(cmd_list=cmdList)
    
    def show_series(self, ds_filter=None):
        return self._show_series(ds_filter=ds_filter)
        
    def show_series_wrapper(self, ds_filter=None, info=False):
        return self._show_series(ds_filter=ds_filter, info=info)
       
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
    def config(self):
        return self._server

    @property
    def use_internal(self):
        return self._use_internal

    @property
    def use_ssh(self):
        return self._use_ssh
    

class SecureClient(DRMSClient):
    '''
    a class that provides secure access to DRMS API methods
    
    Constructor Parameters
    ----------------------
    server : str
        the name of a server configuration
    email : str
        registered email address used when making an export request; used for the export() method if no email is provided to that method
    verbose : bool
        if True, print export status output
    debug : bool
        if true, print debugging statements
        
    Private Instance Methods
    ------------------------
    all methods are defined in the parent class DRMSClient
        
    Public Instance Methods
    -----------------------
    all methods are defined in the parent class DRMSClient
    '''
    __lifo = LifoQueue()
    
    def __init__(self, config, use_internal=False, email=None, synchronous_export=True, verbose=False, debug=False):
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
        
    def __execute(self, apimethod, **args):
        resp = None

        try:
            if 'api_name' in args:
                apiName = args['api_name']
                del args['api_name'] # API method might not handle this extraneous argument
            else:
                apiName = cast(types.FrameType, inspect.currentframe()).f_back.f_code.co_name

            if not self._server.check_supported(apiName, isinstance(self, SSHClient), self._use_internal):
                raise DrmsOperationNotSupported('Server does not support {api_name} access'.format(api_name=apiName))
        
            # set urls if this is an HTTP client
            if isinstance(self, BasicAccessClient):
                self._config.set_urls(self._use_internal, self._debug)
            
            # pass use_ssh and use_internal to SecureServerConfig.check_supported(), which will might be called by parent 
            # method call
            
            # save old values
            self.__lifo.put((self.config.use_internal, self.config.use_ssh))
            
            # set temporary values (for passing to config)
            self.config.use_internal = self._use_internal
            self.config.use_ssh = isinstance(self, SSHClient)
            
            if self._debug:
                print('[ SecureClient.__execute() ] Executing DRMSClient API method "{method}"'.format(method=apiName))
            
            # this could call __execute() recursively, so a lower-level call could delete attributes, and then if we 
            # try again after the api method call, we'll get an attribute error
            resp = apimethod(**args)
            
            useInternal, useSsh = self.__lifo.get()

            self.config.use_internal = useInternal            
            self.config.use_ssh = useSsh
        except SecureDRMSError as exc:
            print(exc.msg, file=sys.stderr)

            import traceback
            print(traceback.format_exc(), file=sys.stderr)
            
        return resp
        
    def _extract_series_name(self, ds):
        parsed = self.parse_spec(ds)
        return parsed['subsets'][0]['seriesname'].lower()
        
    def _parse_spec(self, spec):
        return self._json.parse_recset(spec)

    # Public Interface
    def check_email(self, address):
        args = { 'email' : address }
        
        # call the parent's check_email() method
        return self.__execute(super().check_email, **args)
        
    def export(self, ds, method='url_quick', protocol='as-is', protocol_args=None, filenamefmt=None, n=None, email=None, requestor=None, synchronous_export=None):
        # n argument is invalid (currently) for drms-export-to-stdout, and it doesn't really make any sense for jsoc_fetch either
        if filenamefmt is None:
            # override parent implementation
            series = self._extract_series_name(ds)
            filenamefmt = self._generate_filenamefmt(series)
            
        if synchronous_export is None:
            synchronous_export = self._synchronous_export

        # new feature - if the user is requesting FITS files via url_quick/FITS, then skip the export system altogether
        # and use drms-export-to-stdout; this module combines the image with the metadata into FITS files, wrapped in a 
        # tar file, on-the-fly; a true export is avoided - drms-export-to-stdout reads the image files (bringing them
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
                args = { 'api_name' : 'export_fits', 'spec' : ds, 'filenamefmt' : filenamefmt }

                # returns a SecureExportRequest
                return self.__execute(self._export_fits, **args)
            except SecureDRMSArgumentError as exc:
                if self._debug:
                    print(exc.args[0])
                pass
            except:
                # some problem calling self._export_fits, propagate to a handler
                raise
            
        args = { 'ds' : ds, 'method' : method, 'protocol' : protocol, 'protocol_args' : protocol_args, 'filenamefmt' : filenamefmt, 'n' : n, 'email' : email, 'requestor' : requestor }

        # call the parent's export() method
        if self._debug:
            print('[ SecureClient.export() ] calling parent export() with args ' + dumps(args))
        
        return self.__execute(super().export, **args)
        
    def export_fits(self, spec, filenamefmt=None):
        args = { 'api_name' : 'export_fits', 'spec' : spec, 'filenamefmt' : filenamefmt }

        # returns a SecureExportRequest
        return self.__execute(self._export_fits, **args)
    
    def export_from_id(self, requestid):
        args = { 'requestid' : requestid }

        # call the parent's export_from_id() method
        return self.__execute(super().export_from_id, **args)
        
    def info(self, series):
        args = { 'ds' : series }
        
        # override utils._extract_series_name
        saved = utils._extract_series_name
        utils._extract_series_name = self._extract_series_name

        response = self.__execute(super().info, **args)

        # restore
        utils._extract_series_name = saved

        return response
        
    def keys(self, series):
        args = { 'ds' : series }
        
        # call the parent's keys() method
        return self.__execute(super().keys, **args)
        
    def parse_spec(self, spec):
        args = { 'spec' : spec }
        
        return self.__execute(self._parse_spec, **args)
        
    def pkeys(self, series):
        args = { 'ds' : series }
        
        # call the parent's pkeys() method
        return self.__execute(super().pkeys, **args)
        
    def query(self, ds, key=None, seg=None, link=None, convert_numeric=True, skip_conversion=None, pkeys=False, rec_index=False, n=None):
        args = { 'ds' : ds, 'key' : key, 'seg' : seg, 'link' : link, 'convert_numeric' : convert_numeric, 'skip_conversion' : skip_conversion, 'pkeys' : pkeys, 'rec_index' : rec_index, 'n' : n }
        
        # call the parent's query() method
        return self.__execute(super().query, **args)
            
    def series(self, regex=None, full=False):
        args = { 'api_name' : 'series', 'regex' : regex, 'full' : full }
        
        # call the parent's series() method [ the server configuration parameters in the parent method will be ignored; they will be used,
        # however, in the self._json.show_series*() methods ]
        return self.__execute(self._series, **args)
        
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


class BasicAccessClient(SecureClient):
    '''
    a class that provides secure HTTP access (Basic Access authentication) to DRMS API methods
    
    Constructor Parameters
    ----------------------
    server : str
        the name of a server configuration
    email : str
        registered email address used when making an export request
    verbose : bool
        if True, print export status output
    debug : bool
        if true, print debugging statements
        
    Private Instance Methods
    ------------------------
    all methods are defined in the parent class DRMSClient
        
    Public Instance Methods
    -----------------------
    all methods are defined in the parent class DRMSClient

    '''
    def __init__(self, config, use_internal=False, email=None, synchronous_export=True, verbose=False, debug=False):
        self._json = BasicAccessHttpJsonClient(config=config, use_internal=use_internal, debug=debug)
        super().__init__(config, use_internal=use_internal, email=email, synchronous_export=synchronous_export, verbose=verbose, debug=debug)
        
    def __repr__(self):
        return '<BasicAccessClient "{name}">'.format(name=self._config.name)
        
    def _export_fits(self, spec, filenamefmt=None):
        # we are going to run drms-export-to-stdout on the server
        resp = self._json.exp_fits(spec, filenamefmt)

        return SecureExportRequest(resp, self, remote_user=None, remote_host=None, remote_port=None, on_the_fly=True, debug=self.debug)
        
    def _series(self, regex=None, full=False):
        if self._use_internal or self._server.cgi_show_series_wrapper is None:
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

    Constructor Parameters
    ----------------------
    server : str
        the name of a server configuration
    email : str
        registered email address used when making an export request
    verbose : bool
        if True, print export status output
    debug : bool
        if true, print debugging statements

    Private Instance Methods
    ------------------------
    __execute() : method:function, args:dict -> object
        executes a DRMS API method `method` with arguments `args` within an exception handler context; returns the object returned by the API method


    Public Instance Methods
    -----------------------
    check_email() : email:str -> bool
        returns True if `email` is valid and registered for use with the export system: 
    
        >>> sshclient.check_email('arta@sun.stanford.edu')
        True
        >>>
    
    export() : ds:str, [ method:str ], [ protocol:str ], [ protocol_args:dict ], [ filenamefmt:str/bool/None ], [ n:int/None ], [ email:str/None ], [ requestor:str/bool/None ] -> object (ExportRequest)
        returns an ExportRequest object
        
        

    export_from_id() : requestid:str -> object (ExportRequest)
 
    info() : series:str -> object (SeriesInfo)
    
    keys() : series:str -> list
    
    pkeys() : series:str -> list
    
    query() : ds:str, [ key:str/list/None ], [ seg:str/list/None ], [ link:str/list/None ], [ convert_numeric:bool ], [ skip_conversion:list/None ], [ pkeys:bool ], [ rec_index:bool ], [ n:int/None ] -> pandas.DataFrame/tuple 
        returns keyword, segment, and link information for a record set:
        
        >>> response = sshclient.query('hmi.b_720s[2014.7.8/60m]', key='T_REC', seg='inclination')
        >>> response[0]
                             T_REC
        0  2014.07.08_00:00:00_TAI
        1  2014.07.08_00:12:00_TAI
        2  2014.07.08_00:24:00_TAI
        3  2014.07.08_00:36:00_TAI
        4  2014.07.08_00:48:00_TAI
        >>> response[1]
                                         inclination
        0   /SUM1/D586204465/S00000/inclination.fits
        1  /SUM59/D586214018/S00000/inclination.fits
        2  /SUM18/D586223071/S00000/inclination.fits
        3  /SUM61/D586230553/S00000/inclination.fits
        4  /SUM36/D586248087/S00000/inclination.fits
        >>>
        
        `str` is a record-set specification, `key` is the name of a keyword, or a list of keyword names, `seg` is the name of a segment, or a list of segment names, and `link` is the name of a link, or a list of link names; with these three arguments the caller specifies a set of keywords, segments, and links for which DRMS-record-specific information is to be printed; if `convert_numeric` is True, then numeric-datatype keywords not in the list `skip_conversion` are converted from string to numbers; if `pkeys` is True, then the set of keys specified by `key` is augmented with the prime-key keywords; if `rec_index` is True, then the rows in the resulting pandas.DataFrame are no longer indexed by number 

    
    series() : [ regex:str/None ], [ full:bool ] -> list/object
        returns a list of series names, optionally filtered by `regex`, a POSIX Extended Regular Expression; if `regex` is not None or is ommitted, then all series whose names match `regex` are included in the returned list; if `full` is True, then returns a pandas data frame that contains a list of series and series descriptions; by default a list of series without descriptions is returned
    '''
    def __init__(self, config, use_internal=False, email=None, synchronous_export=True, verbose=False, debug=False):
        self._json = SSHJsonClient(config=config, use_internal=use_internal, debug=debug)
        super().__init__(config, use_internal=use_internal, email=email, synchronous_export=synchronous_export, verbose=verbose, debug=debug)
        
    def __repr__(self):
        return '<SSHClient "{name}">'.format(name=self._config.name)

    # private methods
    def _export_fits(self, spec, filenamefmt):
        # we are going to run drms-export-to-stdout on the server
        resp = self._json.exp_fits(spec, filenamefmt)

        return SecureExportRequest(resp, self, remote_user=self._config.ssh_remote_user, remote_host=self._config.ssh_remote_host, remote_port=self._config.ssh_remote_port, on_the_fly=True, debug=self.debug)

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
    def clearTimer(self):
        self._json.clearTimer()


class SecureClientFactory(object):
    '''
    a class to make a SecureClient factory
    
    Constructor Parameters
    ----------------------
    server : str
        name of the secure server configuration for clients to use
    email : str
        registered email address used when making an export request
    verbose : bool
        if True, print export status output
    debug : bool
        if true, print debugging statements
        
    Public Instance Methods
    -----------------------
    create_client() : use_ssh:bool -> SecureClient
        returns an SSHClient if `use_ssh` is True, otherwise returns a BasicAccessClient
    '''

    __clients = []
    
    def __init__(self, *, server='__JSOC', email=None, verbose=False, debug=False):
        self._config = SecureServerConfig.get(server)
        self._config.debug = debug
        self._email = email
        self._verbose = verbose
        self._debug = debug
        
    def create_client(self, *, use_ssh=False, use_internal=False, debug=None):
        client = None
        args = { 'email' : self._email, 'verbose' : self._verbose, 'debug' : self._debug, 'use_internal' : use_internal }

        if use_ssh:
            client = SSHClient(self._config, **args)
            
            # add to list of clients whose timers might need to be canceled upon termination
            SecureClientFactory.__clients.append(client)
        elif False:
            # if we need additional clients, like https, make a new elsif statement immediately above this one
            pass
        else:
            # default to basic access
            client = BasicAccessClient(self._config, **self._args)

        return client

    @classmethod
    def terminator(cls, *args):
        for client in cls.__clients:
            client.clearTimer()

# intercept ctrl-c and kill the child timer threads
signal.signal(signal.SIGINT, SecureClientFactory.terminator)
signal.signal(signal.SIGTERM, SecureClientFactory.terminator)
signal.signal(signal.SIGHUP, SecureClientFactory.terminator)

# register secure JSOC DRMS server
SecureServerConfig.register_server(SecureServerConfig(
    name='__JSOC',
    cgi_baseurl='http://jsoc.stanford.edu/cgi-bin/ajax/',
    cgi_baseurl_internal='http://jsoc2.stanford.edu/cgi-bin/ajax/',
    cgi_baseurl_authority='hmiteam:hmiteam',
#   cgi_baseurl_authorityfile='/Users/art/HEPL/drmsPy/auth.py',
    cgi_check_address='checkAddress.sh',
    cgi_export_fits='drms-export.sh',
    cgi_jsoc_info='jsoc_info',
    cgi_jsoc_fetch='jsoc_fetch',
    cgi_parse_recset='drms_parserecset',    
    cgi_show_series='show_series',
    cgi_show_series_wrapper='showextseries',
    qcgi_export_fits_args=
    {
        'dbhost' : 'hmidb2',
        'webserver' : 'jsoc.stanford.edu'
    },
    qcgi_export_fits_internal_args=
    {
        'dbhost' : 'hmidb',
        'webserver' : 'jsoc2.stanford.edu'
    },
    has_full_export_system=False,
    server_tmp='/tmp',
    show_series_wrapper_dbhost='hmidb2',
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
    ssh_jsoc_info='jsocextinfo.py',
    ssh_jsoc_info_args=
    {
        'dbhost' : 'hmidb2',
        'N' : 1
    },
    ssh_jsoc_info_internal='jsoc_info',
    ssh_jsoc_info_internal_args=
    {
        'JSOC_DBHOST' : 'hmidb'
    },
    ssh_jsoc_fetch='jsocextfetch.py',
    ssh_jsoc_fetch_args=
    {
        'dbhost' : 'hmidb2',
        'JSOC_DBUSER' : 'production',
        'n' : 1
    },
    ssh_jsoc_fetch_internal='jsoc_fetch',
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
        'JSOC_DBHOST' : 'hmidb2'
    },
    ssh_show_series_internal_args=
    {
        'JSOC_DBHOST' : 'hmidb'
    },
    ssh_show_series_wrapper='showextseries.py',
    ssh_show_series_wrapper_args=
    {
        'dbhost' : 'hmidb2', 
        '--wlfile' : '/home/jsoc/cvs/Development/JSOC/proj/export/webapps/whitelist.txt'
    },
    http_download_baseurl='http://jsoc.stanford.edu/',
    ftp_download_baseurl='ftp://pail.stanford.edu/export/'))
