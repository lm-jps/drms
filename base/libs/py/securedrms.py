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
import atexit
import base64
import getpass
import importlib
import inspect
from json import loads, decoder
import os
import pexpect
import re
import shlex
import signal
import sys
import threading
import types
from typing import cast
from urllib.parse import urlencode, urlparse, urlunparse, urljoin

# third party imports
import pandas
from six.moves.urllib import request as sixUrlRequest

# local imports
import drms
from drms import utils
from drms.client import Client as DRMSClient, SeriesInfo, ExportRequest
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
        
class SecureDRMSTimeOutError(SecureDRMSError):
    '''
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class SecureDRMSResponseError(SecureDRMSError):
    '''
    time-out waiting for DRMS Server to respon
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
        ssh_show_series_args_internal : dict
            `ssh_show_series_args_internal` contains arguments for the `ssh_show_series` SSH call; these arguments are used when accessing an INTERNAL series server:
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
    __validKeys = [ 'cgi_baseurl_internal', 'cgi_baseurl_authority', 'url_baseurl_internal', 'url_baseurl_authority', 'cgi_baseurl_authorityfile', 'ssh_base_bin', 'ssh_base_script', 'ssh_check_email', 'ssh_check_email_addresstab', 'ssh_check_email_domaintab', 'ssh_jsoc_fetch', 'ssh_jsoc_fetch_args', 'ssh_jsoc_fetch_internal', 'ssh_jsoc_fetch_internal_args', 'ssh_jsoc_info', 'ssh_jsoc_info_args', 'ssh_jsoc_info_internal', 'ssh_jsoc_info_internal_args', 'ssh_parse_recset', 'ssh_remote_env', 'ssh_remote_host', 'ssh_remote_port', 'ssh_remote_user', 'ssh_show_series', 'ssh_show_series_args', 'ssh_show_series_internal_args', 'ssh_show_series_wrapper', 'ssh_show_series_wrapper_args' ]


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
        
    def check_supported(self, op, use_ssh=False, use_internal=False):
        """Check if an operation is supported by the server."""
        if use_ssh:
            if op == 'check_email' or op == 'email':
                return self.ssh_check_email is not None and self.ssh_check_email_addresstab is not None and self.ssh_check_email_domaintab is not None and self.ssh_base_script is not None
            elif op == 'export':
                return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_jsoc_fetch is not None and self.ssh_base_bin is not None
            elif op == 'export_from_id':
                return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_jsoc_fetch is not None and self.ssh_base_bin is not None
            elif op == 'info':
                return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
            elif op == 'keys':
                return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
            elif op == 'pkeys':
                return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
            elif op == 'query':
                return self.ssh_jsoc_info is not None and self.ssh_parse_recset is not None and self.ssh_base_bin is not None
            elif op == 'series':
                if use_internal:
                    return (self.ssh_show_series is not None and self.ssh_show_series_internal_args is not None and self.ssh_base_bin is not None) or (self.ssh_show_series_wrapper is not None and self.ssh_show_series_wrapper_args is not None and self.ssh_base_script is not None)
                else:
                    return (self.ssh_show_series is not None and self.ssh_show_series_args is not None and self.ssh_base_bin is not None) or (self.ssh_show_series_wrapper is not None and self.ssh_show_series_wrapper_args is not None and self.ssh_base_script is not None)                
            else:
                return False
        else:
            if op == 'check_email' or op == 'email':
                return self.cgi_check_address is not None and self.cgi_baseurl is not None
            elif op == 'keys' or op == 'pkeys' or op == 'info':
                return self.cgi_jsoc_info is not None and self.cgi_baseurl is not None
            elif op == 'export' or op == 'export_from_id':
                return self.cgi_jsoc_info is not None and self.cgi_jsoc_fetch is not None and self.cgi_baseurl is not None
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
    def __init__(self, request, encoding):
        self._request = request
        self._encoding = encoding
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
        super().__init__(config.name, debug)
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

        return BasicAccessHttpJsonRequest(request, self._server.encoding)
        
    def _show_series(self, ds_filter=None, info=False):
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
    
    def show_series(self, ds_filter=None):
        return self._show_series(ds_filter)
        
    def show_series_wrapper(self, ds_filter=None, info=False):
        return self._show_series(ds_filter, info)


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
    def __init__(self, cmds, encoding, remote_user, remote_host, remote_port, password=None, debug=None):
        self._cmds = cmds
        self._encoding = encoding
        self._remoteUser = remote_user
        self._remoteHost = remote_host
        self._remotePort = remote_port
        self._debug = debug
        self._password = password
        self._passwordFailed = False
        self._data_str = None
        self._data = None
        
    def __repr__(self):
        return '<SSHJsonRequest "{name}"'.format(name=' '.join(self._cmds))
        
    def __getPassword(self):
        if self._password is None:
            print('please enter a password for {address}'.format(address=self._remoteUser + '@' + self._remoteHost))
            self._password = getpass.getpass()
        elif self._passwordFailed:
            print('permission denied, please re-enter a password for {address}'.format(address=self._remoteUser + '@' + self._remoteHost))
            self._password = getpass.getpass()
            
        return self._password
        
    def __runCmd(self):
        try:            
            # sshCmdList = [ '/usr/bin/ssh', '-p', str(self._remotePort), self._remoteUser + '@' + self._remoteHost, shlex.quote('/bin/bash -c ' + shlex.quote(' '.join(self._cmdList))) ]
            
            sshCmdList = [ '/usr/bin/ssh', '-p', str(self._remotePort), self._remoteUser + '@' + self._remoteHost, shlex.quote('/bin/bash -c ' + shlex.quote(' '.join(self._cmds))) ]
            
            if self._debug:
                print('running ssh command: {cmd}'.format(cmd=' '.join(sshCmdList)))

            child = pexpect.spawn(' '.join(sshCmdList))
            passwordAttempted = False
            while True:
                index = child.expect([ 'password:', pexpect.EOF ])
                if index == 0:
                    if passwordAttempted:
                        if self._debug:
                            print('ssh password failed; requesting user re-try')
                        self._passwordFailed = True
                    # user was prompted to enter password
                    self.__getPassword()
                    child.sendline(self._password.encode('UTF8'))
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
    def password(self):
        return self._password

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
    def __init__(self, config, use_internal=False, debug=False):
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
        
    def clearTimer(self):
        if self._debug:
            print('clearing timer for json client {client}'.format(client=repr(self)))

        if self._password_timer is not None:
            self._password_timer.cancel()
            
        if self._debug:
            print('cleared')

    def _json_request(self, cmdList):
        # prepend the cmdList with the env var settings
        envCmds = [ cmd + ';' for cmd in self._server.ssh_remote_env.bash_cmd() ]
        cmds = envCmds + [ ' '.join(cmdList) ]
        
        if self._debug:
            print('[ SSHJsonClient ] JSON request {cmd}'.format(cmd=' '.join(cmdList)))

        request = SSHJsonRequest(cmds, self._server.encoding, self._server.ssh_remote_user, self._server.ssh_remote_host, self._server.ssh_remote_port, self._password, self._debug)
            
        return request
        
    def __runCmd(self, cmdList):
        request = self._json_request(cmdList)

        # runs the ssh command
        response = request.data
        
        if self._password is None:
            if self._debug:
                print('[ SSHJsonClient._json_request ] storing password' )
            
            # we just obtained a password from the user; set a timer to clear it
            self._password = request.password        
        else:
            if self._debug:
                print('[ SSHJsonClient._json_request ] renewing password' )
                
            # a stored password, that has not yet expired, was used for the ssh command; renew it (reset the timer since the user used the stored password again
            self._password_timer.cancel()

        self._password_timer = threading.Timer(PASSWORD_TIMEOUT, self.__clearPassword)
        # IMPORTANT! making the timer threads daemons allows the interpreter to terminate in response to EOF (ctrl-d); otherwise, 
        # the main thread blocks on a join() on the timer thread until the timer 'fires' (and calls self.__clearPassword()); normally
        # this isn't so cool, but there does not seem to be a way for this module to 'intercept' an EOF sent to the interpreter
        # interactively
        self._password_timer.daemon = True
        
        if self._debug:
            print('[ SSHJsonClient._json_request ] starting password timer' )

        self._password_timer.start()

        return response
        
    def _show_series(self, ds_filter=None, info=False):
        # we have to intercept calls to both show_series parent methods, show_series() and show_series_wrapper(), and then do the
        # right thing depending on configuration parameters
        if self._use_internal or self._server.ssh_show_series_wrapper is None:
            # binary executable
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_show_series), '-qz' ]
            
            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')
            
            if self._use_internal:
                if self._server.ssh_show_series_internal_args is not None:
                    cmdList.extend([ key + '=' + val for key, val in self._server.ssh_show_series_internal_args.items() ])
            else:
                if self._server.ssh_show_series_args is not None:
                    cmdList.extend([ key + '=' + val for key, val in self._server.ssh_show_series_args.items() ])

            if ds_filter is not None:
                cmdList.append(ds_filter)
        else:
            # script
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_show_series_wrapper), '--json' ]
            
            if self._server.ssh_show_series_wrapper_args is not None:
                cmdList.extend([ key + '=' + val for key, val in self._server.ssh_show_series_wrapper_args.items() ])
            
            if ds_filter is not None:
                cmdList.append('--filter=' + ds_filter)

            if info:
                cmdList.append('--info' )

        return self.__runCmd(cmdList)
        
    def check_address(self, address):
        # external calls only (checkAddress.py)
        if address is None or not isinstance(address, str):
            raise SecureDRMSArgumentError('[ check_address ] missing or invalid argument email')
        
        # this check_email script accepts a URL argument string (like QUERY_STRING)
        argStr = urlencode({ 'address' : address, 'addresstab' : self._server.ssh_check_email_addresstab, 'checkonly' : 1, 'domaintab' : self._server.ssh_check_email_domaintab })
        cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_check_email), shlex.quote(argStr) ]
        return self.__runCmd(cmdList)
        
    def exp_request(self, ds, notify, method, protocol, protocol_args, filenamefmt, n, requestor):
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
                cmdList.append(shlex.quote('filenamefmt' + filenamefmt))
            if n is not None:
                cmdList.append('process=n=' + str(n))
            if requestor is None:
                cmdList.append('requestor=' + notify.split('@')[0])
            elif requestor is not False:
                cmdList.append('requestor=' + requestor)

            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._server.ssh_jsoc_fetch_internal_args is not None:
                cmdList.extend([ key + '=' + val for key, val in self._server.ssh_jsoc_fetch_internal_args.items() ])            
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

        return self.__runCmd(cmdList)
        
    def exp_status(self, requestid):
        # both external (jsocextfetch.py) and internal (jsoc_fetch) calls
        if requestid is None or not isinstance(requestid, str):
            raise SecureDRMSArgumentError('[ exp_status ] missing or invalid argument requestid')
            
        if self._use_internal:
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_fetch_internal), '-W', 'op=exp_status', 'requestid=' + requestid ]

            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')

            if self._server.ssh_jsoc_fetch_internal_args is not None:
                cmdList.extend([ key + '=' + val for key, val in self._server.ssh_jsoc_fetch_internal_args.items() ])
        else:
            # this script accepts a URL argument string (like QUERY_STRING)
            argStrUnencoded = { 'op' : 'exp_status', 'requestid' : requestid }
            
            if self._server.ssh_jsoc_fetch_args is not None:            
                argStrUnencoded.update(self._server.ssh_jsoc_fetch_args)

            argStr = urlencode(argStrUnencoded)
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_fetch), shlex.quote(argStr) ]
        
        return self.__runCmd(cmdList)

    def parse_recset(self, recset):
        # external call only (drms_parserecset)
        if recset is None or not isinstance(recset, str):
            raise SecureDRMSArgumentError('[ parse_recset ] missing or invalid argument recset')

        cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_parse_recset), shlex.quote('spec=' + recset) ]
        if self._server.encoding.lower() == 'utf8':
            cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')

        return self.__runCmd(cmdList)
        
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
                cmdList.extend([ key + '=' + val for key, val in self._server.ssh_jsoc_info_internal_args.items() ])
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
            
        return self.__runCmd(cmdList)
            
    def rs_summary(self, ds):
        # both external (jsocextinfo.py) and internal (jsoc_info) calls
        if self._use_internal:
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_info_internal), '-s', 'op=rs_summary', shlex.quote('ds=' + ds) ]
        
            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')
                
            if self._server.ssh_jsoc_info_internal_args is not None:
                cmdList.extend([ key + '=' + val for key, val in self._server.ssh_jsoc_info_internal_args.items() ])
        else:
            argStrUnencoded = { 'op' : 'rs_summary', 'ds' : ds }

            if self._server.ssh_jsoc_info_args is not None:            
                argStrUnencoded.update(self._server.ssh_jsoc_info_args)

            argStr = urlencode(argStrUnencoded)
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_info), shlex.quote(argStr) ]

        return self.__runCmd(cmdList)
    
    def series_struct(self, series):
        # both external (jsocextinfo.py) and internal (jsoc_info) calls
        if series is None or not isinstance(series, str):
            raise SecureDRMSArgumentError('[ series_struct ] missing or invalid argument series')

        if self._use_internal:
            cmdList = [ os.path.join(self._server.ssh_base_bin, self._server.ssh_jsoc_info_internal), '-s', 'op=series_struct', 'ds=' +  series ]

            if self._server.encoding.lower() == 'utf8':
                cmdList.append('DRMS_DBUTF8CLIENTENCODING=1')            

            if self._server.ssh_jsoc_info_internal_args is not None:
                cmdList.extend([ key + '=' + val for key, val in self._server.ssh_jsoc_info_internal_args.items() ])
        else:
            argStrUnencoded = { 'op' : 'series_struct', 'ds' : series }

            if self._server.ssh_jsoc_info_args is not None:
                argStrUnencoded.update(self._server.ssh_jsoc_info_args)

            argStr = urlencode(argStrUnencoded)
            cmdList = [ os.path.join(self._server.ssh_base_script, self._server.ssh_jsoc_info), shlex.quote(argStr) ]

        
        return self.__runCmd(cmdList)
    
    def show_series(self, ds_filter=None):
        return self._show_series(ds_filter)
        
    def show_series_wrapper(self, ds_filter=None, info=False):
        return self._show_series(ds_filter, info)
        
    @property
    def server(self):
        return self._server
        
    @server.setter
    def server(self, value):
        self._server = value
        
    @property
    def debug(self):
        return self._debug
        
    @debug.setter
    def debug(self, value):
        self._debug = value
    

class SecureClient(DRMSClient):
    '''
    a class that provides secure access to DRMS API methods
    
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
    def __init__(self, server='__JSOC', email=None, verbose=False, debug=False):
        self._config = SecureServerConfig.get(server)
        self._info_cache = {}   # for the info() method
        self.email = email      # use property for email validation
        self.verbose = verbose  # use property for conversion to bool

        # do not call parent's __init__() since that method creates an HttpJsonClient instance, but we need to create a BasicAccessHttpJsonClient/SSHJsonClient instead
        
    def __repr__(self):
        return '<SecureClient "{name}">'.format(name=self._config.name)
        
    def _extract_series_name(self, ds):
        # ART - eventually, call drms_parserecset CGI
        return utils._extract_series_name(ds)
        
    def __execute(self, apimethod, **args):
        resp = None

        try:
            apiName = cast(types.FrameType, inspect.currentframe()).f_back.f_code.co_name
        
            # ART - DO YOUR OWN CHECK, don't use parent
            if not self._server.check_supported(apiName, isinstance(self, SSHClient), self._use_internal):
                raise DrmsOperationNotSupported('Server does not support ' + apiName + ' access')        
        
            # set urls if this is an HTTP client
            if isinstance(self, BasicAccessClient):
                self._config.set_urls(self._use_internal, self.debug)
            
            if self.debug:
                print('[ SecureClient ] Executing DRMSClient API method "{method}"'.format(method=apiName))
            resp = apimethod(**args)
        except SecureDRMSError as exc:
            print(exc.msg, file=sys.stderr)

            import traceback
            print(traceback.format_exc(), file=sys.stderr)
            
        return resp
        
    # Public Interface
    def check_email(self, address):
        args = { 'email' : address }
        
        # call the parent's check_email() method
        return self.__execute(super().check_email, **args)
        
    def export(self, ds, method='url_quick', protocol='as-is', protocol_args=None, filenamefmt=None, n=None, email=None, requestor=None):        
        if filenamefmt is None:
            # override parent implementation
            series = self._extract_series_name(ds)
            filenamefmt = self._generate_filenamefmt(series)
            
        args = { 'ds' : ds, 'method' : method, 'protocol' : protocol, 'protocol_args' : protocol_args, 'filenamefmt' : filenamefmt, 'n' : n, 'email' : email, 'requestor' : requestor }

        # call the parent's export() method
        return self.__execute(super().export, **args)
    
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
        
    def pkeys(self, series):
        args = { 'ds' : series }
        
        # call the parent's pkeys() method
        return self.__execute(super().pkeys, **args)
        
    def query(self, ds, key=None, seg=None, link=None, convert_numeric=True, skip_conversion=None, pkeys=False, rec_index=False, n=None):
        args = { 'ds' : ds, 'key' : key, 'seg' : seg, 'link' : link, 'convert_numeric' : convert_numeric, 'skip_conversion' : skip_conversion, 'pkeys' : pkeys, 'rec_index' : rec_index, 'n' : n }
        
        # call the parent's query() method
        return self.__execute(super().query, **args)
            
    def series(self, regex=None, full=False):
        args = { 'regex' : regex, 'full' : full }
        
        # call the parent's series() method [ the server configuration parameters in the parent method will be ignored; they will be used,
        # however, in the self._json.show_series*() methods ]
        return self.__execute(self._series, **args)


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
    def __init__(self, server='__JSOC', use_internal=False, email=None, verbose=False, debug=False):
        super().__init__(server, email, verbose, debug)
        self._use_internal = use_internal
        self._json = BasicAccessHttpJsonClient(config=self._config, use_internal=use_internal, debug=debug)
        self.debug = debug
        
    def __repr__(self):
        return '<BasicAccessClient "{name}">'.format(name=self._config.name)
        
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
    def __init__(self, server='__JSOC', use_internal=False, email=None, verbose=False, debug=False):
        super().__init__(server, email, verbose, debug)
        self._use_internal = use_internal
        self._json = SSHJsonClient(config=self._config, use_internal=use_internal, debug=debug)
        self.debug = debug
        
    def __repr__(self):
        return '<SSHClient "{name}">'.format(name=self._config.name)
        
    def _extract_series_name(self, ds):
        parsed = self._json.parse_recset(ds)
        return parsed['subsets'][0]['seriesname'].lower()
        
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
    
    def __init__(self, server='__JSOC', email=None, verbose=False, debug=False):
        self._config = SecureServerConfig.get(server)
        self._args = { 'email' : email, 'verbose' : verbose, 'debug' : debug }
        
    def create_client(self, use_ssh=False, use_internal=False):
        client = None
        args = self._args
        args['use_internal'] = use_internal
            
        if use_ssh:
            client = SSHClient(self._config.name, **args)
            
            # add to list of clients whose timers might need to be canceled upon termination
            SecureClientFactory.__clients.append(client)
        elif False:
            # if we need additional clients, like https, make a new elsif statement immediately above this one
            pass
        else:
            # default to basic access
            client = BasicAccessClient(self._config.name, **self._args)

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
    cgi_jsoc_info='jsoc_info',
    cgi_jsoc_fetch='jsoc_fetch',
    cgi_check_address='checkAddress.sh',
    cgi_show_series='show_series',
    cgi_show_series_wrapper='showextseries',
    show_series_wrapper_dbhost='hmidb2',
    ssh_base_bin='/home/jsoc/cvs/Development/JSOC/bin/linux_avx',
    ssh_base_script='/home/jsoc/cvs/Development/JSOC/scripts',
    ssh_check_email='checkAddress.py',
    ssh_check_email_addresstab='jsoc.export_addresses',
    ssh_check_email_domaintab='jsoc.export_addressdomains',
    ssh_jsoc_info='jsocextinfo.py',
    ssh_jsoc_info_args=
    {
        'dbhost' : 'hmidb2',
        'N' : 1
    },
    ssh_jsoc_info_internal='jsoc_info',
    ssh_jsoc_info_internal_args=
    {
        'JSOC_DBHOST' : 'hmidb',
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
