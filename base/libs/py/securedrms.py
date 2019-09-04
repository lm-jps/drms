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
>>> sshclient.series('su_arta')
please enter password for arta@solarport
Password:
['su_arta.Lw_720s', 'su_arta.M_45s_test2', 'su_arta.M_720s', 'su_arta.V_720s', 'su_arta.aia__lev1_euv_12s', 'su_arta.aia__lev1b', 'su_arta.ambig_tst_in0', 'su_arta.awesometest', 'su_arta.b_thr0150_thr1250', 'su_arta.dark', 'su_arta.detune', 'su_arta.export', 'su_arta.export_procs', 'su_arta.fds', 'su_arta.fds_orbit_vectors', 'su_arta.gentrack', 'su_arta.globalhs_history', 'su_arta.hmi_lev1', 'su_arta.lev1', 'su_arta.lev1_12s4arc', 'su_arta.psf', 'su_arta.slonylogs', 'su_arta.testBigHDU', 'su_arta.vw_V', 'su_arta.vw_V_sht', 'su_arta.ylm_vw_V_1024_sht_8h']
>>>
>>> # create a Basic Access HTTP client
>>> httpclient = factory.create_client()
>>> httpclient
<BasicAccessClient "__JSOC">
>>> # list DRMS series whose names contain the string 'su_arta'; print a listing that contains series information
>>> httpclient.series('su_arta', True)
                            name  \
0                su_arta.Lw_720s
1            su_arta.M_45s_test2
2                 su_arta.M_720s
3                 su_arta.V_720s
4      su_arta.aia__lev1_euv_12s
5             su_arta.aia__lev1b
6          su_arta.ambig_tst_in0
7            su_arta.awesometest
8      su_arta.b_thr0150_thr1250
9                   su_arta.dark
10                su_arta.detune
11                su_arta.export
12          su_arta.export_procs
13                   su_arta.fds
14     su_arta.fds_orbit_vectors
15              su_arta.gentrack
16      su_arta.globalhs_history
17              su_arta.hmi_lev1
18                  su_arta.lev1
19          su_arta.lev1_12s4arc
20                   su_arta.psf
21             su_arta.slonylogs
22            su_arta.testBigHDU
23                  su_arta.vw_V
24              su_arta.vw_V_sht
25  su_arta.ylm_vw_V_1024_sht_8h

                                                 note
0           linewidths with a cadence of 720 seconds.
1          magnetograms with a cadence of 45 seconds.
2         magnetograms with a cadence of 720 seconds.
3          Dopplergrams with a cadence of 720 seconds
4                      AIA Level 1, 12 second cadence
5                                        AIA level 1b
6     Test input data for vector field disambiguation
7                                    AWESOME RAW DATA
8                               Disambiguated ME HARP
9                                     HMI bias / dark
10                               MDI Calibration data
11                                               JSOC
12  This dataseries contains one record per availa...
13  This series contains ingested, pre-launch and ...
14  Helio- and Geo-centric orbit position and velo...
15                 miscellaneous tracked mapped cubes
16       ancillary dataseries for processing metadata
17                                        HMI Level 1
18                                        HMI Level 1
19                                        AIA Level 1
20                          HMI Point Spread Function
21  This series contains ingested, pre-launch FDS ...
22                 test the creation of an HDU > 4GB.
23                    MDI Vector-Weighted Dopplergram
24  medium-l timeseries for output of jretile and ...
25  spherical harmonic transforms of artificial im...
'''

# standard library imports
import base64
import getpass
import importlib
from json import loads, decoder
import os
import pexpect
import re
import sys

# third party imports
import pandas
from six.moves.urllib import request as sixUrlRequest

# local imports
import drms
from drms.config import ServerConfig, register_server
from drms.json import HttpJsonClient, HttpJsonRequest
from drms.client import Client as DRMSClient


__all__ = [ 'SecureDRMSConfigurationError', 'SecureDRMSConfigurationError', 'SecureDRMSArgumentError', 'SecureDRMSTimeOutError', 'SecureDRMSResponseError', 'SecureServerConfig', 'BasicAccessHttpJsonRequest', 'BasicAccessHttpJsonClient', 'SSHJsonRequest', 'SSHJsonClient', 'BasicAccessClient', 'SSHClient', 'SecureClientFactory' ]

DEFAULT_SSH_PORT=22
DEFAULT_SERVER_ENCODING='utf8'

class SecureDRMSError(Exception):
    '''
    base exception class for all Secure DRMS exceptions
    '''
    def __init__(self, msg):
        super(SecureDRMSError, self).__init__(msg)
        self.msg = msg

class SecureDRMSConfigurationError(SecureDRMSError):
    '''
    invalid or missing Secure DRMS server configuration property
    '''
    def __init__(self, msg):
        super(SecureDRMSConfigurationError, self).__init__(msg)
        self.msg = msg

class SecureDRMSArgumentError(SecureDRMSError):
    '''
    invalid or missing method argument
    '''
    def __init__(self, msg):
        super(SecureDRMSArgumentError, self).__init__(msg)
        self.msg = msg
        
class SecureDRMSTimeOutError(SecureDRMSError):
    '''
    '''
    def __init__(self, msg):
        super(SecureDRMSTimeOutError, self).__init__(msg)
        self.msg = msg

class SecureDRMSResponseError(SecureDRMSError):
    '''
    time-out waiting for DRMS Server to respon
    '''
    def __init__(self, msg):
        super(SecureDRMSResponseError, self).__init__(msg)
        self.msg = msg


class SecureServerConfig(ServerConfig):
    '''
    a class to create, maintain, and store DRMS server configurations
    
    Class Variables
    ---------------
    __configs : dictionary
        a container of all registered server configurations
    __validKeys : list
        a list of all valid supplemental (to ServerConfig._valid_keys) configuration properties
    
    Constructor Parameters
    ----------------------
    config : SecureServerConfig
        an existing SecureServerConfig used to initialize the new SecureServerConfig; the new SecureServerConfig is created by copying the properties in the existing SecureServerConfig
    kwargs : dict
        a dictionary of SecureServerConfig properties; these properties are added to the initial SecureServerConfig (after the properties in config have been copied)

    Class Methods
    -------------
    register_server : config:SecureServerConfig -> None
        add the server configuration `config` to the dictionary of SecureServerConfigs (SecureServerConfig.__configs); add `config` to the dictionary of  ServerConfigs (config._server_configs)
    get : name:str -> SecureServerConfig
        return the SecureServerConfig by the name of `name`
    set : name:str, config:SecureServerConfig -> None
        add the server configuration `config` to the dictionary of SecureServerConfigs (SecureServerConfig.__configs)
        
    Public Instance Methods
    -----------------------
    check_supported : op:string -> boolean
        returns true of if the operation `op` is supported by the instance
        
    additional methods are defined in the parent class ServerConfig    
    '''
    __configs = {}
    __validKeys = [ 'cgi_baseurl_authority', 'cgi_baseurl_authorityfile', 'ssh_base_bin', 'ssh_remote_user', 'ssh_remote_host', 'ssh_remote_port', 'ssh_show_series', 'ssh_jsoc_info', 'ssh_jsoc_fetch', 'ssh_check_address', 'ssh_show_series_wrapper' ]

    def __init__(self, config=None, **kwargs):
        # add parameters for the login information
        for key in self.__validKeys:
            self._valid_keys.append(key)

        # add the authority to the base URL
        kwargsToParent = kwargs
        if 'cgi_baseurl' in kwargs:
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

        super(SecureServerConfig, self).__init__(config, **kwargsToParent)
        
    def __repr__(self):
        return '<SecureServerConfig ' + '"' + self.name + '">'
        
    def check_supported(self, op):
        """Check if an operation is supported by the server."""
        if self.use_ssh:
            if op == 'series':
                return self.ssh_show_series is not None or self.ssh_show_series_wrapper is not None
        else:
            return super(SecureServerConfig, self).check_supported(op)
        
    @classmethod
    def register_server(cls, config):
        cls.set(config.name, config)

    @classmethod
    def get(cls, name='__JSOC'):
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
    
    Constructor Parameters
    ----------------------
    request : urllib.request.Request
        abstraction of an HTTP URL request; BasicAccessHttpJsonRequest._http is a http.client.HTTPResponse
    encoding : str
        the name of one of the following JSON encodings: UTF-8, UTF-16, or UTF-32
    '''
    def __init__(self, request, encoding):
        self._request = request
        self._encoding = encoding
        self._http = sixUrlRequest.urlopen(request)
        self._data_str = None
        self._data = None
        # do not call parent's __init__() since that method calls urlopen without first making a Request; we need to make a Request so we can add the authentication header
        
    def __repr__(self):
        return '<BasicAccessHttpJsonRequest ' + '"' + self._request.full_url + '">'


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
    _json_request : url:string -> BasicAccessHttpJsonRequest
        returns a JSON request appropriate for a web API (implemented as web-application URL `url`)
       
    Public Instance Methods
    -----------------------
    all methods are defined in the parent class HttpJsonClient
    '''
    def __init__(self, config, debug=False):
        super(BasicAccessHttpJsonClient, self).__init__(config.name, debug)
        
    def __repr__(self):
        return '<SSHJsonClient ' + '"' + self._server.name + '">'

    def _json_request(self, url):
        if self.debug:
            print(url)

        # we need to add the authority information
        request = sixUrlRequest.Request(url)

        try:
            # self._server is the SecureServerConfig that has the server authority information
            passPhrase = self._server.cgi_baseurl_authority
            request.add_header("Authorization", "Basic " + base64.b64encode(passPhrase.encode()).decode())
        except AttributeError:
            # the user did not provide a passPhrase
            pass

        return BasicAccessHttpJsonRequest(request, self._server.encoding)


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
    __runCmd : None -> bytes
        executes the ssh command on the ssh server; returns a bytes object that represents an encoded JSON string
    '''
    def __init__(self, cmdList, encoding, remote_user, remote_host, remote_port, debug=None):
        self._cmdList = cmdList
        self._encoding = encoding
        self._remoteUser = remote_user
        self._remoteHost = remote_host
        self._remotePort = remote_port
        self._debug = debug
        self._pword = None
        self._data_str = None
        self._data = None
        
    def __repr__(self):
        return '<SSHJsonRequest ' + '"' + ' '.join(self._cmdList) + '">'
        
    def __getPassword(self):
        if self._pword is None:
            print('please enter password for ' + self._remoteUser + '@' + self._remoteHost)
            pword = getpass.getpass()
            
        return pword

    def __runCmd(self):
        try:
            sshCmdList = [ '/usr/bin/ssh', '-p', str(self._remotePort), self._remoteUser + '@' + self._remoteHost, ' '.join(self._cmdList) ]
            
            if self._debug:
                print('running ssh command: ' + ' '.join(sshCmdList))

            child = pexpect.spawn(' '.join(sshCmdList))
            while True:
                index = child.expect([ 'password:', pexpect.EOF ])
                if index == 0:
                    # user was prompted to enter password
                    password = self.__getPassword()
                    child.sendline(password.encode('UTF8'))
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
                print('json response: ' + jsonStr)
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
    debug : bool
        if true, print debugging statements
        
    Private Instance Methods
    ------------------------
    _json_request : cmdList:list -> SSHJsonRequest
        creates, from `cmdList` and returns a JSON request appropriate for an ssh-server API
        
    Public Instance Methods
    -----------------------
    show_series : ds_filter:str -> object 
        executes the show_series command on the server; `ds_filter` is a POSIX Extended Regular Expression that filters-in the series that are returned in the returned list object
    show_series_wrapper : ds_filter:str, info:bool -> object 
        executes the showintseries.py command on the server; `ds_filter` is a POSIX Extended Regular Expression that filters-in the series that are returned in the object; if `info` is True, then a description of each series is included in the returned pandas.DataFrame object
    '''
    def __init__(self, config, debug=False):
        self._server = config
        self._debug = debug
        
    def __repr__(self):
        return '<SSHJsonClient ' + '"' + self._server.name + '">'

    def _json_request(self, cmdList):
        if self._debug:
            print(' '.join(cmdList))

        # runs the ssh command
        return SSHJsonRequest(cmdList, self._server.encoding, self._server.ssh_remote_user, self._server.ssh_remote_host, self._server.ssh_remote_port, self._debug)
        
    def show_series(self, ds_filter=None):
        cmdList = [ self._server.ssh_show_series ]

        if ds_filter is not None:
            cmdList.append('filter=' + ds_filter)

        req = self._json_request(cmdList)
        resp = req.data

        return resp
        
    def show_series_wrapper(self, ds_filter=None, info=False):
        cmdList = [ self._server.ssh_show_series_wrapper ]

        cmdList.append('--json')

        if ds_filter is not None:
            cmdList.append('--filter=' + ds_filter)

        if info:
            cmdList.append('--info' )

        req = self._json_request(cmdList)
        resp = req.data

        return resp
        
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
        # do not call parent's __init__() since that method creates an HttpJsonClient instance, but we need to create a BasicAccessHttpJsonClient instead
        
    def __repr__(self):
        return '<SecureClient ' + '"' + self._config.name + '">'


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
    def __init__(self, server='__JSOC', email=None, verbose=False, debug=False):
        super(BasicAccessClient, self).__init__(server, email, verbose, debug)
        self._json = BasicAccessHttpJsonClient(config=self._config, debug=debug)
        
    def __repr__(self):
        return '<BasicAccessClient ' + '"' + self._config.name + '">'


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
    __series() : regex:str, full:bool -> object
        returns a list of series names filtered by regex, POSIX Extended Regular Expression; if full is True, then returns a pandas data frame that contains a list of series and series descriptions
        
    additional methods are defined in the parent class DRMSClient

    Public Instance Methods
    -----------------------
    series() : regex:str, full:bool -> object
        API method wrapper around __series()
    '''
    def __init__(self, server='__JSOC', email=None, verbose=False, debug=False):
        super(SSHClient, self).__init__(server, email, verbose, debug)
        self._json = SSHJsonClient(self._config, debug)
        
    def __repr__(self):
        return '<SSHClient ' + '"' + self._config.name + '">'
        
    def __execute(self, method, **args):
        resp = None

        try:
            resp = method(**args)
        except DRMSException as exc:
            print(exc.msg, file=sys.stderr)

            import traceback
            print(traceback.format_exc(), file=sys.stderr)
            
        return resp

        # do not call parent's __init__() since that method creates an HttpJsonClient instance, but we need to create a BasicAccessHttpJsonClient instead
    def __series(self, regex=None, full=False):
        if not self._server.check_supported('series'):
            raise DrmsOperationNotSupported('Server does not support series list access')

        if self._server.ssh_show_series_wrapper is None:
            # No wrapper CGI available, use the regular version.
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
            # Use show_series_wrapper instead of the regular version.
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
            
    def series(self, regex=None, full=False):
        args = { 'regex' : regex, 'full' : full }
        return self.__execute(self.__series, **args)


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
    create_client : use_ssh:bool -> SecureClient
        returns a SSHClient if `use_ssh` is True, otherwise returns a BasicAccessClient
    '''
    def __init__(self, server='__JSOC', email=None, verbose=False, debug=False):
        self._config = SecureServerConfig.get(server)
        self._args = { 'email' : email, 'verbose' : verbose, 'debug' : debug }
        
    def create_client(self, use_ssh=False):
        self._config.use_ssh = use_ssh

        if self._config.use_ssh:
            return SSHClient(self._config.name, **self._args)
        elif False:
            # if we need additional clients, like https, make a new elsif statement immediately above this one
            pass
        else:
            # default to basic access
            return BasicAccessClient(self._config.name, **self._args)


# register secure JSOC DRMS server
SecureServerConfig.register_server(SecureServerConfig(
    name='__JSOC',
#   use_ssh=True,
    cgi_baseurl='http://jsoc2.stanford.edu/cgi-bin/ajax/',
    cgi_baseurl_authority='hmiteam:hmiteam',
#   cgi_baseurl_authorityfile='/Users/art/HEPL/drmsPy/auth.py',
    cgi_show_series='show_series',
    cgi_jsoc_info='jsoc_info',
    cgi_jsoc_fetch='jsoc_fetch',
    cgi_check_address='checkAddress.sh',
    cgi_show_series_wrapper='showintseries',
    ssh_base_bin='/home/jsoc/cvs/Development/JSOC/bin/linux_avx',
    ssh_remote_user='arta',
    ssh_remote_host='solarport',
    ssh_remote_port='22',
    ssh_show_series='show_series',
    ssh_show_series_wrapper='showintseries.py',
    ssh_jsoc_info='jsoc_info',
    ssh_jsoc_fetch='jsoc_fetch',
    ssh_check_address='checkAddress.sh',
    show_series_wrapper_dbhost='hmidb',
    http_download_baseurl='http://jsoc.stanford.edu/',
    ftp_download_baseurl='ftp://pail.stanford.edu/export/'))
