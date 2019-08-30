import sys
import os
import base64
import re
import pexpect
import importlib
from json import loads, decoder
import getpass
from six.moves.urllib import request as sixUrlRequest
import pandas
import drms
from drms.config import ServerConfig, register_server
from drms.json import HttpJsonClient, HttpJsonRequest
from drms.client import Client as DRMSClient


DEFAULT_SSH_PORT=22
DEFAULT_SERVER_ENCODING='utf8'

class DRMSException(Exception):
    '''
    '''
    def __init__(self, msg):
        super(DRMSException, self).__init__(msg)
        self.msg = msg


class DRMSConfigurationException(DRMSException):
    '''
    '''
    def __init__(self, msg):
        super(DRMSConfigurationException, self).__init__(msg)
        self.msg = msg

class DRMSArgumentException(DRMSException):
    '''
    '''
    def __init__(self, msg):
        super(DRMSArgumentException, self).__init__(msg)
        self.msg = msg
        
class DRMSTimeOutException(DRMSException):
    '''
    '''
    def __init__(self, msg):
        super(DRMSTimeOutException, self).__init__(msg)
        self.msg = msg

class DRMSResponseException(DRMSException):
    '''
    '''
    def __init__(self, msg):
        super(DRMSResponseException, self).__init__(msg)
        self.msg = msg



class PrivateServerConfig(ServerConfig):
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
        
        super(PrivateServerConfig, self).__init__(config, **kwargsToParent)    
        
    def check_supported(self, op):
        """Check if an operation is supported by the server."""
        if self.use_ssh:
            if op == 'series':
                return self.ssh_show_series is not None or self.ssh_show_series_wrapper is not None
        else:
            return super(PrivateServerConfig, self).check_supported(op)

    def getAuthority(self, encodedAuthority):
        return base64.b64decode(encodedAuthority)
        
    @classmethod
    def register_server(cls, config):
        cls.set(config.name, config)

    @classmethod
    def get(cls, name='__JSOC'):
        try:
            return cls.__configs[name.lower()]
        except KeyError:
            raise DRMSArgumentException('configuration ' + name + ' does not exist')

    @classmethod
    def set(cls, name, config):
        if isinstance(config, cls):
            cls.__configs[name.lower()] = config

            if isinstance(config, ServerConfig):
                # put this in the global used by the parent ServerConfig class too
                register_server(config)
        else:
            raise DRMSArgumentException('config is of type ' + type(config) + '; must be of type ' + cls.__name__)

class PrivateHttpJsonRequest(HttpJsonRequest):
    def __init__(self, request, encoding):
        self._encoding = encoding
        self._http = sixUrlRequest.urlopen(request)
        self._data_str = None
        self._data = None

        # do not call parent's __init__() since that method calls urlopen without first making a Request; we need to make a Request so we can add the authentication header


class PrivateSSHJsonRequest(object):
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
        return '<PrivateSSHJsonRequest ' + '"' + self.url + '">'
        
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
            raise DRMSConfigurationException(traceback.format_exc(1))
        except pexpect.exceptions.TIMEOUT:
            raise DRMSTimeOutException('time-out waiting server to respond')

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
            try:
                self._data = loads(self.raw_data.decode(self._encoding))
            except decoder.JSONDecodeError:
                raise DRMSResponseException('invalid json response: ' + self.raw_data.decode(self._encoding))
        return self._data


class PrivateHttpJsonClient(HttpJsonClient):
    def __init__(self, config, debug=False):
        super(PrivateHttpJsonClient, self).__init__(config.name, debug)

    def _json_request(self, url):
        if self.debug:
            print(url)

        # we need to add the authority information
        request = sixUrlRequest.Request(url)

        try:
            # self._server is the PrivateServerConfig that has the server authority information
            passPhrase = self._server.cgi_baseurl_authority
            request.add_header("Authorization", "Basic " + base64.b64encode(passPhrase.encode()).decode())
        except AttributeError:
            # the user did not provide a passPhrase
            pass

        return PrivateHttpJsonRequest(request, self._server.encoding)


class PrivateSSHJsonClient(object):
    def __init__(self, config, debug=False):
        self._server = config
        self._debug = debug

    def _json_request(self, cmdList):
        if self._debug:
            print(' '.join(cmdList))

        # runs the ssh command
        return PrivateSSHJsonRequest(cmdList, self._server.encoding, self._server.ssh_remote_user, self._server.ssh_remote_host, self._server.ssh_remote_port, self._debug)
        
    def show_series(self, ds_filter=None):
        cmdList = [ self._server.ssh_show_series ]

        if ds_filter is not None:
            cmdList.append('filter=' + ds_filter)

        req = self._json_request(cmdList)
        resp = req.data
        
        if self._debug:
            print('json response: ' + resp)

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
        
        if self._debug:
            print('json response: ' + resp)

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
    

class Client(DRMSClient):
    def __init__(self, server='__JSOC', email=None, verbose=False, debug=False):
        self._config = PrivateServerConfig.get(server)
        self._json = PrivateHttpJsonClient(config=self._config, debug=debug)
        self._info_cache = {}
        self.email = email      # use property for email validation
        self.verbose = verbose  # use property for conversion to bool

        # do not call parent's __init__() since that method creates an HttpJsonClient instance, but we need to create a PrivateHttpJsonClient instead


class SSHClient(Client):
    def __init__(self, server='__JSOC', email=None, verbose=False, debug=False):
        self._config = PrivateServerConfig.get(server)
        self._json = PrivateSSHJsonClient(self._config, debug)
        self._info_cache = {}
        self.email = email      # use property for email validation
        self.verbose = verbose  # use property for conversion to bool
        
    def __master(self, method, **args):
        resp = None

        try:
            resp = method(**args)
        except DRMSException as exc:
            print(exc.msg, file=sys.stderr)

            import traceback
            print(traceback.format_exc(), file=sys.stderr)
            
        return resp

        # do not call parent's __init__() since that method creates an HttpJsonClient instance, but we need to create a PrivateHttpJsonClient instead
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
        return self.__master(self.__series, **args)


class ClientFactory(object):
    '''
        Use use_ssh server configuration parameter to determine which Client instance (original CGI, or SSH) to create
    '''
    def __init__(self, server='__JSOC', email=None, verbose=False, debug=False):
        self._config = PrivateServerConfig.get(server)
        self._args = { 'email' : email, 'verbose' : verbose, 'debug' : debug }
        
    def CreateClient(self, use_ssh=False):
        self._config.use_ssh = use_ssh

        if self._config.use_ssh:
            return SSHClient(self._config.name, **self._args)
        else:
            return Client(self._config.name, **self._args)


# register private JSOC DRMS server
PrivateServerConfig.register_server(PrivateServerConfig(
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
