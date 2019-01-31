import sys
import os
import base64
import re
import importlib
from six.moves.urllib import request as sixUrlRequest
import drms
from drms.config import ServerConfig, register_server
from drms.json import HttpJsonClient, HttpJsonRequest
from drms.client import Client

class PrivateServerConfig(ServerConfig):
    __validKeys = [ 'cgi_baseurl_authority', 'cgi_baseurl_authorityfile' ]

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
                        raise
                        raise ValueError('authority file ' + kwargs['cgi_baseurl_authorityfile'] + ' is invalid')
                    except NameError as exc:
                        if re.search(r'getAuthority', str(exc)) is not None:
                            raise ValueError('authority file ' + kwargs['cgi_baseurl_authorityfile'] + ' does not contain getAuthority() definition')
                        raise
                else:
                    raise ValueError('authority file ' + kwargs['cgi_baseurl_authorityfile'] + ' does not exist')

        super(PrivateServerConfig, self).__init__(config, **kwargsToParent)

    def getAuthority(self, encodedAuthority):
        return base64.b64decode(encodedAuthority)


class PrivateHttpJsonClient(HttpJsonClient):
    def __init__(self, server='__jsoc', debug=False):
        super(PrivateHttpJsonClient, self).__init__(server, debug)

    def _json_request(self, url):
        if self.debug:
            print(url)

        # we need to add the authority information
        request = sixUrlRequest.Request(url)

        # self._server is the PrivateServerConfig that has the server authority information
        if 'cgi_baseurl_authority' in self._server._d:
            request.add_header("Authorization", "Basic " + base64.b64encode(self._server._d['cgi_baseurl_authority'].encode()).decode())

        return PrivateHttpJsonRequest(request, self._server.encoding)

class PrivateHttpJsonRequest(HttpJsonRequest):
    def __init__(self, request, encoding):
        self._encoding = encoding
        self._http = sixUrlRequest.urlopen(request)
        self._data_str = None
        self._data = None

        # do not call parent's __init__() since that method calls urlopen without first making a Request; we need to make a Request so we can add the authentication header


class Client(Client):
    def __init__(self, server='__jsoc', email=None, verbose=False, debug=False):
        self._json = PrivateHttpJsonClient(server=server, debug=debug)
        self._info_cache = {}
        self.verbose = verbose  # use property for convertion to bool
        self.email = email      # use property for email validation

        # do not call parent's __init__() since that method creates an HttpJsonClient instance, but we need to create a PrivateHttpJsonClient instead

# register private JSOC DRMS server
register_server(PrivateServerConfig(
    name='__JSOC',
    cgi_baseurl='http://jsoc2.stanford.edu/cgi-bin/ajax/',
    cgi_baseurl_authority='hmiteam:hmiteam',
#    cgi_baseurl_authorityfile='/Users/art/HEPL/drmsPy/auth.py',
    cgi_show_series='show_series',
    cgi_jsoc_info='jsoc_info',
    cgi_jsoc_fetch='jsoc_fetch',
    cgi_check_address='checkAddress.sh',
    cgi_show_series_wrapper='showextseries',
    show_series_wrapper_dbhost='hmidb',
    http_download_baseurl='http://jsoc.stanford.edu/',
    ftp_download_baseurl='ftp://pail.stanford.edu/export/'))
