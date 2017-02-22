'''
Created on Jun 13, 2015

@author: LewisGF
'''
import requests, json
import datetime
import logging
from pytz import timezone

logging.basicConfig()
logger = logging.getLogger(__name__)

TZ_UTC   = timezone('UTC')
TZ_LOCAL = timezone('US/Eastern')

DATE_FORMAT     = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

ACCEPT_JSON = {'Accept': 'application/json'}

def setLocalTimeZone(tzname):
    global TZ_LOCAL
    TZ_LOCAL = timezone(tzname)

class DateTime(datetime.datetime):
    """
    This subclass of datetime.datetime is for working with ServiceNow dates
    which are in UTC and use a standard format.
    The constructor has been overridden so that you can easily instantiate
    a datetime from a string value.
    
    Example:
        d1 = DateTime('2015-09-15 13:45:00')
    """       
    def __new__(cls, arg, local=False):
        if isinstance(arg, str):
            # argument is a str as YYYY-MM-DD HH:MM:SS
            if len(arg) == 10:
                # time is mising; set it to midnight 
                arg += ' 00:00:00'
            dt = datetime.datetime.strptime(arg, DATETIME_FORMAT)
            if local:
                dt = TZ_LOCAL.localize(dt).astimezone(TZ_UTC)
            else:
                dt = dt.replace(tzinfo=TZ_UTC)
        else:
            if isinstance(arg, datetime.datetime):
                # argument is a datetime.datetime object
                if arg.tzinfo is None:
                    if local:
                        dt = TZ_LOCAL.localize(arg).astimezone(TZ_UTC)
                    else:
                        dt = arg.replace(tzinfo=TZ_UTC)
                else:
                    dt = arg.astimezone(TZ_UTC)
            else:
                if isinstance(arg, datetime.date):
                    # argument is a datetime.date object
                    if local:
                        dt = TZ_LOCAL.localize(
                            datetime.datetime(arg.year, arg.month, arg.day))
                    else:
                        dt = TZ_UTC.localize(
                            datetime.datetime(arg.year, arg.month, arg.day))
                    dt = dt.astimezone(TZ_UTC)
                else:
                    raise ValueError('Invalid argument type: ' + str(type(arg)))
        return super().__new__(cls, 
            dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo=TZ_UTC)
        
    def asLocal(self):
        return self.astimezone(TZ_LOCAL).strftime(DATETIME_FORMAT)
    
    def asUTC(self):
        return self.astimezone(TZ_UTC).strftime(DATETIME_FORMAT)

    def __str__(self):
        return self.asUTC()
    
    @staticmethod
    def now():
        return DateTime(datetime.datetime.now(TZ_UTC))
        
    @staticmethod
    def today(local=True):
        t = DateTime.now().asLocal()[0:10]
        return DateTime(t, local=local)

class DateTimeRange(tuple):
    """
    This class represents a datetime range, i.e. a pair of datetimes
    """
    
    def __new__(cls, dt1, dt2):
        if not isinstance(dt1, datetime.datetime): raise ValueError('dt1 not datetime')
        if not isinstance(dt2, datetime.datetime): raise ValueError('dt2 not datetime')
        return super().__new__(cls, (dt1, dt2))
    
    def start(self):
        return self[0]
    
    def end(self):
        return self[1]
    
    def __str__(self):
        return "[%s,%s]" % (self[0].asUTC(), self[1].asUTC())
                
    @staticmethod
    def fromDate(ymd, local=True):
        """        
        Convert a date into a pair of DateTime values that are 24 hours apart
        """
        t0 = DateTime(ymd.strftime(DATE_FORMAT), local=local)
        almost24hours = datetime.timedelta(seconds=(24*60*60 - 1))
        t1 = DateTime(t0 + almost24hours)
        r = DateTimeRange(t0, t1)
        # logging.DEBUG('rangeFromDate(%s)=%s' % (ymd, str(r)))
        return r
    
    def overlapSeconds(self, other):
        """
        Does this range overlap with another range?
        If so, return the number of seconds.
        """
        latest_start = max(self[0], other[0])
        earliest_end = min(self[1], other[1])
        if latest_start >= earliest_end:
            diff_sec = 0
        else:
            diff_sec = (earliest_end - latest_start).total_seconds()
        return diff_sec

    def overlapsWith(self, other):
        return self.overlapSeconds(other) > 0
        
class ServiceNowError(RuntimeError):
    pass

DEBUG_LEN = 256

def logRequest(logger, request, level=logging.DEBUG):
    if logger.isEnabledFor(level):
        logger.log(level, 'request=%s' % request)
        
def logResponse(logger, response, level=logging.DEBUG):
    if logger.isEnabledFor(level):
        logger.log(level, 'status_code=%s' % response.status_code)
        logger.log(level, 'headers=%s' % response.headers)
        # logging.DEBUG('cookies=%s' % response.cookies)       
        text = response.text
        if DEBUG_LEN is not None and len(text) > DEBUG_LEN:
            text = text[0:DEBUG_LEN] + '<<truncated>>'
        logger.log(level, 'text=%s' % text)
    
class ServiceNow:
    """
    This class holds the connection credentials for a ServiceNow instance.
    """
    
    def __init__(self, instance, username, password, debug=False):
        if instance.startswith('https://'): 
            base = str(instance)
        else: 
            base = 'https://' + str(instance);
        if instance.find('.') < 0: base += '.service-now.com'
        if base.endswith('/'): base = base[0:-1] # remove trailing slash
        self.baseurl = base
        self.username = username
        # self.password = password
        self.auth = (username, password)
        self.jsessionid = None
        self.cookies = None
        self.lastRequest = None
        self.lastResponse = None

    def table(self, name):
        return Table(self, name)
    
    def url(self, stuff):
        result = self.baseurl
        if not stuff.startswith('/'): result += '/'
        result += stuff
        return result
    
    def link(self, rec, menu=False):
        """
        Return a link to a record
        """
        table = rec['sys_class_name']
        sys_id = rec['sys_id']
        if menu:
            url = self.url('nav_to.do?uri=' + table + '.do?sys_id=' + sys_id)
        else:
            url = self.url(table + '.do?sys_id=' + sys_id)
        return url        
        
    def connect(self):
        """
        This method is superfluous; but it can be used to verify connection credentials.
        It throws 
        """
        try:
            sys_user = self.table('sys_user')
            recs = sys_user.query('user_name=' + self.username).run()
        except:
            raise ServiceNowError(
                'Unable to connect to %s as %s' % (self.baseurl, self.username))
        if len(recs) != 1:
            raise ServiceNowError(
                'Unable to connect to %s as %s' % (self.baseurl, self.username)) 
        return self

    def _request(self, method, url, sys_id=None, params=None, data=None):
        if sys_id: url += '/' + sys_id
        method = method.upper()
        self.lastRequest = {
            'method' : method,
            'url' : url,
            'params' : params,
            }
        if logger.isEnabledFor(logging.DEBUG):
            logRequest(logger, self.lastRequest, logging.DEBUG)
        self.lastResponse = requests.request(method, url, 
            headers=ACCEPT_JSON, params=params, data=data, 
            auth=self.auth, cookies=self.cookies)
        if logger.isEnabledFor(logging.DEBUG):
            logResponse(logger, self.lastResponse, logging.DEBUG)
        if self.lastResponse.status_code != 200:
            logRequest(logger, self.lastRequest, logging.INFO)
            logResponse(logger, self.lastResponse, logging.INFO)
        self._setSession(self.lastResponse)
        return self.lastResponse

    def _setSession(self, response):
        """
        Update the sn cookie from a response.
        Note: This method does not appear to work reliably
        (perhaps because REST is supposed to be stateless).
        """
        if not isinstance(response, requests.Response):
            raise ValueError('argument has incorrect type')
        jsessionid = response.cookies.get('JSESSIONID')
        if jsessionid:
            self.jsessionid = jsessionid
            self.cookies = dict(JSESSIONID=jsessionid) 
    
class Table():

    def __init__(self, sn, name):        
        self.session = sn
        self.name = name
        self.tableurl = sn.url('/api/now/v1/table/' + name)
    
    def _request(self, method, sys_id=None, params=None, data=None):
        return self.session._request(method, self.tableurl, 
            sys_id=sys_id, params=params, data=data)
        
#     def _print_response(self, method, response):
#         return self.session._print_response(method, self.tableurl, response)
    
    def query(self, query=None, fields=None, limit=None):
        """
        Create a Query object
        """
        return Query(self, query=query, fields=fields, limit=limit)
    
    def get(self, sys_id, refLinks=False):
        """
        Retrieve a single record.
        If no corresponding sys_id exists then None is returned.        
        If refLinks is true then each reference field will contain two values:
        'value' (sys_id) and 'link' (URL).
        """
        parms = {}
        if (refLinks):
            parms['sysparm_exclude_reference_link'] = 'false'
        else:
            parms['sysparm_exclude_reference_link'] = 'true'
        self.response = self._request('GET', sys_id, parms)
        if self.response.status_code == 401:
            raise ServiceNowError('Unauthorized\n%s' % (self.response.json()))
        if self.response.status_code == 404:
            return None
        result = self.response.json()['result']
        return result
                        
    def insert(self, record, fields=None):
        """
        Insert a single record.
        fields is a list of fields to be returned.
        If fields==None then return a sys_id.
        Otherwise return return a dict containing specified fields.
        """
        data = json.dumps(record)
        if fields:
            if isinstance(fields, str):
                sysparm_fields = fields
            else:
                sysparm_fields = ','.join(fields)
        else:
            sysparm_fields = 'sys_id'
        params = {'sysparm_fields': sysparm_fields} # values we want returned
        self.response = self._request('POST', params=params, data=data)
        if not 200 <= self.response.status_code <= 299:
            raise ServiceNowError('Status: %s\nHeaders: %s\n%s' % 
                (self.response.status_code, self.response.headers, self.response.json()))
        result = self.response.json()['result']
        if fields:
            return result
        else:
            return result['sys_id']
        
    def update(self, sys_id, fieldvalues):
        """
        Update a single record.
        Pass in a sys_id and a dict of field values.
        """
        data = json.dumps(fieldvalues)
        self._request('PUT', sys_id=sys_id, data=data)
        
    def delete(self, sys_id):
        """
        Delete a single record.
        """
        self._request('DELETE', sys_id=sys_id)
        
    def getChoices(self, element, inactive=False):
        """
        Return a dict of the values of a choice field.
        If inactive==False then only active values will be included.
        Otherwise all values will be included.
        """
        sys_choice = self.session.table('sys_choice')
        query_str = 'name=' + self.name + '^element=' + element
        result = {}
        recs = sys_choice.query(query_str).run()
        for rec in recs:
            if inactive and rec['inactive']=='true':
                pass
            else:
                result[rec['value']] = rec['label']
        if not len(result): self.session.logLastRequest()
        assert len(result), 'getChoices returned no values'
        return result
        
class Query:
     
    def __init__(self, table, query=None, fields=None, limit=None, refLinks=False):
        self.table = table
        self.session = table.session
        self.setQuery(query)
        self.setFields(fields)
        self.setLimit(limit)
        self.setRefLinks(refLinks)
        self.response = None # last response
         
    def setQuery(self, query):
        """
        Specify an encoded query string to restrict the rows that will be returned.
        If query is None then all rows in the table will be returned.
        """
        self.query = query
        return self

    def setFields(self, fields=None):
        """
        Set the list of fields to be returned by the query.
        If fields is None then all fields will be returned.
        """
        if (fields):
            if isinstance(fields, str):
                self.fields = fields
            else:
                self.fields = ','.join(fields)
        else:
            self.fields = None
        return self
                
    def setLimit(self, limit):
        """
        Specify the maximum number of rows to be returned.
        """
        if limit:
            self.limit = str(limit)
        else:
            self.limit = None
        return self
    
    def setRefLinks(self, refLinks=False):
        """
        If refLinks is false then reference fields return sys_id only.
        If refLinks is true then reference fields return sys_id and URL.
        """
        if refLinks:
            self.exclude_ref_links = 'false'
        else:
            self.exclude_ref_links = 'true'
        return self
        
    def run(self):
        """
        This function runs the query. 
        It returns a list of dicts.
        If there are no qualifying records then an empty tuple is returned.
        """
        table = self.table
        params = {}
        if (self.limit):
            params['sysparm_limit'] = self.limit
        if (self.fields):
            params['sysparm_fields'] = self.fields
        if (self.query):
            params['sysparm_query'] = self.query
        if (self.exclude_ref_links):
            params['sysparm_exclude_reference_link'] = self.exclude_ref_links 
        self.response = table._request('GET', params=params)
        if self.response.status_code != 200:
            # table._print_response('GET', self.response)
            return list()
        json = self.response.json()
        return json['result']

