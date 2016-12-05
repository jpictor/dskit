import os
import sys
import requests
import getopt
import time
import ujson as json
import yaml
import glob
from fnmatch import fnmatchcase
from datetime import date, timedelta, datetime
from requests import ConnectionError
from requests.exceptions import Timeout
from requests.auth import HTTPBasicAuth


def print_yaml(r):
    print yaml.dump(r, Dumper=yaml.SafeDumper, default_flow_style=False)


def print_stderr(m):
    sys.stderr.write('{}\n'.format(m))


class OperationalError(Exception):
    pass


MAX_REQUEST_RETRIES = 10
RETRY_WAIT_SEC = 5


def es_request(method, url, **kwargs):
    kwargs['verify'] = False

    ## make the call
    attempt = 0
    result = None
    while True:
        attempt += 1
        try:
            result = method(url, **kwargs)
            break
        except Timeout:
            print_stderr('---- TIMEOUT ERROR  %s' % url)
            break
        except ConnectionError:
            if attempt >= MAX_REQUEST_RETRIES:
                print_stderr('---- MAX RETRIES  %s' % url)
                raise
            else:
                print_stderr('---- cannot connect, attempt %d of %s...' % (attempt, MAX_REQUEST_RETRIES))
                time.sleep(RETRY_WAIT_SEC)

    if result.status_code != 200:
        raise OperationalError('{} returned {}'.format(url, result.status_code))
    return result


def date_from_time_series_date_string(s):
    year, month, day = s.split('.')
    year = int(year)
    month = int(month)
    day = int(day)
    return date(year, month, day)


class TimeSeriesIndexParser(object):
    def __init__(self, prefix):
        self.prefix = prefix
        self.start = len(self.prefix)
        self.end = self.start + 10

    def date(self, index):
        return date_from_time_series_date_string(index[self.start:self.end])

    def index(self, _date):
        return '{0}{1:04d}.{2:02d}.{3:02d}'.format(self.prefix, _date.year, _date.month, _date.day)

    def filename(self, _date):
        return '{0}{1:04d}.{2:02d}.{3:02d}.txt'.format(self.prefix, _date.year, _date.month, _date.day)


def get_indexes(elastic_url, pattern=None):
    response = es_request(requests.get, '{0}/_aliases'.format(elastic_url))
    doc = json.loads(response.text)
    if pattern is not None:
        index_names = [k for k in doc if fnmatchcase(k, pattern)]
    else:
        index_names = doc.keys()
    index_names.sort()
    return index_names


def get_time_series_indexes_in_range(elastic_url, index_prefix, start_date, end_date):
    indexes = get_indexes(elastic_url, index_prefix+'*')
    tsp = TimeSeriesIndexParser(index_prefix)
    indexes_in_date_range = []
    for index in indexes:
        try:
            d = tsp.date(index)
        except ValueError:
            continue
        if d >= start_date and d <= end_date:
            indexes_in_date_range.append(index)
    indexes_in_date_range.sort()
    return indexes_in_date_range


def get_time_series_files_in_range(path, index_prefix, start_date, end_date):
    data_files = glob.glob(os.path.join(path, '{}*'.format(index_prefix)))
    print data_files
    tsp = TimeSeriesIndexParser(index_prefix)

    files_in_date_range = []
    for data_file in data_files:
        if os.path.getsize(data_file) == 0:
            continue
        basename = os.path.basename(data_file)
        filename, ext = os.path.splitext(basename)

        try:
            d = tsp.date(filename)
        except ValueError:
            continue

        if d >= start_date and d <= end_date:
            files_in_date_range.append(basename)

    files_in_date_range.sort()
    return files_in_date_range



SCAN_DOCS_QUERY = {'match_all': {}}


def scan_docs(elastic_url, index, chunk_size=5000, scroll_ttl='5m'):
    query = {
        'size': chunk_size,
        'query': SCAN_DOCS_QUERY
    }

    url = '{0}/{1}/_search?search_type=scan&scroll={2}'.format(elastic_url, index, scroll_ttl)
    result = es_request(requests.get, url, data=json.dumps(query))
    doc = json.loads(result.text)
    scroll_id = doc['_scroll_id']
    total_docs = doc['hits']['total']

    def doc_iter(_scroll_id):
        url = '{0}/_search/scroll?scroll={1}'.format(elastic_url, scroll_ttl)
        while True:
            r = es_request(requests.get, url, data=_scroll_id)
            doc = json.loads(r.text)
            num_hits = len(doc['hits']['hits'])
            hits = doc['hits']['hits']
            _scroll_id = doc['_scroll_id']
            for hit in hits:
                yield hit
            if num_hits == 0:
                break

    return total_docs, doc_iter(scroll_id)


def store_index(elastic_url_src, src_index, dst_path):
    chunk_size = 5000
    num_docs, doc_iter = scan_docs(elastic_url_src, src_index, chunk_size=chunk_size)

    def report_percent_complete(num_docs_written):
        percent_complete = 100.0 * float(num_docs_written) / float(num_docs)
        print_stderr('{:.1f}% complete'.format(percent_complete))

    dst_file = open(dst_path, 'w')

    for i, doc in enumerate(doc_iter):
        dst_file.write(json.dumps(doc) + '\n')
        if ((i+1) % chunk_size) == 0:
            report_percent_complete(i+1)


def copy_time_series_operations_iter(src_elastic_url, src_index_prefix, start_date, end_date):
    start_date = date_from_time_series_date_string(start_date)
    end_date = date_from_time_series_date_string(end_date)
    src_indexes = get_time_series_indexes_in_range(src_elastic_url, src_index_prefix, start_date, end_date)
    src_tsp = TimeSeriesIndexParser(src_index_prefix)
    for src_index in src_indexes:
        yield (src_elastic_url, src_index, src_tsp.date(src_index))


def list_indexes_cmd(elastic_url):
    for index in get_indexes(elastic_url):
        print_stderr(index)


def store_index_cmd(src_elastic_url, src_index, dst_path):
    store_index(src_elastic_url, src_index, dst_path)


def store_time_series_index_range_cmd(src_elastic_url, src_index_prefix, dst_dir, dst_index_prefix, start_date, end_date):
    dst_tsp = TimeSeriesIndexParser(dst_index_prefix)
    copy_ops_iter = copy_time_series_operations_iter(src_elastic_url, src_index_prefix, start_date, end_date)

    for _src_elastic_url, src_index, src_date in copy_ops_iter:
        dst_path = os.path.join(dst_dir, dst_tsp.filename(src_date))
        print_stderr('store_time_series_index_range src={0} dst={1}'.format(src_index, dst_path))
        store_index(src_elastic_url, src_index, dst_path)


_USAGE = """\
es2json.py [-q] <command> <command-args>
Generally useful Elasticsearch utilities.

Options
  -q --query=
      Add an Elasticsearch query string to filter.

Commands:
  list_indexes
      <elastic-url>

  store_index
      <src-elastic-url> <src-index-prefix> <dst-file-path>
      This is a backup utility.  Copies the data in the elasticsearch index to a local file.

  store_time_series_index_range
      <src-elastic-url> <src-index-prefix> <dst-dir> <dst-index-prefix> <start-date> <end-date>
      This is a backup utility.  It copies the time series indexes to local files.
"""


def usage():
    print_stderr(_USAGE)


def main(cmd_args=None):
    if cmd_args is None:
        cmd_args = sys.argv[1:]

    try:
        opts, args = getopt.getopt(cmd_args, "hq:f", ["help", "query=", "full"])
    except getopt.GetoptError as err:
        usage()
        raise SystemExit(-1)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            raise SystemExit(-1)
        elif o in ('-q', '--query'):
            global SCAN_DOCS_QUERY
            SCAN_DOCS_QUERY = { "constantScore" : { "filter" : { "query" : { "query_string" : { "query" : a } } } } }
        else:
            assert False, "unhandled option"

    try:
        command = args[0]
        command_args = args[1:]
    except IndexError:
        usage()
        raise SystemExit(-1)

    elif command == 'copy_time_series_index_range':
        copy_time_series_index_range_cmd(
            command_args[0], command_args[1], command_args[2], command_args[3],
            command_args[4], command_args[5])

    elif command == 'store_time_series_index_range':
        store_time_series_index_range_cmd(
            command_args[0], command_args[1], command_args[2], command_args[3],
            command_args[4], command_args[5])

    elif command == 'restore_time_series_index_range':
        restore_time_series_index_range_cmd(
            command_args[0], command_args[1], command_args[2], command_args[3],
            command_args[4], command_args[5])

    elif command == 'list_indexes':
        list_indexes_cmd(command_args[0])

    elif command == 'delete_index':
        delete_index(command_args[0], command_args[1])

    elif command == 'set_number_of_replicas':
        set_number_of_replicas_cmd(command_args[0], command_args[1], int(command_args[2]))

    else:
        usage()
        raise SystemExit(-1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print_stderr(' ** terminated')
        pass
