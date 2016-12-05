import os
import sys
import MySQLdb
import MySQLdb.cursors
import ujson as json
import argparse
import datetime


def my_connect(**cargs):
    cargs['charset'] = 'utf8'
    cargs['cursorclass'] = MySQLdb.cursors.SSDictCursor
    conn = MySQLdb.connect(**cargs)
    conn.autocommit(False)
    conn.set_character_set('utf8')
    return conn


def new_cursor(conn, cursor_class=MySQLdb.cursors.SSDictCursor):
    cursor = conn.cursor(cursor_class)
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')
    return cursor


def server_side_cursor_fetchall(conn, sql_query, sql_args=None, chunk_size=5000, using='default'):
    cursor = new_cursor(conn)
    try:
        cursor.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT')
        print sql_query
        cursor.execute(sql_query)
        try:
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    yield row
        finally:
            cursor.execute('ROLLBACK')
    finally:
        cursor.close()


def get_tables(conn):
    cursor = new_cursor(conn, MySQLdb.cursors.Cursor)
    cursor.execute("show tables")
    tables = [x[0] for x in cursor.fetchall()]
    cursor.close()
    return tables


def table2json(conn, output_dir, table):
    path = os.path.join(output_dir, '{}.txt'.format(table))
    print('exporting table {} to json file {}'.format(table, path))
    sql = u"select * from `{}`".format(table)
    f = open(path, 'w')
    i = 0

    def json_dump(r):
        try:
            return json.dumps(r)
        except Exception:
            for k, v in r.items():
                if isinstance(v, datetime.datetime) or isinstance(v, datetime.date):
                    r[k] = v.isoformat()
                elif isinstance(v, str):\
                    r[k] = unicode(v, 'utf-8', errors='ignore')
            return json.dumps(r)

    for i, row in enumerate(server_side_cursor_fetchall(conn, sql)):
        try:
            f.write(json_dump(row) + '\n')
        except Exception:
            print(u'BAD ROW JSON: {}'.format(row))
            raise
#        if i > 0 and i % 1000 == 0:
#            print('wrote {} records'.format(i))
    print('wrote {} records'.format(i))
    f.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='database hostname', type=str, default='localhost')
    parser.add_argument('--port', help='database port', type=int, default=3306)
    parser.add_argument('--password', help='database password', type=str, default='')
    parser.add_argument('--user', help='database user', type=str, default='root')
    parser.add_argument('name', help='database name', type=str)
    parser.add_argument('output_dir', help='output directory for JSON files', type=str)
    args = parser.parse_args()

    db_source = {
        'db': args.name,
        'host': args.host,
        'passwd': args.password,
        'user': args.user
    }
    if args.port:
        db_source['port'] = args.port

    def connect():
        _conn = my_connect(**db_source)
        return _conn

    conn = connect()
    tables = get_tables(conn)
    conn.close()

    for table in tables:
        try:
            conn = connect()
            table2json(conn, args.output_dir, table)
        except MySQLdb.Error as e:
            print('EXCEPTION: {}'.format(e))
        finally:
            conn.close()


if __name__ == '__main__':
    main()
