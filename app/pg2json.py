import os
import sys
import psycopg2
import psycopg2.extras
import ujson
import json
import argparse



def pg_connect(**cargs):
    pg_conn = psycopg2.connect(**cargs)
    pg_conn.reset()
    pg_conn.set_session(
        isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
        readonly=True,
        deferrable=True,
        autocommit=True)
    cursor = pg_conn.cursor()
    cursor.execute("set timezone='UTC'")
    cursor.close()
    return pg_conn


def server_side_cursor_fetchall(pg_conn, sql_query, sql_args=None, chunk_size=5000, using='default'):
    sql = 'DECLARE ssc CURSOR FOR {}'.format(sql_query, sql_args)
    sql_fetch_chunk = 'FETCH {} FROM ssc'.format(chunk_size)
    cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cursor.execute('BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')
        cursor.execute(sql)
        try:
            cursor.execute(sql_fetch_chunk)
            while True:
                rows = cursor.fetchall()
                if not rows:
                    break
                for row in rows:
                    yield row
                cursor.execute(sql_fetch_chunk)
        finally:
            cursor.execute('CLOSE ssc')
    finally:
        cursor.close()


def get_pg_tables(pg_conn):
    cursor = pg_conn.cursor()
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    tables = [x[0] for x in cursor.fetchall()]
    return tables


def table2json(pg_conn, output_dir, table):
    path = os.path.join(output_dir, '{}.txt'.format(table))
    print('exporting table {} to json file {}'.format(table, path))
    sql = "select * from \"{}\"".format(table)
    f = open(path, 'w')
    i = 0
    for i, row in enumerate(server_side_cursor_fetchall(pg_conn, sql)):
        try:
            f.write(ujson.dumps(row) + '\n')
        except Exception:
            print('BAD ROW JSON')
#        if i > 0 and i % 1000 == 0:
#            print('wrote {} records'.format(i))
    print('wrote {} records'.format(i))
    f.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='database hostname', type=str, default='localhost')
    parser.add_argument('--port', help='database port', type=int, default=5432)
    parser.add_argument('--password', help='database password', type=str, default='password')
    parser.add_argument('--user', help='database user', type=str, default='postgres')
    parser.add_argument('name', help='database name', type=str)
    parser.add_argument('output_dir', help='output directory for JSON files', type=str)
    args = parser.parse_args()

    db_source = {
        'database': args.name,
        'host': args.host,
        'password': args.password,
        'user': args.user
    }
    if args.port:
        db_source['port'] = args.port

    def connect():
        _pg_conn = pg_connect(**db_source)
        try:
            psycopg2.extras.register_hstore(_pg_conn)
        except:
            pass
        return _pg_conn

    pg_conn = connect()
    tables = get_pg_tables(pg_conn)
    pg_conn.close()

    for table in tables:
        try:
            pg_conn = connect()
            table2json(pg_conn, args.output_dir, table)
        except psycopg2.ProgrammingError as e:
            print('EXCEPTION: {}'.format(e))
        finally:
            pg_conn.close()


if __name__ == '__main__':
    main()

