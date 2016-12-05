import time
import sys
from collections import defaultdict
import os
import argparse
import glob
import pytz
import datetime
from itertools import tee, izip, combinations, chain
import isodate
import ujson as json
import numpy as np
import logging
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import IDF
from pyspark.sql import SQLContext, Row
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import IntegerType, ArrayType, StringType, LongType, DoubleType, BooleanType, StructType, StructField
from pyspark.sql.types import DateType, TimestampType
from udf_functions import datetime_from_isodate, timestamp_from_isodate, datetime_from_timestamp


logger = logging.getLogger('')


class SparkSQLJob(object):

    app_name = 'default name'
    load_tables = None

    def __init__(self):
        self.app_label = self.app_name.lower().replace(' ', '_')
        self.app_path = os.path.join(os.environ['SERVICE_ROOT'], 'app')
        self.sc = None
        self.conf = None
        self.sql_context = None
        self.args = None
        self.data_dir = None

    def add_args(self, parser):
        """
        Implement in derived class to add arguments.
        """

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('data_dir', help='directory of data dir with JSON files', type=str)
        parser.add_argument('--output-type', type=str, dest='output_type', default='text')
        parser.add_argument('--output-path', type=str, dest='output_path', default='./output.csv')
        self.add_args(parser)
        self.args = parser.parse_args()
        self.data_dir = self.args.data_dir

    def create_spark_conf(self):
        ## set up Spark SQL context
        self.conf = SparkConf().setAppName(self.app_name)

    def create_spark_context(self):
        self.sc = SparkContext(conf=self.conf)
        ## pretty funny way of pulling the Spark logger
        global logger
        log4jLogger = self.sc._jvm.org.apache.log4j
        logger = log4jLogger.LogManager.getLogger(__name__)
        logger.info("*** pyspark script logger initialized")
        ## add extra python lib files
        def add_py_file(filename):
            self.sc.addPyFile(os.path.join(self.app_path, filename))
        add_py_file('spark_sql_job.py')
        add_py_file('csv_unicode.py')
        add_py_file('udf_functions.py')
        self.add_python_files()

    def add_python_files(self):
        """
        Implement in derived class to add more python files.
        """

    def create_spark_sql_context(self):
        self.sql_context = SQLContext(self.sc)
        self.register_default_sql_proceedures()
        self.register_sql_proceedures()

    def register_default_sql_proceedures(self):
        self.sql_context.registerFunction('timestamp_from_isodate', timestamp_from_isodate, IntegerType())
        self.sql_context.registerFunction('datetime_from_isodate', datetime_from_isodate, TimestampType())
        self.sql_context.registerFunction('datetime_from_timestamp', datetime_from_timestamp, TimestampType())

    def register_sql_proceedures(self):
        """
        Implement in derived method to add your custom SQL proceedures.
        """

    def load_directory_as_db(self, dir_path, db_name):
        """
        Loads a directory of .txt files containg JSON rows into the Spark
        SQL context as tables named '<dir-name>_<file-name>'.
        Filenames with a hyphen are treated as paritioned files to be
        combined into one table using the name before the hyphen.
        For example: logs-201601.txt, logs-201602.txt would be combined
        into a 'logs' table.
        """
        load_dir = os.path.join(self.data_dir, dir_path)
        data_files = glob.glob(os.path.join(load_dir, '*.txt'))
        file_groups = defaultdict(list)
        for path in data_files:
            path_noext, _  = os.path.splitext(path)
            filename_noext = os.path.basename(path_noext)
            i = filename_noext.find('-')
            if i == -1:
                table_name = filename_noext
            else:
                table_name =  filename_noext[:i]
            file_groups[table_name].append(path)

        for table_name in sorted(file_groups.keys()):
            register_name = '{}_{}'.format(db_name, table_name)
            data_files = file_groups[table_name]
            logger.info('REGISTERING {}:{}'.format(register_name, data_files))
            data_files = filter(lambda x: os.path.getsize(x) > 0, data_files)
            if self.load_tables and register_name not in self.load_tables:
                continue
            jdb = self.sql_context.read.json(data_files)
            jdb.printSchema()
            jdb.registerTempTable(register_name)

    def load_data_dir(self):
        data_dirs = os.listdir(self.data_dir)
        logger.info('*** DATA-DIR:{} DATA-DIRS:{}'.format(self.data_dir, data_dirs))
        for dirname in data_dirs:
            self.load_directory_as_db(dirname, dirname)

    def write_local_output(self, row_iter):
        """
        utility to write local output file
        """
        import csv_unicode

        if self.args.output_type == 'text':
            for row in row_iter:
                print row
        else:
            with open(self.args.output_path, 'wb') as csv_file:
                writer = csv_unicode.UnicodeWriter(csv_file)
                for row in row_iter:
                    writer.writerow(row)

    def run(self):
        self.parse_args()
        start_dt = datetime.datetime.utcnow()
        try:
            self.create_spark_conf()
            self.create_spark_context()
            self.create_spark_sql_context()
            self.register_default_sql_proceedures()
            self.load_data_dir()
            self.task()
            end_dt = datetime.datetime.utcnow()
            seconds = long((end_dt - start_dt).total_seconds())
        except Exception as e:
            end_dt = datetime.datetime.utcnow()
            seconds = long((end_dt - start_dt).total_seconds())
            raise

    def task(self):
        """
        Implement in derived class to do stuff.
        """
