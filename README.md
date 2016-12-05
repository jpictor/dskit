# Spark SQL Data Science Kit

## Introduction

This data science "kit" was built to help data science teams get started with exploratory
data analysis and algorithm development with a product architected with a service or
microservice architecture.  With such architectures, data is distributed across a number
of isolated databases, SQL and non-SQL.  This presents a challenge to developing analytics
that require complex joins of data across the service databases.  This data science toolkit
solves this problem by providing programs to dump service databases as JSON row files,
and then use those files as a unified Spark SQL data where big-data queries and map/reduce
algorithms can be applied.

### Data Dump Programs

Let's say you have the following service databases: identity, notification, groups,
and chat.  Also, some of the databases are MySQL and some are Postgres.  Use the data
dump programs `pg2json` and `my2json` to export the data for analysis.

```bash
$ ./manage.py pg2json --host=localhost --user=postgres --password=foo identity /disk/data/identity
$ ./manage.py pg2json --host=localhost --user=postgres --password=foo notification /disk/data/notification
$ ./manage.py my2json --host=localhost --user=root --password=foo groups /disk/data/groups
$ ./manage.py my2json --host=localhost --user=root --password=foo chat /disk/data/chat
```

### Joining Data with SparkSQLJob and Spark SQL

The `SparkSQLJob` class is a utility class for loading the exported JSON data directory created
above into a Spark SQL context.  The data directory is arranged as `<service>/<table>.txt` files
and registered into the SQL context as `<service>_<table>`.  Partitioned files are supported using
the convention `<service>/<table>-<partition>.txt`.

Here is an example job which joins the data form the identity and notification services to
generate a count the number of notifications per user:

```python
from spark_sql_job import SparkSQLJob

class Job(SparkSQLJob):
    app_name = 'Messages per user'
    load_tables = [
        'identity_user',
        'notification_message'
    ]

    def task(self):
        sql = """
        select     u.id as id,
                   u.username as username,
                   u.email as email,
                   max(datetime_from_timestamp(u.last_login)) as last_login,
                   count(m.id) as message_count
        from       identity_user u
        join       notification_message m on m.user_id = u.id
        group by   u.id, u.username, u.email
        order by   message_count desc
        """

        users_rdd = self.sql_context.sql(sql)
        self.write_local_output(users_rdd.collect())

if __name__ == '__main__':
    Job().run()
```

This example is run with the command:

```bash
$ ./manage.sh submit examples/messages_per_user.py --output-type=csv --output-path=user_messages.csv /disk/data/
```

## Installation Instructions

```bash
$ ./manage.sh build_all
```

## Run Example

There is sample data in the data/ directory from a Django database: data/pictorlabs/auth_user.txt.
This data was exported from the database using the pg2json command.  The auth_user.txt data contains
the rows from the auth_user database table in JSON format one row per line.  Each line is a JSON
hash with the keys the database column names.

The sample run a Spark SQL query on the sample data and outputs a CSV file.  Run the sample:

The example `user_example.py` uses the SparkSQLJob class to generate a Spark SQL context that
is pre-loaded with the JSON row data in the data directory.  The SparkSQLJob loader uses the directory
and file names to register the data into a Spark SQL database.  The JSON record file
`pictorlabs/auth_user.txt` is registered as the database table `pictorlabs_auth_user`.

Run the example using the submit command:

```bash
$ ./manage.sh submit examples/user_example.py --output-type=csv --output-path=users.csv data/
```

After the sample runs, the `users.csv` contains the sample output.
