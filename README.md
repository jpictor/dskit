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

## Usage
