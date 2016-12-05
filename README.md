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

## Usage

