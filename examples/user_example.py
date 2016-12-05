from spark_sql_job import SparkSQLJob


class Job(SparkSQLJob):
    app_name = 'Example Query User Table'
    load_tables = [
        'pictorlabs_auth_user'
    ]

    def task(self):
        sql = """
        select     u.id as id,
                   u.username as username,
                   u.email as email,
                   u.last_login as last_login_time_stamp,
                   datetime_from_timestamp(u.last_login) as last_login
        from       pictorlabs_auth_user u
        order by   u.last_login desc
        """

        users_rdd = self.sql_context.sql(sql)
        self.write_local_output(users_rdd.collect())


if __name__ == '__main__':
    Job().run()
