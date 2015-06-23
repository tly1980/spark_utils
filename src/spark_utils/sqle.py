#!/usr/bin/env python
import argparse

AP = argparse.ArgumentParser(description="Spark SQL executor")
AP.add_argument('sql', nargs='+')
AP.add_argument('-x', nargs='+', type=str, default=[], help="Override the permissions.")
AP.add_argument('--show', action='store_true', default=False, help="Dry run")
AP.add_argument('--dry', action='store_true', default=False, help="Dry run")
AP.add_argument('--no-hive', action='store_true', default=False, help="Use SqlContext rather than HiveContext.")


class DrySQLContext(object):
    def sql(self, stmt):
        print "Dry run: " + stmt


class SQLRunner(object):
    def __init__(self, sql_context, show):
        self.stmt_idx = 0
        self.sql_context = sql_context
        self.show = show

    def run_sql(self, sql_stmt):
        df = self.sql_context.sql(sql_stmt)
        if df and self.show:
            df.show()
        return df

def x_params(x_pairs):
    return dict([ x.split('=') for x in x_pairs])

def x_sql_content(sql_str, params):
    if sql_str[0] in ['.', '/']:
        with open(sql_str) as f:
            sql_str = f.read()

    return sql_str.format(**params)

def x_sql_stmt(sql_contents):
    for sql in sql_contents:
        for stmt in sql.split(';'):
            stmt2 = stmt.strip()
            if stmt2:
                yield stmt2


def main(args):
    if not args.dry:
        from pyspark import SparkContext
        from pyspark.sql import SQLContext, HiveContext

        sc = SparkContext(appName=__file__)
        if args['no-hive']:
            sql_context = SQLContext(sc)
        else:
            sql_context = HiveContext(sc)
    else:
        sql_context = DrySQLContext()

    runner = SQLRunner(sql_context, args.show)
    params = x_params(args.x)

    sql_cnts = [ x_sql_content(s, params) for s in args.sql ]

    for stmt in x_sql_stmt(sql_cnts):
        runner.run_sql(stmt)


if __name__ == '__main__':
    main(AP.parse_args())
