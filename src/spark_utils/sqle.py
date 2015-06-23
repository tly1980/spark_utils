#!/usr/bin/env python
import argparse
import logging
import os


AP = argparse.ArgumentParser(description="Spark SQL executor")
AP.add_argument('src', nargs='+')
AP.add_argument('-x', nargs='+', type=str, default=[], help="Override the permissions.")
AP.add_argument('--quiet', action='store_false', dest='show', help='Not show')
AP.add_argument('--dry', action='store_true', default=False, help="Dry run")
AP.add_argument('--nohive', action='store_true', default=False, help="Use SqlContext rather than HiveContext.")


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)-2s %(levelname)-2s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)


class SQLRunner(object):
    def __init__(self, sql_context, show):
        self.logger = logging.getLogger(
            self.__class__.__name__)
        self.stmt_idx = 0
        self.sql_context = sql_context
        self.show = show

    def run_sql(self, sql_stmt):
        sql, coming_from = sql_stmt
        msg = 'exec stmt[idx=%s, src=%s]: "%s"' % (self.stmt_idx, coming_from, sql)
        self.stmt_idx += 1
        if not self.sql_context:
            msg = '[DRY] ' + msg

        self.logger.info(msg)

        if not self.sql_context:
            return 

        df = self.sql_context.sql(sql)
        if df and self.show:
            df.show()
        return df


class SQLRender(object):
    def __init__(self, srcs, params):
        self.srcs = srcs
        self.params = params

    def next_stmt(self):
        for s in self.srcs:
            for stmt in self.load(s):
                yield stmt

    def load(self, src):
        if src[0] in ['.', '/']:
            # this means src could be either a file or directory
            if not os.path.isdir(src):
                for stmt in x_file(src, self.params):
                    yield (stmt, src)
            else:
                for root, dirs, files in os.walk(src):
                    for f in sorted(files):
                        path = os.path.join(root, f)
                        for stmt in x_file(path, self.params):
                            yield (stmt, path)

        else:
            src = src.format(**self.params)
            for stmt in x_sql_stmt(src):
                yield stmt, '<FROM ARGS>'


def x_sql_stmt(cnt):
    for stmt in cnt.split(';'):
        stmt2 = stmt.strip()
        if stmt2:
            yield stmt2

def x_file(src_file, params):
    with open(src_file) as f:
        cnt = f.read().format(**params)
        cnt = cnt.strip()
        return x_sql_stmt(cnt)


def x_params(x_pairs):
    return dict([ x.split('=') for x in x_pairs])

def x_sql_content(sql_str, params):
    if sql_str[0] in ['.', '/']:
        with open(sql_str) as f:
            sql_str = f.read()

    return sql_str.format(**params)



def main(args):
    if not args.dry:
        from pyspark import SparkContext
        from pyspark.sql import SQLContext, HiveContext

        sc = SparkContext(appName=__file__)
        if args.nohive:
            logging.info('nohive is set')
            logging.info('using SQLContext')
            sql_context = SQLContext(sc)
        else:
            logging.info('using HiveContext')
            sql_context = HiveContext(sc)
    else:
        sql_context = None

    runner = SQLRunner(sql_context, args.show)
    params = x_params(args.x)

    render = SQLRender(args.src, params)

    for stmt in render.next_stmt():
        runner.run_sql(stmt)


if __name__ == '__main__':
    main(AP.parse_args())
