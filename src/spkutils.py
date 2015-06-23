#!/usr/bin/env python
import argparse


AP = argparse.ArgumentParser(description="Spark Utilities")

AP.add_argument('action')


def xargs():
    pass


def sql(argv):
    ap = argparse.ArgumentParser(description="Spark Utilities")
    ap.add_argument('sqlfile')
    ap.add_argument('-x', nargs='+', type=str)
    ap.parse_args(argv)



