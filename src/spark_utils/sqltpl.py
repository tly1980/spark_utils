import argparse
import yaml
import os

import jinja2

AP = argparse.ArgumentParser(description="SQL template render")
AP.add_argument('src', type=str)
AP.add_argument('dst', type=str)
AP.add_argument('-v','-var', type=str)
AP.add_argument('-vf', '--varfile', type=str)
AP.add_argument('--dry', action='stroe_true')
AP.add_argument('--template_dir', type=str, nargs='+')


def xargs():
    pass


def template_env(*tpl_folder_lst):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(tpl_folder_lst)
    )
    return env

def load_template(args):
    if not args.tpldir:
        tenv = template_env(args.tpldir)
    else:
        tenv = template_env(os.getcwd())


def main(args):
    pass

def call(argv):
    args = AP.parse_args(argv)
    main(args)

if __name__ == '__main__':
    main(AP.parse_args())
