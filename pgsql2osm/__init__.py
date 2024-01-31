#!/usr/bin/python3

import importlib.metadata

__version__=importlib.metadata.version(__package__)
__metadata__=dict(importlib.metadata.metadata(__package__))

from . import pgsql2osm

def main() :
    print('entry point!!!')
    import sys
    print(sys.argv)

