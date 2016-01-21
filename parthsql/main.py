# -*- coding: utf-8 -*-

import sqlparse

from . import parthsql

DATABASE = parthsql.Database(name="Default Database", tables=[])


def usage():
    """
        How to use the commandline parser.
    """
    return "parthsql "


def main():
    """
        The main loop for the commandline parser.
    """

    DATABASE.load_contents()

    continue_flag = False

    while not continue_flag:
        # TODO Parse commands
        # TODO Run main loop
        command = raw_input(">>> ")


if __name__ == "__main__":
    main()
