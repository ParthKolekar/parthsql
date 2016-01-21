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

        DATABASE.print_contents()

        command = raw_input(">>> ")
        for statement in sqlparse.parse(command):
            type = statement.tokens[0]
            if str(type).lower() == "drop":
                if str(statement.tokens[2]).lower() == "table":
                    tablename = str(statement.tokens[4])
                    table = DATABASE.get_table(tablename)
                    table.rows = []
                    table.store_contents()
                    DATABASE.delete_table(tablename)
                    DATABASE.store_contents()
                else:
                    raise Exception(
                        "Invalid Syntax of DROP TABLE tablename"
                    )
            elif str(type).lower() == "truncate":
                if str(statement.tokens[2]).lower() == "table":
                    tablename = str(statement.tokens[4])
                    table = DATABASE.get_table(tablename)
                    table.rows = []
                    table.store_contents()
                else:
                    raise Exception(
                        "Invalid Syntax of TRUNCATE TABLE tablename"
                    )

if __name__ == "__main__":
    main()
