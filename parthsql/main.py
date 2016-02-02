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

        for stmnt_unformated in sqlparse.parse(command):
            statement = sqlparse.parse(
                sqlparse.format(
                    str(
                        stmnt_unformated
                    ),
                    reindent=True
                )
            )[0]
            type = statement.tokens[0]
            print statement.tokens
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
                        "Invalid Syntax of DROP TABLE t"
                    )
            elif str(type).lower() == "truncate":
                if str(statement.tokens[2]).lower() == "table":
                    tablename = str(statement.tokens[4])
                    table = DATABASE.get_table(tablename)
                    table.rows = []
                    table.store_contents()
                else:
                    raise Exception(
                        "Invalid Syntax of TRUNCATE TABLE t"
                    )
            elif str(type).lower() == "delete":
                if str(statement.tokens[2]).lower() == "from":
                    tablename = str(statement.tokens[4])
                    table = DATABASE.get_table(tablename)
                    whereclause = statement.tokens[6]
                    if str(whereclause.tokens[0]).lower() == "where":
                        comparison = whereclause.tokens[2]
                        key = str(comparison.tokens[0])
                        value = int(str(comparison.tokens[4]))
                        table.delete_row(key, value)
                        table.store_contents()
                    else:
                        raise Exception(
                            "Invalid Syntax of DELETE FROM t where k = v"
                        )

                else:
                    raise Exception(
                        "Invalid Syntax of DELETE FROM t WHERE k = v"
                    )
            elif str(type).lower() == "insert":
                if str(statement.tokens[2]).lower() == "into":
                    tablename = str(statement.tokens[4])
                    table = DATABASE.get_table(tablename)
                    if str(statement.tokens[6]).lower() == "values":
                        parenthesis = statement.tokens[8]
                        idlist = parenthesis.tokens[1]
                        values_list = map(
                            lambda x: int(str(x)),
                            idlist.get_identifiers()
                        )
                        table.put_row_raw(values_list)
                        table.store_contents()
                    else:
                        raise Exception(
                            "Invalid Syntax of INSERT INTO t VALUES (v,v,v...)"
                        )
                else:
                    raise Exception(
                        "Invalid Syntax of INSERT INTO t VALUES (v,v,v...)"
                    )
            elif str(type).lower() == "create":
                if str(statement.tokens[2]).lower() == "table":
                    sublist = list(statement.tokens[4].get_sublists())
                    tablename = str(sublist[0])
                    garbage = str(sublist[1])
                    column_list = map(
                        lambda x: x.strip(" ()",).split()[0],
                        garbage.split(",")
                    )
                    DATABASE.tables.append(
                        parthsql.Table(
                            name=tablename,
                            columns=column_list[:],
                            rows=[]
                        )
                    )
                    DATABASE.store_contents()


if __name__ == "__main__":
    main()
