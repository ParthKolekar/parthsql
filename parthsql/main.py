# -*- coding: utf-8 -*-

import sqlparse
import itertools

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
                            "Invalid Syntax of INSERT INTO t VALUES (v...)"
                        )
                else:
                    raise Exception(
                        "Invalid Syntax of INSERT INTO t VALUES (v...)"
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
                    DATABASE.create_table_raw(
                        tablename=tablename,
                        columns=column_list[:],
                    )
                    DATABASE.store_contents()
            elif str(type).lower() == "select":
                col_list_or_single = statement.tokens[2]
                if "," not in str(col_list_or_single):
                    if str(col_list_or_single) == "*":
                        column_list = ['*']
                    else:
                        column_list = [str(col_list_or_single)]
                else:
                    column_list = map(
                        lambda x: str(x),
                        col_list_or_single.get_identifiers()
                    )
                if str(statement.tokens[4]).lower() == "from":
                    tab_list_or_single = statement.tokens[6]
                    if "," not in str(tab_list_or_single):
                        table_list = [str(tab_list_or_single)]
                    else:
                        table_list = map(
                            lambda x: str(x),
                            tab_list_or_single.get_identifiers()
                        )
                    cross_columns = reduce(
                        lambda x, y: x + y,
                        map(
                            lambda x: DATABASE.get_table(
                                x
                            ).get_column_list_prefixed(),
                            table_list
                        )
                    )
                    cross_table = parthsql.Table(
                        name="temp",
                        columns=cross_columns,
                        rows=[]
                    )
                    for i in itertools.product(
                        *map(
                            lambda x: DATABASE.get_table(x).get_all_rows(),
                            table_list
                        )
                    ):
                        cross_table.put_row_raw(
                            reduce(
                                lambda x, y: x + y,
                                i
                            )
                        )
                    if "*" in column_list:
                        cross_table.print_contents()
                    else:
                        temp_list = []
                        for i in column_list:
                            temp_list.append(cross_table.get_column(i))
                        for i in zip(*(temp_list)):
                            print "\t\t".join(map(str, i))
                else:
                    raise Exception(
                        "Invalid Syntax of SELECT c... FROM t... WHERE k = v"
                    )
            else:
                raise Exception(
                    "Unsupported Operation"
                )


if __name__ == "__main__":
    main()
