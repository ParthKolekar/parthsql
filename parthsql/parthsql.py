# -*- coding: utf-8 -*-

import os
import itertools
METADATA_FILE = "metadata.txt"


def make_row(columns, values):
    return dict(
        zip(
            columns,
            map(
                lambda x: int(x),
                values
            )
        )
    )


class Database(object):
    def __init__(self, name, tables):
        """
            Class representation for a database object.
        """
        self.name = name
        self.tables = tables

    def load_contents(self):
        """
            Loads contents of the tables into database.
        """
        with open(METADATA_FILE) as f:
            lines = f.readlines()

        lines = map(lambda x: x.strip(), lines)

        exclude_strings = ['<begin_table>', '<end_table>']

        list_of_databases_and_columns = filter(
            lambda x: not x[0] in exclude_strings, [
                list(value) for key, value in itertools.groupby(
                    lines,
                    lambda x: x in exclude_strings
                )
            ]
        )

        for iterator in list_of_databases_and_columns:
            self.create_table_raw(
                tablename=iterator[0],
                columns=iterator[1:][:],
            )

        for i in self.tables:
            i.load_contents()

    def store_contents(self):
        """
            Stores the contents of tables into file.
        """
        string_buffer = os.linesep.join(
            map(
                lambda x: os.linesep.join(
                    ["<begin_table>"] + [x.name] + x.columns + ["<end_table>"]
                ),
                self.tables
            )
        )

        with open(METADATA_FILE, "w") as f:
            f.write(string_buffer)

        for i in self.tables:
            i.store_contents()

    def create_table(self, table):
        """
            Adds table to Database
        """
        self.tables.append(table)

    def create_table_raw(self, tablename, columns):
        self.tables.append(
            Table(
                name=tablename,
                columns=columns,
                rows=[]
            )
        )

    def delete_table(self, tablename):
        """
            Deletes a table from the database.
        """
        self.tables = filter(lambda x: x.name != tablename, self.tables)

    def get_table(self, tablename):
        """
            Returns the table whoose name is tablename.
        """
        temp = filter(lambda x: x.name == tablename, self.tables)
        if temp == list():
            raise Exception("No such table")
        return temp[0]

    def print_contents(self):
        print os.linesep.join(
            map(
                lambda x: x.name,
                self.tables
            )
        )

    def __str__(self):
        """
            String representation of a database.
        """
        return "%s" % (self.name)


class Table(object):
    """
        A table representaion for database.
    """
    def __init__(self, name, columns, rows):
        """
            Class representaion for a table object.

            Rows are dictionary objects which contain key value pairs for
            columns.
        """
        self.name = name
        self.columns = columns
        self.rows = rows

    def get_column_list_prefixed(self):
        """
            Returns a list of columns
        """
        return map(
            lambda x: ".".join([self.name, x]),
            self.columns
        )

    def get_column(self, column):
        """
            Return the values having of column.
        """
        if "(" in str(column):
            temp_list = column.split("(")
            key = temp_list[1].strip("()")
            func = temp_list[0].lower()
        else:
            key = column
            func = None

        if "." not in key:
            for i in self.columns:
                if i.split(".")[1] == key:
                    key = i

        col = map(
            lambda x: x.get(
                key
            ),
            self.rows
        )

        if func is not None:
            if func == "sum":
                return [sum(col)]
            elif func == "max":
                return [max(col)]
            elif func == "min":
                return [min(col)]
            elif func == "avg":
                return [sum(col) / float(len(col))]
            elif func == "count":
                return [len(col)]
            elif func == "distinct":
                return list(set(col))
            else:
                raise Exception(
                    "Unknown function called on column"
                )

        return col

    def get_all_rows(self):
        """
            Returns all values from the table.
        """
        return map(
            lambda x: x.values(),
            self.rows
        )

    def put_row(self, row):
        """
            Adds a row to the table.
        """
        self.rows.append(row)

    def put_row_raw(self, row):
        """
            Adds a raw raw by first making it to a proper row.
        """
        self.put_row(make_row(self.columns, row))

    def delete_row(self, key, value):
        """
            Deletes the rows where key = value.
        """
        self.rows = filter(lambda x: x.get(key) != value, self.rows)

    def invert_delete_row(self, key, value):
        """
            Inverts delete_row and returns the rows where key = value
        """
        self.rows = filter(lambda x: x.get(key) == value, self.rows)

    def load_contents(self):
        """
            Loads contents of Database from a filename database.csv.
        """
        with open(self.name + ".csv") as f:
            list_of_rows = f.readlines()

        list_of_rows = map(
            lambda x: x.strip(),
            map(
                lambda x: x.replace("\"", ""),
                list_of_rows
            )
        )

        for row in list_of_rows:
            self.put_row(make_row(self.columns, row.split(',')))

    def store_contents(self):
        """
            Stores contests of the Database into a filename database.csv.
        """

        string_buffer = os.linesep.join(
            map(
                lambda x: ",".join(x),
                map(
                    lambda x: map(
                        str,
                        x.values()
                    ),
                    self.rows
                )
            )
        )

        with open(self.name + ".csv", "w") as f:
            f.write(string_buffer)

    def print_contents(self):
        """
            Prints Contents of Table.
        """
        print "\t\t\t".join(self.columns)
        print os.linesep.join(
            map(
                self.make_output_row,
                self.rows
            ),
        )

    def make_output_row(self, row):
        return "\t\t\t".join(
            map(
                str,
                row.values()
            )
        )

    def __str__(self):
        """
            String Representation of a table
        """
        return "%s" % (self.name)
