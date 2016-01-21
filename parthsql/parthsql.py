# -*- coding: utf-8 -*-

import os
import itertools
METADATA_FILE = "metadata.txt"


def make_row(columns, values):
    return dict(zip(columns, values))


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
            self.tables.append(
                Table(
                    name=iterator[0],
                    columns=iterator[1:][:],
                    rows=[]
                )
            )

        for i in self.tables:
            i.load_contents

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

        with open(METADATA_FILE, "rw") as f:
            f.write(string_buffer)

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

    def get_column(self, column):
        """
            Return the values having of column.
        """
        return map(
            lambda x: x.get(
                column
            ),
            self.rows
        )

    def put_row(self, row):
        """
            Adds a row to the table.
        """
        self.rows.append(row)

    def delete_row(self, key, value):
        """
            Deletes the rows where key = value.
        """
        self.rows = filter(lambda x: x.get(key) is not value, self.rows)

    def load_contents(self):
        """
            Loads contents of Database from a filename database.csv.
        """
        with open(self.name + ".csv") as f:
            list_of_rows = f.readlines()

        list_of_rows = map(lambda x: x.strip(), list_of_rows)

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
                    lambda x: x.values(),
                    self.rows
                )
            )
        )

        with open(self.name + ".csv", "rw") as f:
            f.write(string_buffer)

    def __str__(self):
        """
            String Representation of a table
        """
        return "%s" % (self.name)
