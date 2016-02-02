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
            self.tables.append(
                Table(
                    name=iterator[0],
                    columns=iterator[1:][:],
                    rows=[]
                )
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

        return os.linesep.join(
            map(
                self.make_output_row,
                self.rows()
            ),
        )

    def make_output_row(self, row):
        print self, row

    def __str__(self):
        """
            String Representation of a table
        """
        return "%s" % (self.name)
