from logging import NullHandler
from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.num_records = 0
        self.page_directory = {}
        self.index = Index(self)
        pass

    def get_rid(self):
        self.num_records += 1
        return self.num_records

    def find_record(self, key):


    def add_record(self, key, column_values):

        if key <= self.key:
            print('key already exists')
            return

        column_values.insert(INDIRECTION_COLUMN, None)
        column_values.insert(RID_COLUMN, self.get_rid())
        column_values.insert(TIMESTAMP_COLUMN, time())
        column_values.insert(SCHEMA_ENCODING_COLUMN, 0)

        record = Record(column_values[1], key, column_values)

        for page in self.page_directory['base']:
            if page.has_capacity():
                page.write(record)
                self.key += 1
                return
            
        self.page_directory['base'].append(Page())
        self.page_directory['base'][-1].write(record)
        self.key += 1

    def update_record(self, key, column_values):





    def __merge(self):
        print("merge is happening")
        pass
 
