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
    
    name: name of the table
    key: index of the table key
    num_columns: number of columns in table
    num_records: number of records in table
    page_directory: rid | page, offset
    base_pages: list of base pages
    tail_pages: list of tail pages
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.keys = {}
        self.num_columns = num_columns + 4
        self.num_records = 0
        self.page_directory = {}
        self.base_pages = []
        self.tail_pages = []
        self.index = Index(self)
        pass

    def get_rid(self):
        self.num_records += 1
        return self.num_records

    def find_record(self, key):
        pass


    def add_record(self, record):

        if record.key <= self.key:
            print('key already exists')
            return

        record.columns.insert(INDIRECTION_COLUMN, None)
        record.columns.insert(RID_COLUMN, record.rid)
        record.columns.insert(TIMESTAMP_COLUMN, time())
        record.columns.insert(SCHEMA_ENCODING_COLUMN, 0)


        if len(self.base_pages) == 0:
            for i in range(self.num_columns):
                self.base_pages.append(Page())

        for j in range(self.num_columns,0,-1):
            index = len(self.base_pages)-j
            if self.base_pages[index].has_capacity():
                self.base_pages[index].write(record.columns[self.num_columns-j])
                self.key += 1
            else:
                page = Page()
                page.write(record.columns[self.num_columns-j])
                self.base_pages.append(page)
                self.key += 1


    def update_record(self, record):
        pass




    def __merge(self):
        print("merge is happening")
        pass
 
