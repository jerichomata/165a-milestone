from logging import NullHandler
from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:
    """
    RID of 0 means None RIDS start at 1
    """
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
    page_directory: rid | base?(bool), page(int), offset(int)
    base_pages: list of base pages
    tail_pages: list of tail pages
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.primary_key_column = key + 4
        self.num_columns = num_columns + 4
        self.num_records = 0
        self.page_directory = {}
        self.base_pages = []
        self.tail_pages = []
        self.index = Index(self)
        pass

    def get_base_rid(self, rid):
        current_rid = rid
        while (current_rid != 0):
            current_rid = self.get_value(rid, 0)
            if self.is_base(current_rid):
                return current_rid


    def get_rid(self):
        self.num_records += 1
        return self.num_records


    def get_newest_value(self, base_rid, column):
        location = self.page_directory[base_rid]
        rid = self.base_pages[int(location[1] * self.num_columns)].read(location[2])
        if (rid != None):
            location = self.page_directory[rid]
            return self.tail_pages[int(location[1] * self.num_columns) + column].read(location[2])
        else:
            return self.base_pages[int(location[1] * self.num_columns) + column].read(location[2])


    def get_value(self, rid, column):
        base, page, offset = self.page_directory[rid]
        if base:
            return self.base_pages[(page * self.num_columns) + column].read(offset)
        else:
            return self.tail_pages[(page * self.num_columns) + column].read(offset)

    def set_value(self, value, rid, column):
        base, page, offset = self.page_directory[rid]
        if base:
            return self.base_pages[(page * self.num_columns) + column].set_value(value, offset)
        else:
            return self.tail_pages[(page * self.num_columns) + column].set_value(value, offset)


    def is_base(self, rid):
        return self.page_directory[rid][0]


    def add_record(self, record):
        record.columns.insert(INDIRECTION_COLUMN, 0)
        record.columns.insert(RID_COLUMN, record.rid)
        record.columns.insert(TIMESTAMP_COLUMN, time())
        record.columns.insert(SCHEMA_ENCODING_COLUMN, 0)

        if len(self.base_pages) == 0:
            for i in range(self.num_columns):
                self.base_pages.append(Page())


        if self.base_pages[len(self.base_pages)-1].has_capacity():
            for j in range(self.num_columns,0,-1):
                index = len(self.base_pages)-j
                self.base_pages[index].write(record.columns[self.num_columns-j])
        else:
            for j in range(self.num_columns):
                page = Page()
                page.write(record.columns[j])
                self.base_pages.append(page)

        page_number = (len(self.base_pages)/self.num_columns) - 1
        offset = self.base_pages[index].num_records - 1

        self.page_directory[record.rid] = [True, page_number, offset]
        self.index.sorted_insert(record, record.rid)


    def update_record(self, record, base_rid):

        new_rid = record.rid

        #Get old base page indirection to set as indirection column on this entry
        old_base_indirection = self.get_value(base_rid, INDIRECTION_COLUMN)
        record.columns.insert(INDIRECTION_COLUMN, old_base_indirection)

        record.columns.insert(RID_COLUMN, record.rid)
        record.columns.insert(TIMESTAMP_COLUMN, time())
        record.columns.insert(SCHEMA_ENCODING_COLUMN, 0)

        #Set indirection column on base page to point to this record.
        self.set_value(new_rid, base_rid, INDIRECTION_COLUMN)

        if len(self.tail_pages) == 0:
            for i in range(self.num_columns):
                self.tail_pages.append(Page())
        
        if self.tail_pages[len(self.tail_pages)-1].has_capacity():
            for j in range(self.num_columns,0,-1):
                index = len(self.tail_pages)-j
                self.tail_pages[index].write(record.columns[self.num_columns-j])
        else:
            for j in range(self.num_columns):
                page = Page()
                page.write(record.columns[j])
                self.tail_pages.append(page)

        page_number = (len(self.tail_pages)/self.num_columns) - 1
        offset = self.tail_pages[index].num_records - 1

        self.page_directory[new_rid] = [False, page_number, offset]
        self.index.update(record, base_rid)




    def __merge(self):
        print("merge is happening")
        pass
 

new_record = Record(1, 1, [24,25,27])
table = Table("stuff", 3, 0)
table.add_record(new_record)

table.get_newest_value(1, 4)