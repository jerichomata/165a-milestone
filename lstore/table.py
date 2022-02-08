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
    RID of 0 means Invalid
    RID of 1 means None 
    RIDS start at 2
    """
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns_hidden: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    
    name: name of the table
    key: index of the table key
    num_columns_hidden: number of columns in table
    current_rid: number of records in table
    page_directory: rid | base?(bool), page(int), offset(int)
    base_pages: list of base pages
    tail_pages: list of tail pages
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.primary_key_column = key
        self.primary_key_column_hidden = key + 4
        self.num_columns = num_columns
        self.num_columns_hidden = num_columns + 4
        self.current_rid = 1
        self.num_records = 0
        self.page_directory = {}
        self.base_pages = []
        self.tail_pages = []
        self.index = Index(self)
        pass

    def get_base_rid(self, rid):
        current_rid = rid
        while (current_rid != 1):
            current_rid = self.get_value(rid, 0)
            if self.is_base(current_rid):
                return current_rid


    def get_new_rid(self):
        self.current_rid += 1
        self.num_records += 1
        return self.current_rid


    def get_newest_value(self, base_rid, column):
        location = self.page_directory[base_rid]
        rid = self.base_pages[int(location[1] * self.num_columns_hidden)].read(location[2])
        if (rid != 1):
            location = self.page_directory[rid]
            return self.tail_pages[int(location[1] * self.num_columns_hidden) + column].read(location[2])
        else:
            return self.base_pages[int(location[1] * self.num_columns_hidden) + column].read(location[2])


    def get_value(self, rid, column):
        base, page, offset = self.page_directory[rid]
        if base:
            return self.base_pages[int(page * self.num_columns_hidden) + column].read(offset)
        else:
            return self.tail_pages[int(page * self.num_columns_hidden) + column].read(offset)


    def set_value(self, value, rid, column):
        base, page, offset = self.page_directory[rid]
        if base:
            return self.base_pages[int(page * self.num_columns_hidden) + column].set_value(value, offset)
        else:
            return self.tail_pages[int(page * self.num_columns_hidden) + column].set_value(value, offset)


    def is_base(self, rid):
        return self.page_directory[rid][0]


    def add_record(self, record):
        record.columns.insert(INDIRECTION_COLUMN, 1)
        record.columns.insert(RID_COLUMN, record.rid)
        record.columns.insert(TIMESTAMP_COLUMN, time())
        record.columns.insert(SCHEMA_ENCODING_COLUMN, 0)

        if len(self.base_pages) == 0:
            for i in range(self.num_columns_hidden):
                self.base_pages.append(Page())


        if self.base_pages[len(self.base_pages)-1].has_capacity():
            for j in range(self.num_columns_hidden,0,-1):
                index = len(self.base_pages)-j
                self.base_pages[index].write(record.columns[self.num_columns_hidden-j])
        else:
            for j in range(self.num_columns_hidden):
                page = Page()
                page.write(record.columns[j])
                self.base_pages.append(page)
            index = len(self.base_pages)-1

        page_number = (len(self.base_pages)/self.num_columns_hidden) - 1
        offset = self.base_pages[index].num_records - 1

        self.page_directory[record.rid] = [True, int(page_number), offset]
        self.index.sorted_insert(record, record.rid)


    def update_record(self, record, base_rid):
        new_rid = record.rid

        #Get old base page indirection to set as indirection column on this entry
        old_base_indirection = self.get_value(base_rid, INDIRECTION_COLUMN)
        if old_base_indirection == 1:
            old_base_indirection = base_rid

        record.columns.insert(INDIRECTION_COLUMN, old_base_indirection)

        record.columns.insert(RID_COLUMN, record.rid)

        record.columns.insert(TIMESTAMP_COLUMN, time())

        record.columns.insert(SCHEMA_ENCODING_COLUMN, 0)

        schema_encoding = ""

        for i in range(self.num_columns_hidden):
            if(record.columns[i] == None):
                record.columns[i] = self.get_value(old_base_indirection, i)
                schema_encoding += "0"
            else:
                schema_encoding += "1"

        for i in range(self.num_columns_hidden,64):
            schema_encoding += "0"

        self.set_value(int(schema_encoding,2), old_base_indirection, SCHEMA_ENCODING_COLUMN)

        #Set indirection column on base page to point to this record.
        self.set_value(new_rid, base_rid, INDIRECTION_COLUMN)

        if len(self.tail_pages) == 0:
            for i in range(self.num_columns_hidden):
                self.tail_pages.append(Page())
        
        if self.tail_pages[len(self.tail_pages)-1].has_capacity():
            for j in range(self.num_columns_hidden,0,-1):
                index = len(self.tail_pages)-j
                self.tail_pages[index].write(record.columns[self.num_columns_hidden-j])
        else:
            for j in range(self.num_columns_hidden):
                page = Page()
                page.write(record.columns[j])
                self.tail_pages.append(page)
            index = len(self.tail_pages)-1

        page_number = (len(self.tail_pages)/self.num_columns_hidden) - 1
        offset = self.tail_pages[index].num_records - 1

        self.page_directory[new_rid] = [False, page_number, offset]
        self.index.update(record, base_rid)

    def delete_record(self, key):
        starting_rid = self.index.locate(key)[0]
        self.index.drop_record(starting_rid)

        current_rid = self.get_value(starting_rid, INDIRECTION_COLUMN)
        while(current_rid != starting_rid and current_rid != 1):
            self.set_value(0, current_rid, RID_COLUMN)
            current_rid = self.get_value(current_rid, INDIRECTION_COLUMN)

        self.set_value(0, starting_rid, RID_COLUMN)


    def __merge(self):
        print("merge is happening")
        pass
