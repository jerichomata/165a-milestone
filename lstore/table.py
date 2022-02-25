import enum
from heapq import merge
from logging import NullHandler
from re import X
from lstore.index import Index
from lstore.page import Page

import os
import threading

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
    def __init__(self, name, num_columns, key, bpool, mpool):
        self.name = name
        self.thread = threading.Thread(target=merge, args=self.base_page_ranges, daemon=True)
        self.primary_key_column = key
        self.primary_key_column_hidden = key + 4
        self.num_columns = num_columns
        self.num_columns_hidden = num_columns + 4
        self.current_rid = 1
        self.num_records = 0
        self.page_directory = {}
        self.base_page_ranges = 0
        self.tail_page_ranges = 0
        self.index = Index(self)
        self.bpool = bpool
        self.mpool = mpool
        self.tps_list = []
        self.threading_lock = threading.lock()
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

        base_index = self.bpool.find_page( self.name, True, location[1] , INDIRECTION_COLUMN )
        
        rid = self.bpool.bpool[base_index][0].read(location[2])

        if (self.tps_list[location[2]] > rid):
            self.thread.start(target=self.merge, args=location[1])
            self.thread.join()

        if rid == 1:
            base_index = self.bpool.find_page( self.name, True, location[1] , column)
            return self.bpool.bpool[base_index][0].read(location[2])

        new_location = self.page_directory[rid]
        index = self.bpool.find_page( self.name, False, new_location[1], column)
        offset = (new_location[2])
        print("")
        a = self.bpool.bpool[index][0].read(offset)
        return a


    def get_value(self, rid, column):
        location = self.page_directory[rid]

        index = self.bpool.find_page(self.name, location[0], location[1] , column)
        return self.bpool.bpool[index][0].read(location[2])

    def set_value(self, value, rid, column):
        location = self.page_directory[rid]

        index = self.bpool.find_page(self.name, location[0], location[1] , column)
        self.bpool.bpool[index][0].set_value(value, location[2])
        self.bpool.mark_dirty(index)

    def mpool_get_value(self, rid, column):
        location = self.page_directory[rid]

        index = self.mpool.find_page(self.name, location[0], location[1] , column)
        return self.mpool.bpool[index][0].read(location[2])

    def mpool_set_value(self, value, rid, column):
        location = self.page_directory[rid]

        index = self.mpool.find_page(self.name, location[0], location[1] , column)
        self.mpool.bpool[index][0].set_value(value, location[2])
        self.mpool.mark_dirty(index)

    def is_base(self, rid):
        return self.page_directory[rid][0]

    def add_record(self, record):
        record.columns.insert(INDIRECTION_COLUMN, 1)
        record.columns.insert(RID_COLUMN, record.rid)
        record.columns.insert(TIMESTAMP_COLUMN, time.time())
        record.columns.insert(SCHEMA_ENCODING_COLUMN, 0)


        if self.base_page_ranges == 0:
            for i in range(self.num_columns_hidden):
                self.bpool.add_page(Page("B" + str(self.base_page_ranges) + "-" + str(i), self.name))
            self.base_page_ranges = 1
        

        index = self.bpool.find_page(self.name, True, self.base_page_ranges-1, 0)
        if self.bpool.bpool[index][0].has_capacity():
            for j in range(self.num_columns_hidden):
                current_index = self.bpool.find_page(self.name, True, self.base_page_ranges-1, j)
                self.bpool.bpool[current_index][0].write(record.columns[j])
                self.bpool.mark_dirty(current_index)
        else:
            for j in range(self.num_columns_hidden):
                page = Page("B" + str(self.base_page_ranges) + "-" + str(j), self.name)
                page.write(record.columns[j])
                index = self.bpool.add_page(page)
                self.bpool.mark_dirty(index)
            
            self.base_page_ranges += 1

        offset = self.bpool.bpool[index][0].num_records - 1

        self.page_directory[record.rid] = [True, self.base_page_ranges-1, offset]
        self.index.sorted_insert(record, record.rid)


    def update_record(self, record, base_rid):
        new_rid = record.rid

        #Get old base page indirection to set as indirection column on this entry
        old_base_indirection = self.get_value(base_rid, INDIRECTION_COLUMN)
        if old_base_indirection == 1:
            old_base_indirection = base_rid

        record.columns.insert(INDIRECTION_COLUMN, old_base_indirection)

        record.columns.insert(RID_COLUMN, record.rid)

        record.columns.insert(TIMESTAMP_COLUMN, time.time())

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

        if self.tail_page_ranges == 0:
            for i in range(self.num_columns_hidden):
                self.bpool.add_page(Page("T" + str(self.tail_page_ranges) + "-" + str(i), self.name))
            self.tail_page_ranges = 1
        
        index = self.bpool.find_page(self.name, False, self.tail_page_ranges-1, 0)
        if self.bpool.bpool[index][0].has_capacity():
            for j in range(self.num_columns_hidden):
                current_index = self.bpool.find_page(self.name, False, self.tail_page_ranges-1, j)
                self.bpool.bpool[current_index][0].write(record.columns[j])
                self.bpool.mark_dirty(current_index)
        else:
            for j in range(self.num_columns_hidden):
                page = Page("T" + str(self.tail_page_ranges) + "-" + str(j), self.name)
                page.write(record.columns[j])
                index = self.bpool.add_page(page)
                self.bpool.mark_dirty(index)

            self.tail_page_ranges += 1

        offset = self.bpool.bpool[index][0].num_records - 1

        self.page_directory[new_rid] = [False, self.tail_page_ranges-1, offset]
        self.index.update(record, base_rid)


    def delete_record(self, key):
        starting_rid = self.index.locate(key)[0]
        self.index.drop_record(starting_rid)

        current_rid = self.get_value(starting_rid, INDIRECTION_COLUMN)
        while(current_rid != starting_rid and current_rid != 1):
            self.set_value(0, current_rid, RID_COLUMN)
            current_rid = self.get_value(current_rid, INDIRECTION_COLUMN)

        self.set_value(0, starting_rid, RID_COLUMN)

    # store it into mergpool 
    # empty the mergepool 
    def merge(self, page_range):
        tps = 0
        base_rids = []
        base_pages, new_page_range = self.read_base_page_range(page_range)

        for i in range(base_pages[0].num_records):
            
            rid = base_pages[0].read[i]
            base_rid = base_pages[1].read[i]
            base_rids.append(base_rid)

            if rid > tps:
                tps = rid


            for j in range(2, self.num_columns_hidden):
                base_pages[j].set_value(self.mpool_get_value(rid, j), i)

        self.write_base_page_range(base_pages)
        self.threading_lock.aquire()
        for rid in base_rids:
            self.page_directory[rid][1] = new_page_range
        self.threading_lock.release()


        self.tps_list.insert(page_range-1, tps)

    def write_base_page_range(self, base_pages):
        for i, page in enumerate(base_pages):
            cwd = os.getcwd()
            path = cwd + "\\disk\\" + page.table
            os.mkdir(path)
            self.mpool.write_page_to_disk(page, path)


    # reads all base pages from disk and stores it into a list of base pages
    def read_base_page_range(self, page_range):
        new_page_range = len(self.tps_list)
        cwd = os.getcwd()
        page_type = 'B'
        for i in range(self.num_columns_hidden):
            with open(cwd + "\\disk\\" + self.name + "\\" + page_type + str(page_range) + "-" + str(i) + ".txt", 'r') as file:
                lines = file.readlines()
                lines.split(" ")
                page_find = Page(page_type + new_page_range + "-" + i, self.name)
                page_find.set_num_records(lines[0])
                page_find.set_data(lines[1])
            
        return page_find, new_page_range
