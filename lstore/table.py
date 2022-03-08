from msilib import schema
from lstore.index import Index
from lstore.page import Page
from lstore.bpool import bufferpool
from time import time

import os
import threading
import struct
import pickle

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
    def __init__(self, name="table", num_columns=1, key=0, bpool=bufferpool()):
        self.name = name
        self.threads = {}
        self.record_lock = {}
        self.primary_key_column = key
        self.primary_key_column_hidden = key + 4

        self.num_columns = num_columns
        self.num_columns_hidden = num_columns + 4

        self.current_rid = 1
        self.num_records = 0

        self.page_directory = {}

        self.original_base_pages = []
        self.base_pages = 0
        self.tail_pages = 0
        self.newest_base_pages = {}
        self.newest_tail_pages = {}

        self.merged_base_page_ranges = 0
        self.index = Index(self)
        self.bpool = bpool
        self.mpool = None
        self.tps_list = []
        self.threading_lock = threading.Lock()
        pass

    def __getstate__(self):
        return (self.name, self.primary_key_column, self.primary_key_column_hidden, 
            self.num_columns, self.num_columns_hidden, self.current_rid, self.num_records, self.page_directory, 
            self.original_base_pages, self.base_pages, self.tail_pages, self.newest_base_pages, self.newest_tail_pages, self.merged_base_page_ranges,
            self.index, self.bpool, self.mpool, self.tps_list)
    
    def __setstate__(self, state):

        self.name, self.primary_key_column, self.primary_key_column_hidden, self.num_columns, self.num_columns_hidden, self.current_rid, self.num_records, self.page_directory, self.original_base_pages, self.base_pages, self.tail_pages, self.newest_base_pages, self.newest_tail_pages, self.merged_base_page_ranges, self.index, self.bpool, self.mpool, self.tps_list = state

        self.threading_lock = threading.Lock()
        self.threads = {}

    def set_record_lock(self, base_rid, lock_value):
        self.record_lock[base_rid] = lock_value

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

        base_location = self.page_directory[base_rid]
        
        base_pages = base_location['pages']
        base_offsets = base_location['offsets']

        base_indirection = self.bpool.find_page(self.name, True, base_pages[INDIRECTION_COLUMN])
        self.bpool.pin_page(base_indirection)
        base_indirection[1] = time()
        indirection_rid = base_indirection[0].read(base_offsets[INDIRECTION_COLUMN])
        self.bpool.unpin_page(base_indirection)

        base_schema_encoding = self.bpool.find_page( self.name, True, base_pages[SCHEMA_ENCODING_COLUMN])
        self.bpool.pin_page(base_schema_encoding)
        base_schema_encoding[1] = time()
        base_schema_encoding_value = format(base_schema_encoding[0].read(base_offsets[SCHEMA_ENCODING_COLUMN]), '064b')
        self.bpool.unpin_page(base_schema_encoding)

        if indirection_rid == 1 or base_schema_encoding_value[column-4] == '0':
            base_column_page = self.bpool.find_page( self.name, True, base_pages[column])
            self.bpool.pin_page(base_column_page)
            base_column_page[1] = time()
            value = base_column_page[0].read(base_offsets[column])
            self.bpool.unpin_page(base_column_page)
            return value


        tail_location = self.page_directory[indirection_rid]
        while(True):
            tail_pages = tail_location['pages']
            tail_offsets = tail_location['offsets']

            tail_schema_encoding = self.bpool.find_page( self.name, False, tail_pages[SCHEMA_ENCODING_COLUMN])
            self.bpool.pin_page(tail_schema_encoding)
            tail_schema_encoding[1] = time()
            tail_schema_encoding_value = format(tail_schema_encoding[0].read(tail_offsets[SCHEMA_ENCODING_COLUMN]), '064b')
            self.bpool.unpin_page(tail_schema_encoding)

            if tail_schema_encoding_value[column-4] == '1':
                tail_column_page = self.bpool.find_page( self.name, False, tail_pages[column])
                self.bpool.pin_page(tail_column_page)
                tail_column_page[1] = time()
                value = tail_column_page[0].read(tail_offsets[column])
                self.bpool.unpin_page(tail_column_page)
                return value
            else:
                tail_indirection = self.bpool.find_page(self.name, False, tail_pages[INDIRECTION_COLUMN])
                self.bpool.pin_page(tail_indirection)
                tail_indirection[1] = time()
                indirection_rid = tail_indirection[0].read(tail_offsets[INDIRECTION_COLUMN])
                
                if indirection_rid == base_rid:
                    break
                tail_location = self.page_directory[indirection_rid]
                self.bpool.unpin_page(tail_indirection)

        return None

    def get_record(self, base_rid, query_columns):
        return_query_columns = query_columns
        number_of_values_queried = len(query_columns)
        for i in range(len(query_columns)):
            if query_columns[i] == 0:
                number_of_values_queried -= 1
                return_query_columns = None 

        base_location = self.page_directory[base_rid]
        
        base_pages = base_location['pages']
        base_offsets = base_location['offsets']

        base_indirection = self.bpool.find_page(self.name, True, base_pages[INDIRECTION_COLUMN])
        self.bpool.pin_page(base_indirection)
        base_indirection[1] = time()
        indirection_rid = base_indirection[0].read(base_offsets[INDIRECTION_COLUMN])
        self.bpool.unpin_page(base_indirection)

        base_schema_encoding = self.bpool.find_page( self.name, True, base_pages[SCHEMA_ENCODING_COLUMN])
        self.bpool.pin_page(base_schema_encoding)
        base_schema_encoding[1] = time()
        base_schema_encoding_value = format(base_schema_encoding[0].read(base_offsets[SCHEMA_ENCODING_COLUMN]), '064b')
        self.bpool.unpin_page(base_schema_encoding)

        for column, i in enumerate(query_columns):
            if i != 0:
                if indirection_rid == 1 or base_schema_encoding_value[column] == '0':
                    base_column_page = self.bpool.find_page( self.name, True, base_pages[column+4])
                    self.bpool.pin_page(base_column_page)
                    base_column_page[1] = time()
                    value = base_column_page[0].read(base_offsets[column+4])
                    self.bpool.unpin_page(base_column_page)
                    return_query_columns[column] = value
                    
        correct_columns = 0
        for i in range(len(query_columns)):
            if query_columns[i] == 0 and return_query_columns[i] == None:
                correct_columns += 1
        
        if len(query_columns)-correct_columns == number_of_values_queried:
            return return_query_columns
        
        next_indirection = indirection_rid
        while(True):

            tail_location = self.page_directory[next_indirection]
            tail_pages = tail_location['pages']
            tail_offsets = tail_location['offsets']


            tail_schema_encoding = self.bpool.find_page( self.name, False, tail_pages[SCHEMA_ENCODING_COLUMN])
            self.bpool.pin_page(tail_schema_encoding)
            tail_schema_encoding[1] = time()
            tail_schema_encoding_value = format(tail_schema_encoding[0].read(tail_offsets[SCHEMA_ENCODING_COLUMN]), '064b')
            self.bpool.unpin_page(tail_schema_encoding)

            for column, i in enumerate(query_columns):
                if i != 0:
                    if tail_schema_encoding_value[column] == '1':
                        tail_column_page = self.bpool.find_page( self.name, False, tail_pages[column+4])
                        self.bpool.pin_page(tail_column_page)
                        tail_column_page[1] = time()
                        value = tail_column_page[0].read(tail_offsets[column+4])
                        self.bpool.unpin_page(tail_column_page)
                        return_query_columns[column] = value

            correct_columns = 0
            for i in range(len(query_columns)):
                if query_columns[i] == 0 and return_query_columns[i] == None:
                    correct_columns += 1
            
            if len(query_columns)-correct_columns == number_of_values_queried:
                break
            
        
            tail_indirection = self.bpool.find_page(self.name, False, tail_pages[INDIRECTION_COLUMN])
            self.bpool.pin_page(tail_indirection)
            tail_indirection[1] = time()
            next_indirection = tail_indirection[0].read(tail_offsets[INDIRECTION_COLUMN])
            
            
            self.bpool.unpin_page(tail_indirection)

        return return_query_columns


    def get_value(self, rid, column):
        location = self.page_directory[rid]
        pages = location['pages']
        offsets = location['offsets']


        page = self.bpool.find_page(self.name, location['base'], pages[column])

        self.bpool.pin_page(page)
        page[1] = time()
        value = page[0].read(offsets[column])
        self.bpool.unpin_page(page)

        return value

    def set_value(self, value, rid, column):

        location = self.page_directory[rid]
        pages = location['pages']
        offsets = location['offsets']

        page = self.bpool.find_page(self.name, location['base'], pages[column])

        self.bpool.pin_page(page)
        page[1] = time()
        value = page[0].set_value(value, offsets[column])
        self.bpool.mark_dirty(page)
        self.bpool.unpin_page(page)


    def mpool_get_value(self, base_rid, column):
        location = self.page_directory[base_rid]

        base_index = self.mpool.find_page( self.name, True, location[1] , INDIRECTION_COLUMN )
        self.mpool.bpool[base_index][1] = time()
        rid = self.mpool.bpool[base_index][0].read(location[2])

        if rid == 1:
            
            base_index = self.mpool.find_page( self.name, True, location[1] , column)
            return self.mpool.bpool[base_index][0].read(location[2])

        new_location = self.page_directory[rid]
        index = self.mpool.find_page( self.name, False, new_location[1], column)
        self.mpool.bpool[index][1] = time()
        return self.mpool.bpool[index][0].read(new_location[2])

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
        record.columns.insert(TIMESTAMP_COLUMN, time())
        record.columns.insert(SCHEMA_ENCODING_COLUMN, 0)
        self.record_lock[record.rid] = "False"


        if self.base_pages == 0:
            for i in range(self.num_columns_hidden):
                self.base_pages += 1
                index = self.bpool.add_page(Page("B" + str(self.base_pages),self.name))
                self.bpool.mark_dirty(index)
                self.newest_base_pages[i] = self.base_pages
            self.tps_list.append(0)

        added_new_base_page_range = False
        pages = {}
        offsets = {}
        for column_index in range(self.num_columns_hidden):
            #grab newest base page from pool
            page = self.bpool.find_page(self.name, True, self.newest_base_pages[column_index])[0]

            #check if page has space
            if page.has_capacity():

                #pin the page then add to pages and offsets dictionary where value was written
                self.bpool.pin_page(page)
                pages[column_index] = self.newest_base_pages[column_index]
                offsets[column_index] = page.write(record.columns[column_index])

                #unpin and mark dirty
                self.bpool.unpin_page(page)
                self.bpool.mark_dirty(page)
            else:

                #increment number of pages and make a new page
                self.base_pages += 1
                page = Page("B" + str(self.base_pages), self.name)

                #add to pages and offsets dictionary where value was written
                pages[column_index] = self.base_pages
                offsets[column_index] = page.write(record.columns[column_index])

                #add page to pool and mark dirty
                new_page_in_pool = self.bpool.add_page(page)
                self.bpool.mark_dirty(new_page_in_pool)

                #update newest base pages at this column index to be this new page that was created
                self.newest_base_pages[column_index] = self.base_pages
                added_new_base_page_range = True

        # add to tps list for new base page range if a new base page range was created.
        if added_new_base_page_range:
            self.tps_list.append(0)
        

        # keep track of original base pages
        for page in pages:
            if page not in self.original_base_pages:
                self.original_base_pages.append(page)

        self.page_directory[record.rid] = {'base':True, 'pages':pages, 'offsets':offsets}
        self.index.insert(record, record.rid)


    def update_record(self, record, base_rid):
        
        new_rid = record.rid

        #Get old base page indirection to set as indirection column on this entry
        old_base_indirection = self.get_value(base_rid, INDIRECTION_COLUMN)
        if old_base_indirection == 1:
            old_base_indirection = base_rid

        record.columns.insert(INDIRECTION_COLUMN, old_base_indirection)

        record.columns.insert(RID_COLUMN, record.rid)

        record.columns.insert(TIMESTAMP_COLUMN, time())

        

        schema_encoding = ""
        schema_encoding_base = format(self.get_value(base_rid, SCHEMA_ENCODING_COLUMN), "064b")

        for i in range(self.num_columns):
            if(record.columns[i+3] == None):
                schema_encoding += "0"
            else:
                schema_encoding_base = schema_encoding_base[:i] + "1" + schema_encoding_base[i+1:]
                schema_encoding += "1"

        
        schema_encoding_64 = schema_encoding
        for i in range(self.num_columns,64):
            schema_encoding_64 += "0"
        
        record.columns.insert(SCHEMA_ENCODING_COLUMN, int(schema_encoding_64,2))
        self.set_value(int(schema_encoding_base,2), base_rid, SCHEMA_ENCODING_COLUMN)

        #Set indirection column on base page to point to this record.
        self.set_value(new_rid, base_rid, INDIRECTION_COLUMN)

        if self.tail_pages == 0:
            for i in range(self.num_columns_hidden):
                self.tail_pages += 1
                index = self.bpool.add_page(Page("T" + str(self.tail_pages), self.name))
                self.bpool.mark_dirty(index)
                self.newest_tail_pages[i] = self.tail_pages
            
        
        
        pages = {}
        offsets = {}
        for column_index in range(4):

            #Grab page newest tail page in bpool
            page = self.bpool.find_page(self.name, False, self.newest_tail_pages[column_index])[0]
            
            #Check if page has capacity
            if page.has_capacity():

                #pin page
                self.bpool.pin_page(page)

                #add to pages and offsets for page directory
                pages[column_index] = self.newest_tail_pages[column_index]
                offsets[column_index] = page.write(record.columns[column_index])

                #unpin and mark dirty
                self.bpool.unpin_page(page)
                self.bpool.mark_dirty(page)

            else:
                #increment number of tail pages and make new page
                self.tail_pages += 1
                page = Page("T" + str(self.tail_pages), self.name)

                #add to pages and offsets for page directory
                pages[column_index] = self.tail_pages
                offsets[column_index] = page.write(record.columns[column_index])

                #add page to pool and mark dirty
                new_page_in_pool = self.bpool.add_page(page)
                self.bpool.mark_dirty(new_page_in_pool)

                #update newest tail page
                self.newest_tail_pages[column_index] = self.tail_pages

        for column_index in range(4, self.num_columns_hidden):
            #Check if value exists based on schema_encoding
            if schema_encoding[column_index-4] == '1':

                #Grab page newest tail page in bpool
                page = self.bpool.find_page(self.name, False, self.newest_tail_pages[column_index])[0]
                
                #Check if page has capacity
                if page.has_capacity():

                    #pin page
                    self.bpool.pin_page(page)
                    
                    #add to pages and offsets for page directory
                    pages[column_index] = self.newest_tail_pages[column_index]
                    offsets[column_index] = page.write(record.columns[column_index])

                    #unpin and mark dirty
                    self.bpool.unpin_page(page)
                    self.bpool.mark_dirty(page)

                else:
                    #increment number of tail pages and make new page
                    self.tail_pages += 1
                    page = Page("T" + str(self.tail_pages), self.name)

                    #add to pages and offsets for page directory
                    pages[column_index] = self.tail_pages
                    offsets[column_index] = page.write(record.columns[column_index])

                    #add page to pool and mark dirty
                    new_page_in_pool = self.bpool.add_page(page)
                    self.bpool.mark_dirty(new_page_in_pool)

                    #update newest tail page
                    self.newest_tail_pages[column_index] = self.tail_pages

            else:
                pages[column_index] = None
                offsets[column_index] = None


        self.page_directory[new_rid] = {'base':False, 'pages':pages, 'offsets':offsets}
        self.index.update(schema_encoding, record, base_rid)


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
        
        self.threading_lock.acquire()
        if page_range not in self.original_base_pages:
            original = False
        else:
            original = True
        
        self.mpool = bufferpool()
        tps = 0
        base_rids = []
        base_pages, new_page_range = self.read_base_page_range(page_range,original)

        for i in range(base_pages[0].num_records):
            rid = base_pages[0].read(i)
            base_rid = base_pages[1].read(i)
            base_rids.append(base_rid)

            if rid > tps:
                tps = rid


            for j in range(2, self.num_columns_hidden):
                base_pages[j].set_value(self.mpool_get_value(base_rid, j), i)

        self.write_base_page_range(base_pages)
        for rid in base_rids:
            self.page_directory[rid][1] = new_page_range

        if original:
            self.tps_list.append(tps)
        else:
            self.tps_list[page_range] = tps
        self.merged_base_page_ranges += 1

        self.threading_lock.release()

        self.mpool = None

    def write_base_page_range(self, base_pages):
        for i, page in enumerate(base_pages):
            cwd = os.getcwd()
            path = cwd + "\ECS165\\" + page.table_name
            try:
                os.mkdir(path)
            except OSError as error:
                pass
            self.mpool.write_page_to_disk(page, path)


    # reads all base pages from disk and stores it into a list of base pages
    def read_base_page_range(self, page_range, original):
        in_page_range = None
        cwd = os.getcwd()
        page_type = 'B'
        base_pages = []
        for i in range(self.num_columns_hidden):
            with open(cwd + "\ECS165\\" + self.name + "\\" + page_type + str(page_range) + "-" + str(i), 'rb') as file:
                lines = file.read()

                if original:
                    in_page_range = len(self.tps_list)
                else:
                    in_page_range = page_range

                page_find = Page(page_type + str(in_page_range) + "-" + str(i), self.name)
                page_find.set_num_records(struct.unpack('Q', lines[0:8])[0])
                page_find.set_data(lines[8:])
                base_pages.append(page_find)
            
        return base_pages, in_page_range
