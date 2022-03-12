from msilib import schema
from lstore.index import Index
from lstore.mpool import mergepool
from lstore.page import Page
from lstore.bpool import bufferpool

from lstore.lock_manager import LockManager
from lstore.logger import Logger
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
    page_directory: {rid | {base(bool), pages(dictionary of int), offsets(dictionary of ints)}}
    base_pages: list of base pages
    tail_pages: list of tail pages
    """
    def __init__(self, name="table", num_columns=1, key=0, bpool=bufferpool()):
        self.name = name
        self.threads = {}

        self.lock_manager = LockManager(self)
        self.logger = Logger(self.name)

        self.primary_key_column = key
        self.primary_key_column_hidden = key + 4

        self.num_columns = num_columns
        self.num_columns_hidden = num_columns + 4

        self.current_rid = 1
        self.num_records = 0

        self.page_directory = {}
        self.base_pages = 0
        self.tail_pages = 0
        self.newest_base_pages = {}
        self.newest_tail_pages = {}
        self.merge_in_progress = {}
        self.index = Index(self)
        self.bpool = bpool
        self.mpool = mergepool()
        self.tps_list = {}
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
        lock = threading.Lock()
        return_query_columns = [None for _ in query_columns]
        number_of_values_queried = query_columns.count(1)
        actual_number_of_values_queried = 0

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
        

        page_range = int((base_pages[INDIRECTION_COLUMN]+self.num_columns_hidden - 1)/self.num_columns_hidden)
        if indirection_rid < self.tps_list[page_range]:
            for column, i in enumerate(query_columns):
                if i == 1:
                    base_column_page = self.bpool.find_page(self.name, True, base_pages[column+4], True)
                    self.bpool.pin_page(base_column_page)
                    base_column_page[1] = time()
                    value = base_column_page[0].read(base_offsets[column+4])
                    self.bpool.unpin_page(base_column_page)
                    return_query_columns[column] = value

            return return_query_columns

        
        for column, i in enumerate(query_columns):
            if i == 1:
                if indirection_rid == 1 or base_schema_encoding_value[column] == '0':
                    base_column_page = self.bpool.find_page(self.name, True, base_pages[column+4])
                    self.bpool.pin_page(base_column_page)
                    base_column_page[1] = time()
                    value = base_column_page[0].read(base_offsets[column+4])
                    self.bpool.unpin_page(base_column_page)
                    return_query_columns[column] = value
                    actual_number_of_values_queried += 1
        
        
        
        if actual_number_of_values_queried == number_of_values_queried:
            return return_query_columns
        
        iterations = 0
        next_indirection = indirection_rid
        
        while(True):

            lock.acquire()
            if next_indirection < self.tps_list[page_range]:
                lock.release()
                for column, i in enumerate(query_columns):
                    if i == 1 and return_query_columns[column] == None:
                        base_column_page = self.bpool.find_page(self.name, True, base_pages[column+4], True)
                        self.bpool.pin_page(base_column_page)
                        base_column_page[1] = time()
                        value = base_column_page[0].read(base_offsets[column+4])
                        self.bpool.unpin_page(base_column_page)
                        return_query_columns[column] = value

                return return_query_columns
            else:
                lock.release()
            
            # lock.acquire()
            # if iterations == 2 and self.merge_in_progress[int(page_range)] == False:
            #     self.merge_in_progress[page_range] = True
            #     lock.release()
            #     self.bpool.make_clean()
            #     thread = threading.Thread(target = self.merge, args=(base_pages[INDIRECTION_COLUMN], ), daemon=True)
            #     thread.start()
            # else:
            #     lock.release()

            
            tail_location = self.page_directory[next_indirection]
            tail_pages = tail_location['pages']
            tail_offsets = tail_location['offsets']


            tail_schema_encoding = self.bpool.find_page( self.name, False, tail_pages[SCHEMA_ENCODING_COLUMN])
            self.bpool.pin_page(tail_schema_encoding)
            tail_schema_encoding[1] = time()
            tail_schema_encoding_value = format(tail_schema_encoding[0].read(tail_offsets[SCHEMA_ENCODING_COLUMN]), '064b')
            self.bpool.unpin_page(tail_schema_encoding)

            for column, i in enumerate(query_columns):
                if i == 1:
                    if tail_schema_encoding_value[column] == '1':
                        tail_column_page = self.bpool.find_page( self.name, False, tail_pages[column+4])
                        self.bpool.pin_page(tail_column_page)
                        tail_column_page[1] = time()
                        value = tail_column_page[0].read(tail_offsets[column+4])
                        self.bpool.unpin_page(tail_column_page)
                        return_query_columns[column] = value
                        actual_number_of_values_queried += 1

            
            if actual_number_of_values_queried == number_of_values_queried:
                return return_query_columns
            
        
            tail_indirection = self.bpool.find_page(self.name, False, tail_pages[INDIRECTION_COLUMN])
            self.bpool.pin_page(tail_indirection)
            tail_indirection[1] = time()
            next_indirection = tail_indirection[0].read(tail_offsets[INDIRECTION_COLUMN])
            self.bpool.unpin_page(tail_indirection)
            iterations += 1

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

    def mpool_get_record(self, base_rid):
        return_query_columns = [None for _ in range(self.num_columns)]
        actual_number_of_values_queried = 0

        base_location = self.page_directory[base_rid]
        
        base_pages = base_location['pages']
        base_offsets = base_location['offsets']

        base_indirection = self.mpool.find_page(self.name, True, base_pages[INDIRECTION_COLUMN])
        indirection_rid = base_indirection.read(base_offsets[INDIRECTION_COLUMN])

        base_schema_encoding = self.mpool.find_page( self.name, True, base_pages[SCHEMA_ENCODING_COLUMN])
        base_schema_encoding_value = format(base_schema_encoding.read(base_offsets[SCHEMA_ENCODING_COLUMN]), '064b')

        
        for column in range(self.num_columns):
            if indirection_rid == 1 or base_schema_encoding_value[column] == '0':
                base_column_page = self.mpool.find_page( self.name, True, base_pages[column+4])
                value = base_column_page.read(base_offsets[column+4])
                return_query_columns[column] = value
                actual_number_of_values_queried += 1
        
        
        
        if actual_number_of_values_queried == self.num_columns:
            return return_query_columns, 1
        
        next_indirection = indirection_rid
        latest_tail_record = 0
        while(True):
            if next_indirection > latest_tail_record:
                latest_tail_record = next_indirection

            tail_location = self.page_directory[next_indirection]
            tail_pages = tail_location['pages']
            tail_offsets = tail_location['offsets']

            for column in range(self.num_columns):
                if tail_offsets[column+4] is not None:
                    tail_column_page = self.mpool.find_page( self.name, False, tail_pages[column+4])
                    value = tail_column_page.read(tail_offsets[column+4])
                    return_query_columns[column] = value
                    actual_number_of_values_queried += 1

            if actual_number_of_values_queried == self.num_columns:
                break
            
        
            tail_indirection = self.mpool.find_page(self.name, False, tail_pages[INDIRECTION_COLUMN])
            next_indirection = tail_indirection.read(tail_offsets[INDIRECTION_COLUMN])
            if next_indirection == 0:
                print(base_rid)
                break

        return return_query_columns, latest_tail_record


    def is_base(self, rid):
        return self.page_directory[rid][0]

    def add_record(self, record):
        lock = threading.Lock()
        lock.acquire()
        id = self.logger.log_insert(record)
        lock.release()
        record.columns.insert(INDIRECTION_COLUMN, 1)
        record.columns.insert(RID_COLUMN, record.rid)
        record.columns.insert(TIMESTAMP_COLUMN, time())
        record.columns.insert(SCHEMA_ENCODING_COLUMN, 0)

        lock.acquire()
        base_pages = self.base_pages
        if base_pages == 0:
            for i in range(self.num_columns_hidden):
                self.base_pages += 1
                index = self.bpool.add_page(Page("B" + str(self.base_pages),self.name))
                self.bpool.mark_dirty(index)
                self.newest_base_pages[i] = self.base_pages
            self.tps_list[1] = 0
            self.merge_in_progress[1] = False

        lock.release()
        newest_base_pages = self.newest_base_pages
        added_new_base_page_range = False
        pages = {}
        offsets = {}
        for column_index in range(self.num_columns_hidden):
            #grab newest base page from pool
            lock.acquire()
            page = self.bpool.find_page(self.name, True, newest_base_pages[column_index])[0]
            #check if page has space
            if page.has_capacity():
                
                #pin the page then add to pages and offsets dictionary where value was written
                self.bpool.pin_page(page)
                pages[column_index] = newest_base_pages[column_index]
                lock.release()
                offsets[column_index] = page.write(record.columns[column_index])

                #unpin and mark dirty
                self.bpool.unpin_page(page)
                self.bpool.mark_dirty(page)
            else:
                lock.release()
                #increment number of pages and make a new page
                lock.acquire()
                for new_column_index in range(self.num_columns_hidden):
                    self.base_pages += 1
                    pages[new_column_index] = self.base_pages
                
                print(pages)
                for new_column_index in range(self.num_columns_hidden):
                    base_pages += 1
                    page = Page("B" + str(pages[new_column_index]), self.name)

                    #add to pages and offsets dictionary where value was written
                    offsets[new_column_index] = page.write(record.columns[new_column_index])

                    #add page to pool and mark dirty
                    new_page_in_pool = self.bpool.add_page(page)
                    self.bpool.mark_dirty(new_page_in_pool)

                    #update newest base pages at this column index to be this new page that was created

                    self.newest_base_pages[new_column_index] = pages[new_column_index]

                lock.release()

                added_new_base_page_range = True
        
        

        # add to tps list for new base page range if a new base page range was created.

        if added_new_base_page_range:
            self.tps_list[int(base_pages/self.num_columns_hidden)] = 0
            self.merge_in_progress[int(base_pages/self.num_columns_hidden)] = False
        
        self.page_directory[record.rid] = {'base':True, 'pages':pages, 'offsets':offsets}
        self.index.insert(record, record.rid)

        print(record.columns)
        return id


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

        lock = threading.Lock()
        lock.acquire()
        id = self.logger.log_update(record, base_rid, old_base_indirection, schema_encoding_base)
        lock.release()

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
        return id

    def delete_record(self, key):
        starting_rid = self.index.locate(key)[0]
        self.index.drop_record(starting_rid)

        current_rid = self.get_value(starting_rid, INDIRECTION_COLUMN)
        while(current_rid != starting_rid and current_rid != 1):
            self.set_value(0, current_rid, RID_COLUMN)
            current_rid = self.get_value(current_rid, INDIRECTION_COLUMN)

        self.set_value(0, starting_rid, RID_COLUMN)

    def undo_add(self, new_rid):
        try:
            del self.page_directory[new_rid]
            self.index.drop_record(new_rid)
        except:
            pass

    def undo_update(self, new_rid, old_rid, old_indirection, old_schema):
        try:
            del self.page_directory[new_rid]
        except:
            pass

        location = self.page_directory[old_rid]
        pages = location["pages"]
        offsets = location["offsets"]

        indirection_page = self.bpool.find_page(self.name, True, pages[INDIRECTION_COLUMN])
        self.bpool.pin_page(indirection_page)
        indirection_page.set_value(old_indirection, offsets[INDIRECTION_COLUMN])
        self.bpool.mark_dirty(indirection_page)
        self.bpool.unpin_page(indirection_page)

        schema_encoding_page = self.bpool.find_page(self.name, True, pages[SCHEMA_ENCODING_COLUMN])
        self.bpool.pin_page(schema_encoding_page)
        schema_encoding_page.set_value(old_schema, offsets[SCHEMA_ENCODING_COLUMN])
        self.bpool.mark_dirty(schema_encoding_page)
        self.bpool.unpin_page(schema_encoding_page)

    # store it into mergpool 
    # empty the mergepool 
    def merge(self, indirection_page_number):
        lock = threading.Lock()
        page_range = int((indirection_page_number+self.num_columns_hidden)/self.num_columns_hidden)

        base_pages = self.read_base_page_range(indirection_page_number)

        new_tps = 0
        for i in range(base_pages[0].num_records):
            base_rid = base_pages[1].read(i)
            base_pages[2].set_value(0, i)

            new_record, latest_tail_record = self.mpool_get_record(base_rid)

            if latest_tail_record > new_tps:
                new_tps = latest_tail_record
            
            for j in range(4, self.num_columns_hidden):
                base_pages[j].set_value(new_record[j-4], i)

        self.write_base_page_range(base_pages)

        lock.acquire()
        self.tps_list[page_range] = new_tps
        self.merge_in_progress[page_range] = False
        self.mpool.mpool = []
        lock.release()

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
    def read_base_page_range(self, indirection_page_number):
        cwd = os.getcwd()
        page_type = 'B'
        base_pages = []
        for i in range(self.num_columns_hidden):
            with open(cwd + "\ECS165\\" + self.name + "\\" + page_type + str(indirection_page_number+i), 'rb') as file:
                lines = file.read()

                page_find = Page("MB" + str(indirection_page_number+i), self.name)
                page_find.set_num_records(struct.unpack('Q', lines[0:8])[0])
                page_find.set_data(lines[8:])
                base_pages.append(page_find)
            
        return base_pages
