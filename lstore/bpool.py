from imp import acquire_lock, release_lock
from lstore.page import Page
import struct
import time
import os

class bufferpool:

    #intialize a 3x5 bufferpool, this is an arbitrary size. 
    def __init__(self):
        self.MAX_SIZE = 20
        #bufferpool that stores tuple ( page, time_accessed ).
        self.bpool = []
        self.path = []
        #both lists because bufferpool is so small that inefficent searching algorithms don't matter.
        self.pinned_pages = []
        self.dirty_pages = []

    #mark this page as dirty if it has been updated or recently created and not put to "disk".
    def mark_dirty(self, index):
        if self.bpool[index] not in self.dirty_pages:
            self.dirty_pages.append(self.bpool[index])

    #push all updates to "disk".
    def make_clean(self):
        for page in self.dirty_pages:
            cwd = os.getcwd()
            path = cwd + "\lstore\disk\\" + page[0].table_name
            try:
                os.mkdir(path)
            except OSError as error:
                pass
            self.write_page_to_disk(page[0], path)
            self.dirty_pages.remove(page)

    def write_page_to_disk(self, page, path):
        with open(path + "\\" + page.name, 'wb') as file:
            num_records = bytearray(8)
            num_records[0:8] = struct.pack('Q', page.num_records)
            file.write(num_records + page.data)


    def pin_page(self, index):
        if self.bpool[index] not in self.pinned_pages:
            self.pinned_pages.append(self.bpool[index])

    def unpin_page(self, index):
        try:
            self.pinned_pages.remove(self.bpool[index])
        except:
            pass


    #add page to bufferpool because it has been read, updated, or created.
    def add_page(self, page):

        index = len(self.bpool)
        if(self.MAX_SIZE > len(self.bpool)):
            self.bpool.append([page,time.time()])
        else:
            self.evict_page()
            index = len(self.bpool)
            self.bpool.append([page,time.time()])
        return index
        
    def exist_in_bpool(self, page_type, page_range, column):
        for i, page in enumerate(self.bpool):
            #i is a pair containing (page, time)
            if page[0].get_name() == page_type + str(page_range) + "-" + str(column):
                return i
                
        return -1 

    def find_page(self, table_name, prefix, page_range, column):
        cwd = os.getcwd()
        page_type = "B"
        if(not prefix):
            page_type = "T"

        exist_index = self.exist_in_bpool(page_type, page_range, column)

        if(exist_index > -1):
            self.bpool[exist_index][1] = time.time()
            return exist_index

        with open(cwd + "\lstore\disk\\" + table_name + "\\" + page_type + str(page_range) + "-" + str(column), 'rb') as file:
            lines = file.read(5004)
            page_find = Page(page_type + str(page_range) + "-" + str(column), table_name)
            page_find.set_num_records(struct.unpack('Q', lines[0:8])[0])
            page_find.set_data(bytearray(lines[8:5004]))

        return self.add_page(page_find)

    #evict page based on Least Recently Used page. 
    def evict_page(self):

        #before any pages can be evicted we must make all pages clean so that no uncommited changes are lost.
        if self.dirty_pages:
            self.make_clean()
 
        #loop through bpool and sort the pages by recent usage, find the least recently used page that is unpinned and evict.
        #if all pages are pinned, (for whatever reason), return error code/msg.

        #since it's a tuple, we can just sort by 
        self.bpool.sort(key = lambda x: x[1])

        #find first page in pages that that is not pinned.
        for pair in self.bpool:
            if(pair[0] not in self.pinned_pages):
                self.bpool.remove(pair)
                return

        return print("all of the pages in the bufferpool are pinned. ")    