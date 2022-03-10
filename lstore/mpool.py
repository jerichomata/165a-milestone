from imp import acquire_lock, release_lock
from lstore.page import Page
import struct
import time
import os

class mergepool:

    #intialize a 3x5 bufferpool, this is an arbitrary size. 
    def __init__(self):
        #bufferpool that stores tuple ( page, time_accessed ).
        self.mpool = []
        self.path = []
        #both lists because bufferpool is so small that inefficent searching algorithms don't matter.
        self.dirty_pages = []

    #mark this page as dirty if it has been updated or recently created and not put to "disk".
    def mark_dirty(self, page):
        if page in self.mpool and page not in self.dirty_pages:
            # print("marking dirty", self.mpool[index][0].name)
            self.dirty_pages.append(page)

    #push all updates to "disk".
    def make_clean(self):
        while self.dirty_pages:
            page = self.dirty_pages.pop()
            cwd = os.getcwd()
            path = cwd + "\ECS165\\" + page[0].table_name
            try:
                os.mkdir(path)
            except OSError as error:
                pass
            self.write_page_to_disk(page[0], path)


    def write_page_to_disk(self, page, path):
        with open(path + "\\" + page.name, 'wb') as file:
            num_records = bytearray(8)
            num_records[0:8] = struct.pack('Q', page.num_records)
            file.write(num_records + page.data)


    def pin_page(self, page):
        if page in self.mpool and page not in self.pinned_pages:
            self.pinned_pages.append(page)

    def unpin_page(self, page):
        try:
            self.pinned_pages.remove(page)
        except:
            pass


    #add page to bufferpool because it has been read, updated, or created.
    def add_page(self, page):
        self.mpool.append(page)
        return page
        
        
    def exist_in_mpool(self, page_type, page_number):
        for i, page in enumerate(self.mpool):
            #i is a pair containing (page, time)
            if page.get_name() == page_type + str(page_number):
                return i
                
        return -1 

    def find_page(self, table_name, prefix, page_number):
        cwd = os.getcwd()

        if prefix:
            page_type = "B"
        else:
            page_type = "T"


        exist_index = self.exist_in_mpool(page_type, page_number)

        if(exist_index > -1):
            return self.mpool[exist_index]

        page_find = Page(page_type + str(page_number), table_name)

        with open(cwd + "\ECS165\\" + table_name + "\\" + page_type + str(page_number), 'rb') as file:
            lines = file.read()
            page_find.set_num_records(struct.unpack('Q', lines[0:8])[0])
            page_find.set_data(lines[8:])


        return self.add_page(page_find)