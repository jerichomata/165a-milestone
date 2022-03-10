from imp import acquire_lock, release_lock
from lstore.page import Page
import struct
import time
import os

class bufferpool:

    #intialize a 3x5 bufferpool, this is an arbitrary size. 
    def __init__(self):
        self.MAX_SIZE = 55
        #bufferpool that stores tuple ( page, time_accessed ).
        self.bpool = []
        self.path = []
        #both lists because bufferpool is so small that inefficent searching algorithms don't matter.
        self.pinned_pages = []
        self.dirty_pages = []

    #mark this page as dirty if it has been updated or recently created and not put to "disk".
    def mark_dirty(self, page):
        if page in self.bpool and page not in self.dirty_pages:
            # print("marking dirty", self.bpool[index][0].name)
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


    def make_clean2(self, page):
        cwd = os.getcwd()
        path = cwd + "\ECS165\\" + page.table_name
        try:
            os.mkdir(path)
        except OSError as error:
            pass
        self.write_page_to_disk(page, path)

    def write_page_to_disk(self, page, path):
        with open(path + "\\" + page.name, 'wb') as file:
            num_records = bytearray(8)
            num_records[0:8] = struct.pack('Q', page.num_records)
            file.write(num_records + page.data)


    def pin_page(self, page):
        if page in self.bpool and page not in self.pinned_pages:
            self.pinned_pages.append(page)

    def unpin_page(self, page):
        try:
            self.pinned_pages.remove(page)
        except:
            pass


    #add page to bufferpool because it has been read, updated, or created.
    def add_page(self, page):
        if(self.MAX_SIZE > len(self.bpool)):
            page_list = [page,time.time()]
            self.bpool.append(page_list)
            return page_list
        else:
            self.evict_page()
            page_list = [page,time.time()]
            self.bpool.append(page_list)
            return page_list
        
    def exist_in_bpool(self, page_type, page_number):
        for i, page in enumerate(self.bpool):
            #i is a pair containing (page, time)
            if page[0].get_name() == page_type + str(page_number):
                return i
                
        return -1 

    def find_page(self, table_name, prefix, page_number, merged=False):
        cwd = os.getcwd()
        page_type = "B"
        if(not prefix):
            page_type = "T"
        elif(merged):
            page_type = "MB"

        exist_index = self.exist_in_bpool(page_type, page_number)

        if(exist_index > -1):
            self.bpool[exist_index][1] = time.time()
            return self.bpool[exist_index]

        page_find = Page(page_type + str(page_number), table_name)

        with open(cwd + "\ECS165\\" + table_name + "\\" + page_type + str(page_number), 'rb') as file:
            lines = file.read()
            page_find.set_num_records(struct.unpack('Q', lines[:8])[0])
            page_find.set_data(lines[8:])


        return self.add_page(page_find)

    #evict page based on Least Recently Used page. 
    def evict_page(self):

        #before any pages can be evicted we must make all pages clean so that no uncommited changes are lost.
        # self.make_clean()
        #loop through bpool and sort the pages by recent usage, find the least recently used page that is unpinned and evict.
        #if all pages are pinned, (for whatever reason), return error code/msg.

        #since it's a tuple, we can just sort by 
        temp = self.bpool.copy()
        temp.sort(key = lambda x: x[1])

        #find first page in pages that that is not pinned.
        for pair in temp:
            if(pair not in self.pinned_pages):
                self.make_clean2(pair[0])
                self.bpool.remove(pair)
                return # print("page evicted ", pair[0].name)

        

        return print("all of the pages in the bufferpool are pinned. ")    