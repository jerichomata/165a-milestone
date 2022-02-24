from imp import acquire_lock, release_lock
from lstore import Page
import time
import os

class bufferpool:

    #intialize a 3x5 bufferpool, this is an arbitrary size. 
    def __init__(self):
        MAX_SIZE = 5
        #bufferpool that stores tuple ( page, time_accessed ).
        self.bpool = []

        #both lists because bufferpool is so small that inefficent searching algorithms don't matter.
        self.pinned_pages = []
        self.dirty_pages = []

    #mark this page as dirty if it has been updated or recently created and not put to "disk".
    def mark_dirty(self, page):
        self.dirty_pages.append(page)

    #push all updates to "disk".
    def make_clean(self):
        for page in enumerate(self.dirty_pages):
            cwd = os.getcwd()
            path = cwd + "\\disk\\" + page.table
            os.mkdir(path)
            self.write_page_to_disk(page, path)
            self.dirty_pages.remove(page)

    def write_page_to_disk(self, page, path):
        with open(path + "\\" + page.name + ".txt", 'w') as file:
            file.write(page.num_records + " " + page.data.decode())


    def pin_page(self, page):
        self.pinned_pages.append(page)

    def unpin_page(self, page):
        self.pinned_pages.remove(page)

    #add page to bufferpool because it has been read, updated, or created.
    def add_page(self, page):

        if(self.exist_in_bpool(page) != -1):
            index = self.exist_in_bpool(page)
        else:
            index = len(self.bpool)
            if(self.MAX_SIZE > len(self.bpool)):
                self.bpool.append((page,time()))
            else:
                self.evict_page()
                self.bpool.append((page,time()))
        return index
        
    def exist_in_bpool(self, page):
        for i in self.bpool:
            #i is a pair containing (page, time)
            if i[0].get_name() == page.get_name():
                return i
        return -1

    def find_page(self, table_name, prefix, page_range, column):
        cwd = os.getcwd()
        page_type = "B"
        if(not prefix):
            page_type = "T"
        
        with open(cwd + "\\disk\\" + table_name + "\\" + page_type + page_range + "-" + column, 'r') as file:
            lines = file.readline()
            lines.split(" ")
            page_find = Page(page_type+page_range+"-"+column, table_name)
            page_find.set_num_records(lines[0])
            page_find.set_data(bytearray(lines[1].encode()))

        return self.add_page(page_find)

    #evict page based on Least Recently Used page. 
    def evict_page(self):

        #before any pages can be evicted we must make all pages clean so that no uncommited changes are lost.
        if not self.dirty_pages:
            self.make_clean()
 
        #loop through bpool and sort the pages by recent usage, find the least recently used page that is unpinned and evict.
        #if all pages are pinned, (for whatever reason), return error code/msg.

        #since it's a tuple, we can just sort by 
        self.bpool.sort(key = lambda x: x[1])

        #find first page in pages that that is not pinned.
        for pair in self.bpool:
            if(pair[0] not in self.pinned_pages):
                self.bpool.remove(pair)
                return print("page evicted.")

        return print("all of the pages in the bufferpool are pinned. ")    