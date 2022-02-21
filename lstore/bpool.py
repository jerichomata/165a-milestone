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
        for i, page in enumerate(self.dirty_pages):
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
        index = len(self.bpool)
        if(self.MAX_SIZE > len(self.bpool)):
            self.bpool.append(page)
        else:
            self.evict_page()
            self.bpool.append(page)
        return index
        

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

        


    #helper function for sort below.
    def sort_time(pair):
        return pair[1]

    #evict page based on Least Recently Used page. 
    def evict_page(self):
        pages = []

        #before any pages can be evicted we must make all pages clean so that no uncommited changes are lost.
        if not self.dirty_pages:
            self.make_clean()

        #loop through bpool and sort the pages by recent usage, find the least recently used page that is unpinned and evict.
        #if all pages are pinned, (for whatever reason), return error code/msg.
        for i, row in enumerate(self.bpool):
            for p in row:
                #page, row #.
                pages.append((p,i))

        pages.sort(key=self.sort_time)

        #find first page in pages that that is not pinned.
        for pair in pages:
            if(pair[0][0] not in self.pinned_pages):
                self.bpool[p[1]].remove(p)
                return print("page evicted.")

        return print("all of the pages in the bufferpool are pinned. ")    
        