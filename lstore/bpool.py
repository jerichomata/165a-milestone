import time

class bufferpool:

    #intialize a 3x5 bufferpool, this is an arbitrary size. 
    def __init__(self):
        MAX_HEIGHT = 3
        MAX_WIDTH = 5
        #bufferpool that stores tuple ( page, time_accessed ).
        bpool = [[],[],[]]

        #both lists because bufferpool is so small that inefficent searching algorithms don't matter.
        pinned_pages = []
        dirty_pages = []

    #mark this page as dirty if it has been updated or recently created and not put to "disk".
    def mark_dirty(self, page):
        self.dirty_pages.append(page)

    #push all updates to "disk".
    def make_clean(self):
        pass

    def pin_page(self, page):
        self.pinned_pages.append(page)

    def unpin_page(self, page):
        self.pinned_pages.remove(page)

    #add page to bufferpool because it has been read, updated, or created.
    def add_page(self, page):
        for i, row in enumerate(self.bpool):
            if len(row) < 5:
                row.append(page, time.time())
            elif i == 2:
                self.evict_page()
                self.add_page(page)  


    #helper function for sort below.
    def sort_time(pair):
        return pair[1]

    #evict page based on Least Recently Used page. 
    def evict_page(self):
        pages = []

        #before any pages can be evicted we must make all pages clean so that no uncommited changes are lost.
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
        
