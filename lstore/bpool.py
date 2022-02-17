import table

class bufferpool:

    #intialize a 3x5 bufferpool, this is an arbitrary size. 
    def __init__(self):
        MAX_HEIGHT = 3
        MAX_WIDTH = 5
        bpool = [[],[],[]]

        #both lists because bufferpool is so small that inefficent searching algorithms don't matter.
        pinned_pages = []
        dirty_pages = []

    #mark this page as dirty if it has been updated or recently created and not put to "disk".
    def mark_dirty(self, page):
        pass

    #push all updates and creations to "disk".
    def make_clean(self, dirty_pages):
        pass

    def pin_page(self, page):
        self.pinned_pages.append(page)

    def unpin_page(self, page):
        self.pinned_pages.remove(page)

    #add page to bufferpool because it has been read, updated, or created.
    def add_page(self, page):
        pass

    #evict page based on eviction protocal of choice. 
    def evict_page(self, page):
        pass