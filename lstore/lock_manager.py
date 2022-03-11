import pickle
import os

class LockManager:

    def __init__(self, table):
        self.table = table
        self.lock_table = {}
        self.count = {}

    def check_lock(self, key, query_type):
        if key not in self.lock_table.keys(): 
            self.lock_table[key] = "unlocked"
        result = self.lock_table[key] 
        if result == "unlocked":

            return True
        elif result == "locked":

            return False
        elif result == "shared" and query_type == "select":

            return True
        else:
            return False

    def get_lock(self, key):
        return self.lock_table[key]

    def set_shared(self, key):
        self.lock_table[key] = "shared"

    def set_lock(self, key):
        self.lock_table[key] = "locked"

    def release_lock(self, key):
        if(self.lock_table[key] == "locked"):
            self.lock_table[key] = "unlocked"
        elif(self.lock_table[key] == "shared" and self.count[key] == 0):
            print(self.count[key])
            self.lock_table[key] = "unlocked"
        else:
            pass
