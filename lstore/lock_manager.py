import pickle
import os
import threading
class LockManager:

    def __init__(self):
        self.write_lock_table = {}
        self.read_lock_table = {}
        self.threading_lock = threading.Lock()

    def __getstate__(self):
        return

    def __setstate__(self):
        self.write_lock_table = {}
        self.read_lock_table = {}
        self.threading_lock = threading.Lock()

    def check_lock(self, rid, query_type):
        self.threading_lock.acquire()
        if rid not in self.write_lock_table.keys(): 
            self.write_lock_table[rid] = False

        if rid not in self.read_lock_table.keys():
            self.read_lock_table[rid] = False
        write_result = self.write_lock_table[rid]
        read_result = self.read_lock_table[rid]
        if read_result > 0 and (query_type == "insert" or query_type == "update"):
            self.threading_lock.release()
            return False
        elif write_result == False and query_type == "select":
            self.threading_lock.release()
            return True
        elif write_result == True:
            self.threading_lock.release()
            return False
        else:
            self.threading_lock.release()
            return True

    def get_lock(self, rid):
        return self.write_lock_table[rid]

    def set_shared(self, rid):
        if rid not in self.read_lock_table.keys():
            self.read_lock_table[rid] = 1
        else:
            self.read_lock_table[rid] += 1

    def release_shared(self, rid):
        if rid not in self.read_lock_table.keys():
            self.read_lock_table[rid] = 0
        else:
            self.read_lock_table[rid] -= 1

    def set_lock(self, rid):
        self.write_lock_table[rid] = True

    def release_lock(self, rid):
        if(self.write_lock_table[rid] == True):
            self.write_lock_table[rid] = False
