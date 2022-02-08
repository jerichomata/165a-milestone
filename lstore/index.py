"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through self object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from asyncio.windows_events import NULL
from email.mime import base
from operator import contains
from tkinter.tix import COLUMN


def cmp(a, b):
    return (a > b) - (a < b)


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = []
        pass


    def sorted_insert(self, record, base_rid):
        if (not self.indices):
            for i in range(self.table.num_columns):
                temp_list = [base_rid]
                self.indices.append(temp_list)
            return
        
        if (len(self.indices[0]) == 1):
            for i in range(self.table.num_columns):
                if (self.table.get_newest_value(self.indices[i][0], i) < record.columns[i]):
                    self.indices[i].append(base_rid)
                else:
                    self.indices[i].insert(0, base_rid)
            return

        for i, index in enumerate(self.indices):
            #loc to insert
            location = self.find_insert_location(self.indices[i], record.columns[i], i)
            self.indices[i].insert(location, base_rid)
                

    def find_insert_location(self, column, value, index):
        low = 0
        high = len(column)-1
        while (low <= high):
            mid = low + (high - low) // 2
            if (value == self.table.get_newest_value(column[mid], index)):
                return mid + 1
            elif (value > self.table.get_newest_value(column[mid], index)):
                low = mid + 1
            else:
                high = mid - 1
        return low
        

    def drop_record(self, rid):
        for i in range(len(self.indices)):
            self.indices[i].remove(rid)

    def update(self, record, base_rid):
        self.drop_record(base_rid)
        self.sorted_insert(record, base_rid)

    def print_keys(self, column):
        for item in self.indices[column]:
            print(self.table.get_value(item, column))

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, value, index = -1):
        if (index == -1):
            index = self.table.primary_key_column_hidden
            
        column = self.indices[index]
        records = []
        low = 0
        high = len(column)-1
        while(low <= high):
            mid = int((low+high)//2)
            if (self.table.get_newest_value(column[mid], index) > value):
                high = mid - 1
            elif (self.table.get_newest_value(column[mid], index) == value):
                records.append(column[mid])
                current_mid = mid
                while(self.table.get_newest_value(column[current_mid], index) == value and current_mid < high):
                    current_mid+=1
                    if(self.table.get_newest_value(column[current_mid], index) == value):
                        records.append(column[current_mid])
                current_mid = mid
                while(self.table.get_newest_value(column[current_mid], index) == value and current_mid > low):
                    current_mid-=1
                    if(self.table.get_newest_value(column[current_mid], index) == value):
                        records.append(column[current_mid])
                break
            else:
                low = mid + 1
        if(len(records) == 0):
            print("locate: Could not find" + str(value) + "in list.")
            
            return None
        return records
        

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, index = -1):
        if (index == -1):
            index = self.table.primary_key_column_hidden  
        column = self.indices[index]

        start_index = self.first(column, begin, index, len(column))
        end_index = self.last(column, end, index, len(column))+1

        if(start_index == -1 or end_index == -1):
            print("locate_range: Could not find value in list.")
            return None
            
        else:
            return((column[start_index:end_index]))
    """
    # Find first occurence in column
    """

    def first(self, column, value, index, n):
        
        low = 0
        high = n - 1
        res = -1
        
        while (low <= high):
            
            # Normal Binary Search Logic
            mid = (low + high) // 2
            
            if self.table.get_newest_value(column[mid],index) > value:
                high = mid - 1
            elif self.table.get_newest_value(column[mid],index) < value:
                low = mid + 1
                
            # If arr[mid] is same as x, we
            # update res and move to the left
            # half.
            else:
                res = mid
                high = mid - 1
    
        return res
    

    def last(self, column, value, index, n):
        low = 0
        high = n - 1
        res = -1
        
        while(low <= high):
            
            # Normal Binary Search Logic
            mid = (low + high) // 2
            
            if self.table.get_newest_value(column[mid],index) > value:
                high = mid - 1
            elif self.table.get_newest_value(column[mid],index) < value:
                low = mid + 1
                
            # If arr[mid] is same as x, we
            # update res and move to the Right
            # half.
            else:
                res = mid
                low = mid + 1
    
        return res

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
