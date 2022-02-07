"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through self object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from asyncio.windows_events import NULL
from email.mime import base
from operator import contains


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
                if (self.indices[i][0] < record.columns[i]):
                    self.indices[i].append(base_rid)
                else:
                    self.indices[i].insert(0, base_rid)
            return

        for i, index in enumerate(self.indices):
            
            low = 0
            high = len(self.indices[i])-1
            mid = 0
            comparison = 0
            while (low < high):
                mid = int(high + low//2)
                comparison = cmp(record.columns[i], self.table.get_newest_value(self.indices[i][mid], i))
                if (comparison > 0):
                    low = mid + 1
                elif(comparison < 0):
                    high = mid - 1
                else:
                    break

            if (comparison >= 0):
                mid += 1
            self.indices[i].insert(mid, base_rid)
            
        

    def drop_record(self, rid):
        for i in range(len(self.indices)):
            if rid in self.indices[i]:
                self.indices[i].remove(rid)

    def update(self, record, base_rid):
        self.drop_record(base_rid)
        self.sorted_insert(record, base_rid)

    def print_keys(self):
        for item in self.indices[self.table.primary_key_column]:
            print(self.table.get_value(item, self.table.primary_key_column))

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, value, index = -1):
        if (index == -1):
            column = self.indices[self.table.primary_key_column]
            index = self.table.primary_key_column
        else:
            column = self.indices[index]
        records = []
        low = 0
        high = len(column)-1
        while(low <= high):
            mid = int((high-low)/2 + low)
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
            print("locate: Could not find value in list.")
            
            return None
        return records
        

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, index = -1):
        if (index == -1):
            column = self.indices[self.table.primary_key_column]
        else:
            column = self.indices[index]
        start_index = -1
        end_index = -1
        low = 0
        high = len(column)-1
        #Uses Binary Search to the first index to cut the list.

        while (low <= high):
            mid = int(high + low/2)
            if (self.table.get_newest_value(column[mid], index) > begin):
                high = mid - 1
            elif (self.table.get_newest_value(column[mid], index) == begin):
                start_index = mid
                while(self.table.get_newest_value(column[start_index], index) == begin):
                    start_index-=1
                break
            else:
                low = mid + 1
        #Uses Binary search again to find end_index
        low = 0
        high = len(column)-1
        while (low <= high):
            mid = int(high + low/2)
            if (self.table.get_newest_value(column[mid], index) > end):
                high = mid - 1
            elif (self.table.get_newest_value(column[mid], index) == end):
                end_index = mid
                while(self.table.get_newest_value(column[end_index], index) == end):
                    end_index-=1
                break
            else:
                low = mid + 1
        if(start_index == -1 or end_index == -1):
            print("locate_range: Could not find value in list.")
            print(str(begin) + " " + str(end))
            return None
        else:
            return (column[start_index:(end_index+1)])

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
