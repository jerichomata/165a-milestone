"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from asyncio.windows_events import NULL
from operator import contains
import this


def cmp(a, b):
    return (a > b) - (a < b)


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] *  table.num_columns
        pass


    def sorted_insert(self, record, base_rid):
        if len(self.indices[0]) == 0:
            for index in self.indices:
                index.append(base_rid)
            return

        for i, index in enumerate(self.indices):
            low = 0
            high = len(self.indices[0])-1
            mid = 0
            comparison = 0
            while (low < high):
                mid = int(high + low/2)
                comparison = cmp(record.columns[i],self.table.get_value(mid, i))
                if (comparison > 0):
                    low = mid + 1
                elif(comparison < 0):
                    high = mid - 1
                else:
                    break

            if (comparison > 0):
                mid += 1
            
            self.indices[i].insert(mid, record.columns[i])

    def drop_record(self, rid):
        for index in self.indices:
            if rid in index:
                index.remove(rid)

    def update(self, record, base_rid):
        self.drop_record(base_rid)
        self.sorted_insert(record, base_rid)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, index = this.table.primary_key_column):
        column = this.indices[index]
        startIndex = -1
        endIndex = -1
        low = 0
        high = len(column)
        #Uses Binary Search to the first index to cut the list.

        while (low <= high):
            mid = int((high-low)/2 + low)
            if (this.table.get_newest_value(column[mid], index) > begin):
                high = mid - 1;
            elif (this.table.get_newest_value(column[mid], index) == begin):
                startIndex = mid;
                high = mid - 1;
            else:
                low = mid + 1;
        #Uses Binary search again to find endIndex
        low = 0
        high = len(column)
        while (low <= high):
            mid = int((high-low)/2 + low)
            if (this.table.get_newest_value(column[mid], index) > end):
                high = mid - 1;
            elif (this.table.get_newest_value(column[mid], index) == end):
                endIndex = mid;
                low = mid + 1;
            else:
                low = mid + 1;
        if(startIndex == -1 or endIndex == -1):
            print("locate_range: Could not find value in list.")
            return NULL
        else:
            return (column[startIndex:(endIndex+1)])

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
