"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from asyncio.windows_events import NULL


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        startIndex = -1
        endIndex = -1
        low = 0
        high = len(column)
        #Uses Binary Search to the first index to cut the list.

        while (low <= high):
            mid = int((high-low)/2 + low)
            if (lookup(column[mid]) > begin):
                high = mid - 1;
            elif (lookup(column[mid] == begin):
                startIndex = mid;
                high = mid - 1;
            else:
                low = mid + 1;
        #Uses Binary search again to find endIndex
        low = 0
        high = len(a)
        while (low <= high):
            mid = int((high-low)/2 + low)
            if (lookup(column[mid] > end):
                high = mid - 1;
            elif (lookup(column[mid] == end):
                endIndex = mid;
                low = mid - 1;
            else:
                low = mid + 1;
        if(startIndex == -1 or endIndex == -1):
            return NULL
        else:
            return (column[startIndex:(endIndex+1])

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
