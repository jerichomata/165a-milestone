import itertools

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through self object. Indices are usually B-Trees, but other data structures can be used as well.
"""



class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = {self.table.primary_key_column:{}}
        pass      
        

    def insert(self, record, base_rid):
        for key, value in self.indices.items():
            if record.columns[key+4] in value.keys():
                value[record.columns[key+4]].append(base_rid)
            else:
                value[record.columns[key+4]] = [base_rid]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, value, index = -1):
        if index == -1:
            index = self.table.primary_key_column
        found_index = self.indices[index]
        if found_index == None:
            self.create_index(index)
            found_index = len(self.indices) - 1

        found_rid = found_index.get(value)
        if found_rid == None:
            print("could not find", value, "in", index)
            return None
        else:
            return found_rid

        

        

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, index = -1):
        if index == -1:
            index = self.table.primary_key_column

        found_index = self.indices.get(index)
        if found_index == None:
            self.create_index(index)
            found_index = len(self.indices) - 1

        keys_in_range = [key for key in found_index.keys() if begin <= key <= end]

        rids = [found_index[key] for key in keys_in_range]
        rids = [rid for sublist in rids for rid in sublist]
        return rids
            

    def update(self, schema_encoding, new_record, base_rid):

        for i, bit in enumerate(schema_encoding):
            if bit == 1:

                listOfValues = list(self.indices[i].keys())
                listOfRids = list(self.indices[i].values())
                del self.indices[i][listOfValues[listOfRids.index(base_rid)]]

                if new_record.columns[i] in self.indices[i].keys():
                    self.indices[i][new_record.columns[i]].append(base_rid)
                else:
                    self.indices[i][new_record.columns[i]] = [base_rid]

    def drop_record(self, base_rid):
        for key, value in self.indices.items():
            value_at_base_rid = self.table.get_newest_value(base_rid, key+4)
            value[value_at_base_rid].remove(base_rid)
            if len(value[value_at_base_rid]) == 0:
                del value[value_at_base_rid]

            




    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        new_index = {}
        for entry in self.indices[self.table.primary_key_column].values():
            for rid in entry:
                new_index[self.table.get_newest_value(rid, column_number+4)] = [rid]

        self.indices[column_number] = new_index


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        del self.indices[column_number]
