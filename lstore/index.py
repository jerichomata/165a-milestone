import itertools

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through self object. Indices are usually B-Trees, but other data structures can be used as well.
"""



class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = {self.table.primary_key_column_hidden:{}}
        pass      
        

    def insert(self, record, base_rid):
        for key, value in self.indices.items():
            if record.columns[key] in value.keys():
                value[record.columns[key]].append(base_rid)
            else:
                value[record.columns[key]] = [base_rid]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, value, index = -1):
        if index == -1:
            index = self.table.primary_key_column_hidden
        
        found_index = self.indices.get(index)
        if found_index == None:
            self.create_index(index)
            found_index = len(self.indices) - 1

        found_rid = self.indices[found_index].get(value)
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
            index = self.primary_key_column_hidden

        found_index = self.indices.get(index)
        if found_index == None:
            self.create_index(index)
            found_index = len(self.indices) - 1
        
        range_of_index = dict(itertools.islice(self.indices[found_index].items(), begin, end))
        return [x for x in range_of_index.values]
            
    #TODO
    def update(self, old_record, new_record, base_rid):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices_references.append(column_number)
        new_index = {}
        for rid in self.indices[0].values():
            new_index[self.table.get_newest_value(rid, column_number)] = [rid]

        self.indices.append(new_index)


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        index = self.indices_references.find(column_number)
        if index > -1:
            del self.indices_references[index]
            del self.indices[index]
