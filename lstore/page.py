
from operator import truediv
import struct


class Page:

    def __init__(self, name, table_name):
        self.name = name
        self.table_name = table_name
        self.num_records = 0
        self.data = bytearray(4096)

    def set_num_records(self, value):
        self.num_records = value

    def get_name(self):
        return self.name
        
    def set_data(self, data):
        self.data = data
    
    def has_capacity(self):
        if(self.num_records * 8 < len(self.data)):
            return True
        return False


    def write(self, value):
        if self.has_capacity():
            value = int(value)
            self.data[self.num_records * 8: self.num_records*8 + 8] = struct.pack('Q', value)
            self.num_records += 1
            # print("Written" + str(value))
        else:
            print("Could not write")


    def read(self, offset):
        return struct.unpack('Q', self.data[offset * 8 : offset*8 + 8])[0]

    def set_value(self, value, offset):
        if offset < self.num_records:  
            self.data[offset * 8:offset * 8 + 8] = struct.pack('Q', value)
        else:
            print("Could not write")


