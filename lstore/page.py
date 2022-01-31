
from operator import truediv
import struct


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if(self.num_records * 8 < len(self.data)):
            return True
        return False


    def write(self, value):
        if self.has_capacity():
            self.data[self.num_records * 8: self.num_records*8 + 8] = struct.pack('Q', value)
            self.num_records += 1
            print("Written" + str(value))
            return self.num_records - 1
        else:
            print("Could not write")

    def read(self, offset):
        return struct.unpack('Q', self.data[offset * 8 : offset*8 + 8])


