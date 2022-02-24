with open( ".\lstore\disk\Grades\T0-0", 'rb') as file:
        lines = file.read(5004)
        print(bytearray(lines[0:8]))