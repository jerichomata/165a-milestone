from lstore.db import Database
from lstore.query import Query

def demo():
    demo = Database()
    demo_table = demo.create_table('Grades', 5, 0)
    demo_query = Query(demo_table)

    while (1):
        userInput = input("MILESTONE 1 >> ")
        userCmd = userInput.split(' ')

        if userCmd[0] == "exit":
            print("Have a nice day!")
            break
        elif userCmd[0] == "table":
            print( "{demo_table.name} with {demo_table.num_colums} columns and {demo_table.key} key" )
            break
        elif userCmd[0] == "delete":
            grade = int(userCmd[1])
            demo_del = demo_query(grade)
            if demo_del:
                print("You have deleted", grade)
            else:
                print("Unfortunately grade has not been deleted") 
        elif userCmd[0] == "sum":
            startRange = int(userCmd[1])
            endRange = int(userCmd[2])
            aggrCol = int(userCmd[3])
            demo_sum = demo_query.sum(startRange, endRange, aggrCol)
            if (demo_sum == False):
                print("Grades have not been added")
            else:
                print("Calculated sum is", demo_sum)
        else:
            print("COMMAND NOT FOUND") 

    return

demo()
