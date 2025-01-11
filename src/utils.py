# Finds out the index of "name" in the array firstLine
# returns -1 if it cannot find it
def find_col(col, name):
    return col.index(name)

# display the first n lines
def display_x(d, n):
    lines = d.take(n)
    for line in lines:
        print(line)
    print()

# display all the data
def display_all(d):
    for line in d.collect():
        print(line)
    print()
