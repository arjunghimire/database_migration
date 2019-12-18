import csv
import MySQLdb
import pandas

mydb = MySQLdb.connect("127.0.0.1", "root", "", "opaxe")
cursor = mydb.cursor()

csv_data = csv.reader(open('reports.csv'))
header = pandas.read_csv('reports.csv', skiprows=0, low_memory=False)
reports_data = pandas.read_csv('reports.csv', skiprows=1, low_memory=False)
print(reports_data)
# for row in reports_data:  
#     pass
#     print(row,"\n")
    # cursor.execute('INSERT INTO testcsv(names, \
    #       classes, mark )' \
    #       'VALUES("%s", "%s", "%s")',  
    #       row)
# close the connection to the database.
mydb.commit()
cursor.close()
