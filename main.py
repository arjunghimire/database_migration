# import pdb; pdb.set_trace()

import MySQLdb

def connect():
   return MySQLdb.connect("127.0.0.1","root","","opaxe")


def get_rows(con): 
   try:
      with con.cursor() as cursor:
         cursor.execute('SELECT * FROM company')
         header = [item[0] for item in cursor.description]
         rows = cursor.fetchall()
         return header, rows
   except:
      print("Error: unable to fecth data")
      
def get_value_pair(header, rows):
   result = []
   for row in rows:
      result.append({header[i]: value for i, value in enumerate(row)})
   return result

def update_rows(row):
   try:
      with con.cursor() as cursor:
         cursor.execute('''''')
         # rows = cursor.fetchall()
         return  rows
   except:
      print("Error: unable to fecth data")  

def main():
   con  = connect()
   header, rows = get_rows(con)
   dict_result = get_value_pair(header, rows)
   # print(dict_result)
   for row in dict_result:
      # print(row,row["co_id"],"\n")
      for key in row:


         print(key,"\n")

main()

