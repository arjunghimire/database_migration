# # import pdb; pdb.set_trace()

# import MySQLdb

# def connect():
#    return MySQLdb.connect("127.0.0.1","root","","opaxe")


# def get_rows(con): 
#    try:
#          cursor = con.cursor() 
#          cursor.execute('SELECT * FROM company')
#          header = [item[0] for item in cursor.description]
#          rows = cursor.fetchall()
#          print(header)
#          return header, rows
#    except:
#       print("Error: unable to fecth data")
      
# def get_value_pair(header, rows):
#    result = []
#    for row in rows:
#       result.append({header[i]: value for i, value in enumerate(row)})
#    return result

# def update_rows(con,key_name):
#    try:
#          cursor = con.cursor() 
#          print(key_name,"\n")
#          # cursor.execute('UPDATE company SET {0} = CASE WHEN {0} = "" THEN NULL ELSE {0} END '.format(key_name))
#          return True
#    except Exception as exec:
#       print("Error: unable to fecth data",exec)  

# def main():
#    con  = connect()
#    header,rows = get_rows(con)
#    # dict_result = get_value_pair(header, rows)
#    # print(dict_result)
#    for key in header:
#       if key == "co_created_at" or key == "co_updated_at" or key == "co_report_link_url" or key == "co_additional_comment":
#          return True
#       else:
#          update_rows(con,key)   
#    con.close()
# main()
