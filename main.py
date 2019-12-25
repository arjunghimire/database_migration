import jaydebeapi
import MySQLdb
import json
import imp
from data import access_countries,access_regions,access_companies


def connect_mysql():
  mcon = MySQLdb.connect("127.0.0.1","root","","opaxe")
  mcursor = mcon.cursor()
  return mcursor,mcon


def connect_access(): 
  db_path = "/home/arjun/all.accdb"
  ucanaccess_jars = [
      "/home/arjun/Downloads/JDBC/UCanAccess/ucanaccess-4.0.4.jar",
      "/home/arjun/Downloads/JDBC/UCanAccess/lib/commons-lang-2.6.jar",
      "/home/arjun/Downloads/JDBC/UCanAccess/lib/commons-logging-1.1.3.jar",
      "/home/arjun/Downloads/JDBC/UCanAccess/lib/hsqldb.jar",
      "/home/arjun/Downloads/JDBC/UCanAccess/lib/jackcess-2.1.11.jar",
      ]
  classpath = ":".join(ucanaccess_jars)
  acon = jaydebeapi.connect(
    "net.ucanaccess.jdbc.UcanaccessDriver",
    f"jdbc:ucanaccess://{db_path};newDatabaseVersion=V2010",
    ["", ""],
    classpath)
  acursor = acon.cursor()
  return acursor,acon

def select_region(): 
  acursor,acon = connect_access()
  acursor.execute("select * from ddtbl_HO_region;")
  result = acursor.fetchall()
  acursor.close()
  acon.close()
  return result

def insert_region():
  res = [x for x in select_region() if x]
  mcursor,mcon  = connect_mysql()
  mcursor.executemany("INSERT INTO opx_regions (re_name) VALUES (%s)", res)
  mcon.commit()


def select_country(): 
  acursor,acon = connect_access()
  acursor.execute("select * from ddtbl_HO_country;")
  result = acursor.fetchall()
  acursor.close()
  acon.close()
  return result

def select_region_from_mysql(): 
  mcursor,mcon  = connect_mysql()
  mcursor.execute("select re_id,re_name from opx_regions;")
  result = mcursor.fetchall()
  mcursor.close()
  mcon.close()
  return result

get_num = lambda val: list(filter(lambda i: i[1] == val, select_region_from_mysql()))[0][0]

def insert_country():
  mcursor,mcon  = connect_mysql()
  for data in access_countries:
    regions = mcursor.execute("INSERT into opx_countries (cn_name,cn_region_id) VALUES (%s,%s)", (data[0],get_num(data[1])))
  mcon.commit()

def select_company(): 
  acursor,acon = connect_access()
  acursor.execute("select * from Company;")
  result = acursor.fetchall()
  acursor.close()
  acon.close()
  return result

def insert_company():
  mcursor,mcon  = connect_mysql()
  for data in access_companies[:2]:
    regions = mcursor.execute("""INSERT into opx_report_companies (co_type,co_company_name,co_head_office_location,co_head_office_country,co_head_office_region,co_se1,	co_c1,co_se2,co_c2) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (data[1],data[2],data[3],data[4],data[5],data[6],data[7],data[8],data[9]))
  mcon.commit()

print(access_companies[:1])
insert_company()
# print(select_region())
