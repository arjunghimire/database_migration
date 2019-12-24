import jaydebeapi
import MySQLdb

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

def select(): 
  acursor,acon = connect_access()
  acursor.execute("select * from ddtbl_HO_region;")
  result = acursor.fetchall()
  acursor.close()
  acon.close()
  return result

def insert():
  res = [x for x in select() if x]
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
print(select_country())