import jaydebeapi
import MySQLdb
import json
import imp
import ast
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
  res = [x for x in access_regions if x]
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

def select_report(): 
  acursor,acon = connect_access()
  acursor.execute("select * from Reports;")
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

def select_country_from_mysql(): 
  mcursor,mcon  = connect_mysql()
  mcursor.execute("select cn_id,cn_name from opx_countries;")
  result = mcursor.fetchall()
  mcursor.close()
  mcon.close()
  return result

get_region_id = lambda val: list(filter(lambda i: i[1] == val, select_region_from_mysql()))[0][0]
get_country_id = lambda val: list(filter(lambda i: i[1] == val, select_country_from_mysql()))[0][0]

def insert_country():
  mcursor,mcon  = connect_mysql()
  for data in access_countries:
    regions = mcursor.execute("INSERT into opx_countries (cn_name,cn_region_id) VALUES (%s,%s)", (data[0],get_region_id(data[1])))
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
  for data in access_companies[:25]:
    print(data,"\n")
    regions = mcursor.execute("""INSERT into opx_report_companies (co_type,co_company_name,co_head_office_location,co_head_office_country,co_head_office_region,co_se1,	co_c1,co_se2,co_c2,co_se3,co_c3,co_se4,co_c4,co_website,co_subscribed,co_email,co_rsc_comment,co_parent_company,	co_former_company,co_subsidiaries,co_created_at,co_updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (data[1],data[2],data[3],get_country_id(data[4]),get_region_id(data[5]),data[6],data[7],data[8],data[9],data[10],data[11],data[12],data[13],data[14],data[15],data[16],data[17],data[18],data[19],data[20],data[21],data[21]))
  mcon.commit()

def read_report_from_txt():
  with open("report.txt", 'r') as f:
    return  ast.literal_eval(f.read())


def write_report_to_txt():
  with open("report.txt", 'w') as f:
    f.write(str(select_report()))


def insert_report():
  mcursor,mcon  = connect_mysql()
  for data in read_report_from_txt()[:2]:
    regions = mcursor.execute("""INSERT into opx_reports (re_company_name,re_project_name,re_deposit,re_format,	re_code,re_type,re_version,re_reported_resource_status,re_reserve_status,re_data,re_eia_esia_assessment,re_pdf_name,re_pdf,re_dropbox_link,re_url_name,re_value_chain_status,re_project_highest_resource_reserve_class_level,re_project_status,re_commodities,re_commodities_type_1,re_commodities_type_2,re_commodities_type_3,re_reported_commodity_price_usd,re_mineralisation_style) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (data[2],data[3],data[4],data[5],data[10],data[11],data[12],data[13],data[14],data[15],data[16],data[17],data[18],data[19],data[20],data[21],data[22],data[23],data[24],data[25],data[26],data[27],data[28],data[29]))
    print(data,"\n")
    print(data[8],'\n')
  mcon.commit()


insert_report()