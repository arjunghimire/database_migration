import jaydebeapi
import MySQLdb
import json
import os
import ast
from time import localtime, strftime

def connect_mysql():
  mcon = MySQLdb.connect("127.0.0.1","root","","opaxe-test")
  mcursor = mcon.cursor()
  return mcursor,mcon


def connect_access(): 
  db_path = "/home/arjun/all.accdb"
  ucanaccess_jars = [
      "JDBC/UCanAccess/ucanaccess-4.0.4.jar",
      "JDBC/UCanAccess/lib/commons-lang-2.6.jar",
      "JDBC/UCanAccess/lib/commons-logging-1.1.3.jar",
      "JDBC/UCanAccess/lib/hsqldb.jar",
      "JDBC/UCanAccess/lib/jackcess-2.1.11.jar",
      ]
  classpath = ":".join(ucanaccess_jars)
  acon = jaydebeapi.connect(
    "net.ucanaccess.jdbc.UcanaccessDriver",
    f"jdbc:ucanaccess://{db_path};newDatabaseVersion=V2010",
    ["", ""],
    classpath)
  acursor = acon.cursor()
  return acursor,acon


def get_valid_data(x):
  if x == "" or x == "n/a":
    return None
  return x

def enum(x):
  if x == "N":
    return "n"
  return "y"

def current_date_time(x):
  if x == "" or x == "n/a":
    return strftime("%Y-%m-%d %H:%M:%S", localtime())
  return x

def get_region_id(val): 
  try:
    return list(filter(lambda i: i[1] == val, select_region_from_mysql()))[0][0]
  except IndexError:
    return None

def get_country_id(val):
  try:
    return list(filter(lambda i: i[1] == val, select_country_from_mysql()))[0][0]
  except IndexError:
    return None
  
def get_company_id(val):
  try:
    return list(filter(lambda i: i[1] == val, select_company_from_mysql()))[0][0]
  except IndexError:
    return None


def create_file(filename):  
  if(os.path.exists(filename)):
    return True
  with open(filename, "w") as f:
    f.write("")

def write_in_file(filename,func_name):
  file = create_file(filename)
  if not file:
    with open(filename, 'w') as f:
      return f.write(str(func_name()))



def read_file_data(filename):
  with open(filename, 'r') as f:
    data = eval(f.read())
    maps = []
    for x in data:
      s = []
      for l in x:
        try:
          if isinstance(l,int):
            s.append(l)
          else:
            s.append(l.encode('ascii', 'ignore').decode('unicode_escape'))
        except:
          s.append(l)
      maps.append(tuple(s))
    return maps

def select_region(): 
  acursor,acon = connect_access()
  acursor.execute("""select * from ddtbl_HO_region;""")
  result = acursor.fetchall()
  acursor.close()
  acon.close()
  return result



def insert_region(filename,func_name):
  res = [x for x in read_file_data(filename) if x]
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



def select_company(): 
  acursor,acon = connect_access()
  acursor.execute("select * from Company;")
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

def select_company_from_mysql(): 
  mcursor,mcon  = connect_mysql()
  mcursor.execute("select co_id,co_company_name from opx_report_companies;")
  result = mcursor.fetchall()
  mcursor.close()
  mcon.close()
  return result

def insert_additional_region(re_name):
  try:
    mcursor,mcon  = connect_mysql()
    mcursor.executemany("INSERT INTO opx_regions (re_name) VALUES (%s)", re_name)
    mcon.commit()
  except:
    pass

def insert_additional_country(cn_name,cn_region_name):
  try:
    mcursor,mcon  = connect_mysql()
    regions = mcursor.execute("INSERT into opx_countries (cn_name,cn_region_id) VALUES (%s,%s)", (cn_name,get_region_id(cn_region_name)))
    mcon.commit()
  except:
    pass

def insert_additional_company(co_company_name,co_head_office_country,co_head_office_region):
  try:
    mcursor,mcon  = connect_mysql()
    regions = mcursor.execute("INSERT into opx_report_companies (co_company_name,co_head_office_country,co_head_office_region) VALUES (%s,%s,%s)", (co_company_name,get_country_id(co_head_office_country),get_region_id(co_head_office_region)))
    mcon.commit()
  except:
    pass

def insert_region(filename,func_name):
  res = [x for x in read_file_data(filename) if x]
  mcursor,mcon  = connect_mysql()
  mcursor.executemany("INSERT INTO opx_regions (re_name) VALUES (%s)", res)
  mcon.commit()


def insert_country(filename,func_name):
  mcursor,mcon  = connect_mysql()
  for data in read_file_data(filename):
    regions = mcursor.execute("INSERT into opx_countries (cn_name,cn_region_id) VALUES (%s,%s)", (data[0],get_region_id(data[1])))
  mcon.commit()


def insert_company(filename,func_name):
  mcursor,mcon  = connect_mysql()
  for data in read_file_data(filename):
    if get_region_id(data[5]) == None:
      insert_additional_region(data[5])
    if get_country_id(data[4]) == None:
      insert_additional_country(data[4],data[5])
    companies = mcursor.execute("""INSERT into opx_report_companies (co_type,co_company_name,co_head_office_location,co_head_office_country,co_head_office_region,co_se1,	co_c1,co_se2,co_c2,co_se3,co_c3,co_se4,co_c4,co_website,co_subscribed,co_email,co_rsc_comment,co_parent_company,	co_former_company,co_subsidiaries,co_created_at,co_updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (data[1],data[2],data[3],get_country_id(data[4]),get_region_id(data[5]),data[6],data[7],data[8],data[9],data[10],data[11],data[12],data[13],data[14],data[15],data[16],data[17],data[18],data[19],data[20],data[21],data[21]))
  mcon.commit()

def insert_report(filename,func_name):
  mcursor,mcon  = connect_mysql()
  for data in read_file_data(filename):
    if get_country_id(data[31]) == None:
      insert_additional_country(data[31],data[30])
    if get_company_id(data[2]) == None:
      insert_additional_company(data[2],data[31],data[30])
    regions = mcursor.execute("""INSERT into opx_reports (re_announcement_date,re_company_id,re_project_name,re_deposit,re_format,re_release_filing_date,re_effective_date,re_resource_effective_date,re_reserve_effective_date,re_code,re_type,re_version,re_reported_resource_status,re_reserve_status,re_data,re_eia_esia_assessment,re_pdf_name,re_pdf,re_dropbox_link,re_url_name,re_value_chain_status,re_project_highest_resource_reserve_class_level,re_project_status,re_commodities,re_commodities_type_1,re_commodities_type_2,re_commodities_type_3,re_reported_commodity_price_usd,re_mineralisation_style,re_country_id,re_state,re_latitude,re_longitude,re_decimal_lattitude,re_decimal_longitude,re_latlon_id,re_overall_report_qp,re_overall_report_qp_affiliation,re_resource_qp,re_resource_qp_affiliation,re_reserve_qp,re_reserve_qp_affiliation,re_combined_cp_qp,re_highlights,re_deposit_resources,re_metres_drilled_in_report,re_total_meter_drilled,re_start_date_drill_programme,re_total_meters_in_programme,re_full_drill_data_available,re_resource_estimation_technique,re_resource_estimation_technique_2,re_geo_modelling_software,re_estimation_software,re_other_software,re_notes_on_software_leapfrog,re_implicit_modelling,re_dynamic_anisotrophy,re_dynamic_search,re_rotating_anisotropy,re_unfolding,re_uniform_conditioning,re_notes,re_table_1_present_jorc,re_supporting_cautionary_statements,re_cp_qp_statement,re_independent_qp_n143_101,re_unique_code,re_time_start,re_time_end,re_time,re_ret_project,re_id3,re_id4,re_id5,re_date_modified,	re_notes_on_other_software,re_qp_tmp,re_rsrc_qp_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (data[1],get_company_id(data[2]),get_valid_data(data[3]),get_valid_data(data[4]),get_valid_data(data[5]),current_date_time(data[6]),current_date_time(data[7]),current_date_time(data[8]),current_date_time(data[9]),get_valid_data(data[10]),get_valid_data(data[11]),get_valid_data(data[12]),get_valid_data(data[13]),get_valid_data(data[14]),get_valid_data(data[15]),get_valid_data(data[16]),get_valid_data(data[17]),get_valid_data(data[18]),get_valid_data(data[19]),get_valid_data(data[20]),get_valid_data(data[21]),get_valid_data(data[22]),get_valid_data(data[23]),get_valid_data(data[24]),get_valid_data(data[25]),get_valid_data(data[26]),get_valid_data(data[27]),get_valid_data(data[28]),get_valid_data(data[29]),get_country_id(data[31]),get_valid_data(data[30]),get_valid_data(data[33]),get_valid_data(data[34]),get_valid_data(data[35]),get_valid_data(data[36]),get_valid_data(data[37]),get_valid_data(data[38]),get_valid_data(data[39]),get_valid_data(data[40]),get_valid_data(data[41]),get_valid_data(data[42]),get_valid_data(data[43]),get_valid_data(data[44]),get_valid_data(data[45]),get_valid_data(data[46]),get_valid_data(data[48]),get_valid_data(data[49]),get_valid_data(data[50]),get_valid_data(data[51]),get_valid_data(data[52]),get_valid_data(data[53]),get_valid_data(data[54]),get_valid_data(data[55]),get_valid_data(data[56]),get_valid_data(data[57]), get_valid_data(data[58]),enum(data[59]),enum(data[60]),enum(data[61]),enum(data[62]),enum(data[63]),enum(data[64]),get_valid_data(data[65]),enum(data[66]),enum(data[67]),enum(data[68]),enum(data[69]),data[70],data[71],data[72],data[73],get_valid_data(data[74]),get_valid_data(data[75]),get_valid_data(data[76]),get_valid_data(data[77]),get_valid_data(data[78]),get_valid_data(data[80]),get_valid_data(data[81]),get_valid_data(data[82])))
    mcon.commit()
    # print(data[79],data[80],data[81],data[82])

# write_in_file("regions.txt",select_region)
# insert_region("regions.txt",select_region)
# write_in_file("countries.txt",select_country)
#insert_country("countries.txt",select_country)
# write_in_file("companies.txt",select_company)
# insert_company("companies.txt",select_company)

# write_in_file("reports.txt",select_report)
insert_report("reports.txt",select_report)
