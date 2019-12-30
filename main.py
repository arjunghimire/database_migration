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


def get_valid_data(x):
  if x == "" or x == "n/a":
    return None
  return x

def enum(x):
  if x == "N":
    return "n"
  return "y"

get_region_id = lambda val: list(filter(lambda i: i[1] == val, select_region_from_mysql()))[0][0]
get_country_id = lambda val: list(filter(lambda i: i[1] == val, select_country_from_mysql()))[0][0]



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
  for data in read_report_from_txt()[:10]:
    regions = mcursor.execute("""INSERT into opx_reports (re_company_name,re_project_name,re_deposit,re_format,	re_code,re_type,re_version,re_reported_resource_status,re_reserve_status,re_data,re_eia_esia_assessment,re_pdf_name,re_pdf,re_dropbox_link,re_url_name,re_value_chain_status,re_project_highest_resource_reserve_class_level,re_project_status,re_commodities,re_commodities_type_1,re_commodities_type_2,re_commodities_type_3,re_reported_commodity_price_usd,re_mineralisation_style,re_country_id,re_state,re_latitude,re_longitude,re_decimal_lattitude,re_decimal_longitude,re_latlon_id,re_overall_report_qp,re_overall_report_qp_affiliation,re_resource_qp,re_resource_qp_affiliation,re_reserve_qp,re_reserve_qp_affiliation,re_combined_cp_qp,re_highlights,re_deposit_resources,re_summary,re_metres_drilled_in_report,re_total_meter_drilled,re_start_date_drill_programme,re_total_meters_in_programme,re_full_drill_data_available,re_resource_estimation_technique,re_resource_estimation_technique_2,re_geo_modelling_software,re_estimation_software,re_other_software,re_notes_on_software_leapfrog,re_implicit_modelling,re_dynamic_anisotrophy,re_dynamic_search,re_rotating_anisotropy,re_unfolding,re_uniform_conditioning,re_notes,re_table_1_present_jorc,re_supporting_cautionary_statements,re_cp_qp_statement,re_independent_qp_n143_101,re_unique_code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (get_valid_data(data[2]),get_valid_data(data[3]),get_valid_data(data[4]),get_valid_data(data[5]),get_valid_data(data[10]),get_valid_data(data[11]),get_valid_data(data[12]),get_valid_data(data[13]),get_valid_data(data[14]),get_valid_data(data[15]),get_valid_data(data[16]),get_valid_data(data[17]),get_valid_data(data[18]),get_valid_data(data[19]),get_valid_data(data[20]),get_valid_data(data[21]),get_valid_data(data[22]),get_valid_data(data[23]),get_valid_data(data[24]),get_valid_data(data[25]),get_valid_data(data[26]),get_valid_data(data[27]),get_valid_data(data[28]),get_valid_data(data[29]),get_country_id(data[31]),get_valid_data(data[30]),get_valid_data(data[33]),get_valid_data(data[34]),get_valid_data(data[35]),get_valid_data(data[36]),get_valid_data(data[37]),get_valid_data(data[38]),get_valid_data(data[39]),get_valid_data(data[40]),get_valid_data(data[41]),get_valid_data(data[42]),get_valid_data(data[43]),get_valid_data(data[44]),get_valid_data(data[45]),get_valid_data(data[46]),get_valid_data(data[47]),get_valid_data(data[48]),get_valid_data(data[49]),get_valid_data(data[50]),get_valid_data(data[51]),get_valid_data(data[52]),get_valid_data(data[53]),get_valid_data(data[54]),get_valid_data(data[55]),get_valid_data(data[56]),get_valid_data(data[57]), get_valid_data(data[58]),enum(data[59]),enum(data[60]),enum(data[61]),enum(data[62]),enum(data[63]),enum(data[64]),get_valid_data(data[65]),enum(data[66]),enum(data[67]),enum(data[68]),enum(data[69]),data[70]))
    print(data[31],"\n")
  mcon.commit()


insert_report()