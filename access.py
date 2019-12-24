import jaydebeapi

db_path = "/home/arjun/all.accdb"

ucanaccess_jars = [
    "/home/arjun/Downloads/JDBC/UCanAccess/ucanaccess-4.0.4.jar",
    "/home/arjun/Downloads/JDBC/UCanAccess/lib/commons-lang-2.6.jar",
    "/home/arjun/Downloads/JDBC/UCanAccess/lib/commons-logging-1.1.3.jar",
    "/home/arjun/Downloads/JDBC/UCanAccess/lib/hsqldb.jar",
    "/home/arjun/Downloads/JDBC/UCanAccess/lib/jackcess-2.1.11.jar",
    ]
classpath = ":".join(ucanaccess_jars)
cnxn = jaydebeapi.connect(
    "net.ucanaccess.jdbc.UcanaccessDriver",
    f"jdbc:ucanaccess://{db_path};newDatabaseVersion=V2010",
    ["", ""],
    classpath
    )
crsr = cnxn.cursor()
try:
    crsr.execute("select * from CustomerT")
    cnxn.commit()
except jaydebeapi.DatabaseError as de:
    if "user lacks privilege or object not found: TABLE1" in str(de):
        pass
    else:
        raise
# crsr.execute("CREATE TABLE table1 (id COUNTER PRIMARY KEY, fname TEXT(50))")
# cnxn.commit()
# crsr.execute("INSERT INTO table1 (fname) VALUES ('arjun')")
# cnxn.commit()
# crsr.execute("SELECT * FROM table1")
for row in crsr.fetchall():
    print(row)
crsr.close()
cnxn.close()