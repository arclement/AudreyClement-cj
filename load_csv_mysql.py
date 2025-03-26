"""
Contact Master List Update Methodology
To update the master list--master_list_plus--from information stored in new/modified worksheets:
1)  extract the updated worksheet as a .csv file from the latest contacts Excel workbook and save it to a separate source directory, e.g.:
    c:\\users\\owner\\dropbox\\arc_candidate\\outreach\\contacts\\source\\
    Specify the path to that directory on the command line to load_csv, e.g.
    load_csv('environmentalists',['first', 'last', 'email', 'address', 'phone', 'phone2', 'organization'],
        path='c:\\users\\owner\\dropbox\\arc_candidate\\outreach\\contacts\\source\\')
2)  rerun load_csv with
    tablename=[name of updated workbook sheet]
    fieldlist=[comma, quote delimited list of columns in the worksheet]
    sheet=''
    sheetlist=''
3)  rerun load_csv with 
    tablename=all_edits_plus
    fieldlist=[the comma, quote delimited list of columns in all_edits_plus.csv]
    sheet=[name of updated workbook sheet]
    sheetlist=[the comma, quote delimited list of columns in the sheet, 
    INCLUDING ONLY THOSE FIELDS THAT ARE IN ALL_EDITS_PLUS]
4)  rerun load_csv to reload csv_ordered.csv 
    modified if necessary to add the names and ordering of new sheets.
5)  DITTO for deceased.csv
6)  rerun function build_master_from_all_edits_plus
7)  rerun function augment_table_from_all_edits_plus 
8)  upload the split up master file .csv files to corresponding lists in Constant Contact.

Contact Master List Creation Methodology
Function load_csv:
For each sheet in the contact workbook, create a MySQL extract table via function load_csv.

Manual intervention:
Concatenate MySQL extract tables of all sheets with fields seqno, source, last, first and email.
Sort the consolidated extract on last, first and email, export the consolidated extract and 
apply the same email address to all records for same person.

Function merge_edits:
Merge each sheet loaded into a MySQL table with the edited, consolidated extract on sequence number
and source to apply email edits to extract tables stored in MySQL database.

Function concatenate:
Performs 2 tasks: 

Task 1 consolidates csv sheets loaded into MySQL tables according to an ordered list of tables 
and dumps the consolidated output file into a single .csv file for inspection and/or update.

Task 2 dedupes the consolidated output file on last, first and email according to
the latest entry (i.e. highest seqno) from the highest order table (i.e. lowest tableorder)
according to the order list of tables.

The call is like so:
concatenate_tables(tablelist, tablename, task = 1)
    where tablelist is the ordered list of tables, e.g. csv_ordered.csv
    tablename is the name of the consolidated output file, e.g. all_csv or master_list
    and task is either 1 or 2, defaulting to 1.
    
Function augment_table:
Applies additional information, i.e. email2, phone, phone2 and address, to master_list and/or all_edits tables
by merging all_edits with master_list on source and seqno and by aggregating the same information on all_edits
and applying the aggregated information stored in agg_edits_plus to the master list by last, first and email.

"""
import mysql.connector
import csv

def open_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )

        print("MySQL Database connection successful")
    except Exception as err:
        print(f"Error: '{err}'")

    return connection


def get_db_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
    except Exception as err:
        #        print(f"Captured {err=}, {type(err)=}")
        print(f"Captured {err=}, {type(err)=}")
    else:
        print("MySQL connection obtained")
        return connection


def show_columns(tablename, cursor):
    sql = "SHOW FIELDS FROM " + tablename
    cursor.execute(sql, params=None)
    print("MySQL Columns in " + tablename)
    for x in cursor:
        print(x)


def show_tables(cursor):
    cursor.execute("SHOW TABLES")
    print("Showing all tables")
    for x in cursor:
        print(x)
    print("\n")


def delete_tables(pattern, cursor):
    if pattern != '' and pattern != 'ALL':
        sql = "SHOW TABLES LIKE '" + pattern + "'"
        print(sql)
        cursor.execute(sql)

        sql = ""
        droprow = 0
        for x in cursor:
            droptable = str(x[0])
            print(droptable)
            if droprow == 0:
                sql = sql + droptable
            else:
                sql = sql + ", " + droptable
            droprow += 1

        if droprow > 0:
            print("DROPPING TABLES LIKE '" + pattern + "'")
            sql = "DROP TABLE IF EXISTS " + sql
            cursor.execute(sql)

    elif pattern == 'ALL':
        sql = "DROP DATABASE mydatabase"
        cursor.execute(sql)
        sql = "CREATE DATABASE mydatabase"
        cursor.execute(sql)

    print("Done\n")


def csvRead(csvin, csvcol, csvvalues, csvdelim=','):
    """csvRead loads an entire CSV file into list of tuples by row.

    Rows are appended to a user specified list
    and each row converted to a tuple.
    """
    with open(csvin, mode='r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=csvdelim)
        next(csvreader)
        for row in csvreader:
            list = []
            for ii in range(csvcol):
                list.append(row[ii])
            mytuple = tuple(list)
            csvvalues.append(mytuple)
        print("Values loading in csvRead")
        print(csvvalues)
# print(csvRead.__doc__)

def csvWrite(csvout, csvlist, csvvalues):
    """csvWrite outputs a CSV file from a list of tuples.

    First a header row is written out followed by each
    row stored as a tuple.
    """
    with open(csvout, mode='w') as newfile:
        csvwriter = csv.writer(newfile, delimiter=',', lineterminator='\n')
        csvwriter.writerow(csvlist)
        csvwriter.writerows(csvvalues)
        print("\nValues outputting in csvWrite to ", csvout)


#        print(csvvalues)
# print(csvWrite.__doc__)

def csvDictRead(csvin, csvvalues):
    with open(csvin, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=',')
        for row in csvreader:
            list = []
            if row["email"] != None and row["last"] != None and row["first"] != None:
                list.append(row["email"])
                list.append(row["last"])
                list.append(row["first"])
                list.append(row["source"])
                #                list.append(int(row['seqno']))
                list.append(row["seqno"])
            mytuple = tuple(list)
            csvvalues.append(mytuple)
        print("Values loading in csvDictRead")


def csvDictWrite(csvreader, tableout, fieldlist):
    with open(tableout, 'r') as newfile:
        csvwriter = csv.DictWriter(newfile, fieldnames=fieldlist, delimiter=',')
        for row in csvreader:
            csvwriter.writerow(row)

def dump_table(mytable, fieldlist, cursor):
    print("\nDumping " + mytable)

    sql = "SELECT * FROM " + mytable
    cursor.execute(sql, params=None)
    results = cursor.fetchall()
    values = []

    tablecol = len(fieldlist)

    for row in results:
        list = []
        for ii in range(tablecol):
            list.append(row[ii])
        tupl = tuple(list)
        values.append(tupl)
    
    csvWrite(mytable + ".csv", fieldlist, values)

def get_rowcount(mytable, cursor):
    sql = "SELECT COUNT(*) FROM " + mytable
    cursor.execute(sql)
    result=cursor.fetchone()
    mycount=result[0]
    print(mytable + " row count:", mycount)
    return mycount

####################
# Main Routine
####################
def load_csv(tablename, fieldlist, path='', sheet='', sheetlist='', vartyps='VARCHAR(500)'):
    db = get_db_connection("localhost", "arclement", "Kontiki2!")
    cursor = db.cursor()
    try:
        cursor.execute("USE mydatabase")
    except Exception as err:
        print(f"Captured {err=}")
        cursor.execute("CREATE DATABASE mydatabase")
        print("MySQL database created")
        cursor.execute("USE mydatabase")
    else:
        print("MySQL database opened")

    delete_tables('temp%', cursor)

    show_tables(cursor)

    tablesrc = tablename
    tablecsv = path + tablename + ".csv"

    print("tablename:", tablename)
    print("tablecsv:", tablecsv)

    values = []
    tablecol = len(fieldlist)
    print("CSV read column count:", tablecol)
    print("CSV read fields:", fieldlist)
    csvRead(tablecsv, tablecol, values)
    tablecol = len(fieldlist)
    

    fielddef = ""
    fieldcom = ""
    placemark = ""

    print('VARTYPS', vartyps)
    vartyplist = []
    if vartyps == 'VARCHAR(500)':
        for v in range(tablecol):
            vartyplist.append(vartyps)
    else:
        vartyplist = [vartyps]

    for f in range(tablecol):
        if f < tablecol - 1:
            fielddef = fielddef + fieldlist[f] + " " + vartyplist[f] + ", "
            fieldcom = fieldcom + fieldlist[f] + ", "
            placemark = placemark + "%s" + ", "
        else:
            fielddef = fielddef + fieldlist[f] + " " + vartyplist[f]
            fieldcom = fieldcom + fieldlist[f]
            placemark = placemark + "%s"

    print("vartyplist:", vartyplist)
    print("fielddef:", fielddef)
    print("fieldcom:", fieldcom)
    print("placemark:", placemark)

    sql = "DROP TABLE IF EXISTS " + tablename
    cursor.execute(sql)
    print("Dropped table " + tablename + " from MySQL database")

    if tablename not in ("all_edits", "all_edits_plus", "csv_ordered", "arlington_sources", "deceased"):
        sql = "CREATE TABLE " + tablename + " (" + fielddef + ", source VARCHAR(500), seqno INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (seqno))"
        fieldlist.append("source")
        fieldlist.append("seqno")
        print("ALL " + tablename + " fieldlist:", fieldlist)
    else:
        sql = "CREATE TABLE " + tablename + "(" + fielddef + ")"
    print("CREATE TABLE SQL:", sql)
    cursor.execute(sql)

    sql = "INSERT INTO " + tablename + "(" + fieldcom + ") VALUES (" + placemark + ")"
    print("INSERT INTO TABLE SQL:", sql)
    cursor.executemany(sql, values)
    db.commit()

    if tablename not in ("all_edits", "all_edits_plus", "csv_ordered", "arlington_sources", "deceased"):
        sql = "UPDATE " + tablename + " SET source = '" + tablename + "'"
        print("UPDATE TABLE SQL:", sql)
        cursor.execute(sql)
        db.commit()

    row_count = get_rowcount(tablename, cursor)
    show_tables(cursor)
    show_columns(tablename, cursor)
    
    ##################################################
    ##################################################
    #This code updates all_edits_plus with a single updated sheet, e.g. ACCF or MM
    ##################################################
    ##################################################
    
    if sheet != '':
        sql = "DELETE FROM " + tablename + " WHERE source = '" + sheet + "'"
        cursor.execute(sql)
     
        sheetcol = len(sheetlist)
        print("SHEETLIST:", sheetlist)
        print("LENGTH OF SHEETCOL:", sheetcol)
        
        sheetcom = ""
        show_columns(sheet, cursor)
        dump_table(sheet, sheetlist, cursor)
 
        for f in range(sheetcol):
            if f < sheetcol - 1:
                sheetcom = sheetcom + sheetlist[f] + ", "
            else:
                sheetcom = sheetcom + sheetlist[f]

        print("sheetcom:", sheetcom)

        sql = "INSERT INTO " + tablename + " (" + sheetcom + ") "
        sql = sql + " SELECT " + sheetcom
        sql = sql + " FROM " + sheet
        cursor.execute(sql)
        
    row_count = get_rowcount(tablename, cursor)
    dump_table(tablename, fieldlist, cursor)

    cursor.close()

    print("Done\n")

def merge_edits(edits, tablename, fieldlist):
    db = open_db_connection("localhost", "arclement", "Kontiki2!", "mydatabase")
    cursor = db.cursor()

    show_tables(cursor)
    show_columns(tablename, cursor)

    tablecsv = edits + ".csv"
    values = []

    print("Updating table " + tablename + " with information from " + tablecsv)

    csvDictRead(tablecsv, values)

    for ii in range(len(values)):
        tupl = values[ii]
        #        print(tupl)
        sql = "UPDATE " + tablename + " SET email =  %s, last =  %s, first =  %s \
            where source = %s and seqno = %s"
        cursor.execute(sql, tupl)
        db.commit()

    cursor.close()

    print("Done\n")


def concatenate_tables(tablelist, tablename):
    """concatenate_tables concatenates extracts of all CSV tables.
    
        The following fields from each CSV table are concatenated with UNION ALL:
        tableorder, source, seqno, last, first, email

        The concatenated .csv output can be loaded into Excel, edited manually, and
        used to update the MySQL version of the individual tables with function 
        merge_edits.
        
        It can also be used to generate a master file deduped on last, first and email.
    """

    db = open_db_connection("localhost", "arclement", "Kontiki2!", "mydatabase")
    cursor = db.cursor()

    show_tables(cursor)

    sql = "DROP TABLE IF EXISTS " + tablelist
    cursor.execute(sql)
    print("Dropping " + tablelist)

    tablenames = []
    csvRead(tablelist + '.csv', 2, tablenames, ',')

    sql = "DROP TABLE IF EXISTS " + tablename
    cursor.execute(sql)
    #
    #   Concatenate all tables in ordered .csv list via UNION ALL
    #
    sql = "CREATE TABLE " + tablename + " as SELECT " + tablenames[0][
        0] + " as tableorder, source, seqno, first, last, email from " + tablenames[0][1]

    for ii in range(1, len(tablenames)):
        sql = sql + " UNION ALL SELECT " + tablenames[ii][0] + ", source, seqno, first, last, email from " + \
              tablenames[ii][1]

    sql = sql + " ORDER BY last, first, email desc;"

    cursor.execute(sql)
    db.commit()
    print("Done\n")


####################            
# ORIGINAL CSV FILE LOAD AND INITIAL EDITS [SINCE REMOVED]
####################
# load_csv('accf',['first', 'last', 'association', 'phone', 'phone2', 'email', 'email2'])
# load_csv('comments_2023',['event', 'date', 'topic', 'first', 'last', 'email', 'procon', 'comment', 'location', 'position', 'phone', 'phone2', 'address'])
# load_csv('comments_2019_22',['event', 'date', 'topic', 'first', 'last', 'email', 'procon', 'comment', 'location', 'position', 'phone', 'phone2'])
# load_csv('arlington_2018_',['first', 'last', 'email', 'phone', 'phone2', 'address', 'city', 'state', 'zip'])
# load_csv('supporters',['volunteer', 'first', 'last', 'email', 'phone', 'phone2', 'address', 'city', 'state', 'zip', 'poll'])
# load_csv('mm',['first', 'last', 'email', 'phone', 'donation', 'date'])
# load_csv('arnac',['first', 'last', 'location', 'email', 'email2', 'phone', 'phone2'])
# load_csv('neighbors_for_nottingham', ['first', 'middle', 'last', 'email', 'phone', 'donation', 'date'])
# load_csv('arlington_press', ['first', 'last', 'email'])
# load_csv('old_supporters', ['volunteer', 'first', 'last', 'email', 'phone', 'phone2', 'address', 'city', 'state', 'zip'])
# load_csv('environmentalists', ['first', 'last', 'email', 'phone', 'phone2'])
# load_csv('fb', ['first', 'last', 'email'])
# load_csv('ca_2022', ['association', 'officer', 'first', 'last', 'title', 'phone', 'email', 'email2', 'email3'])
# load_csv('ecoaction_arlington', ['first', 'last', 'position', 'location', 'phone', 'phone2', 'email', 'email2'])
# load_csv('fairlington', ['association', 'officer', 'first', 'last', 'title', 'phone', 'email', 'email2', 'email3'])
# load_csv('awaiting_confirmation', ['email', 'first', 'last', 'email2'])
# load_csv('acrc', ['office', 'first', 'last', 'email'])
# load_csv('commissions', ['first', 'last', 'email'])
# load_csv('fgp_greens', ['last', 'first', 'email', 'phone', 'cd', 'hod', 'senate', 'vounteer'])
# load_csv('crystal_city', ['first', 'last', 'email', 'phone', 'cell', 'address', 'city', 'state', 'zip'])
# load_csv('ca_2021f', ['association', 'officer', 'first', 'last', 'office', 'phone', 'email', 'website'])
# load_csv('ca_2021', ['association', 'officer', 'first', 'last', 'office', 'phone', 'email', 'website'])
# load_csv('ca_2020', ['association', 'first', 'last', 'office', 'address', 'email', 'phone', 'email2'])
# load_csv('ca_2019', ['first', 'last', 'office', 'association', 'address', 'location', 'phone', 'email', 'website'])
# load_csv('ca_2018', ['association', 'officer', 'first', 'last', 'location', 'phone', 'email'])
# load_csv('ca_2018_pike', ['first', 'last', 'email', 'association'])
# load_csv('ca_2017', ['association', 'first', 'last', 'office', 'address', 'location', 'phone', 'fax', 'email', 'website'])
# load_csv('ca_2016', ['association', 'first', 'last', 'office', 'address', 'location', 'phone', 'email', 'website'])
# load_csv('ca_2015', ['association', 'first', 'last', 'office', 'address', 'location', 'phone', 'email', 'website'])
# load_csv('ca_2014', ['association', 'name', 'first', 'last', 'office', 'address', 'location', 'phone', 'email', 'website'])
# load_csv('accf_2019', ['office', 'first', 'last', 'email'])
# load_csv('ora', ['first', 'last', 'email'])
# load_csv('md_greens', ['first', 'last', 'email', 'phone', 'cell', 'address', 'city', 'state', 'zip'])
# load_csv('w_l_boosters', ['first', 'last', 'email'])
# load_csv('arlington_hotmail', ['first', 'last', 'email', 'phone', 'street', 'city', 'state', 'zip'])
# load_csv('opera_nova', ['first', 'last', 'email'])
# load_csv('arlington_parks', ['first', 'last', 'email'])
# load_csv('save_the_bridge', ['first', 'last', 'email'])
# load_csv('rosslyn_2015', ['first', 'last', 'email'])
# load_csv('boulevard_manor_2015', ['first', 'last', 'email', 'phone', 'cell', 'address', 'city', 'state', 'zip'])
# load_csv('arlington_2017', ['first', 'last', 'email', 'phone', 'address', 'city', 'state', 'zip'])
# load_csv('arlington_2016', ['first', 'last', 'email', 'phone', 'address', 'city', 'state', 'zip'])
# load_csv('arlington_2015', ['first', 'last', 'email', 'phone', 'address', 'city', 'state', 'zip'])
# load_csv('arlington_2014', ['first', 'last', 'email', 'address', 'city', 'state', 'zip'])
# load_csv('bluemont_2017', ['first', 'last', 'email'])
# load_csv('ballston', ['first', 'last', 'email', 'phone'])
# load_csv('attendance_2018', ['event', 'date', 'topic', 'first', 'last', 'email', 'comment', 'position', 'phone', 'phone2'])
# load_csv('ccpta_2018', ['first', 'last', 'email', 'office'])
# load_csv('ccpta_2016', ['first', 'last', 'office', 'email'])
# load_csv('pta_2014', ['school', 'office', 'first', 'last', 'email'])
# load_csv('nova_greens', ['first', 'last', 'email', 'phone'])
# load_csv('sierra_club', ['first', 'last', 'email'])
# load_csv('acst', ['first', 'last', 'email'])
# load_csv('sanders_supporters', ['first', 'last', 'email'])
# load_csv('jsp_va_donors', ['first', 'last', 'email', 'phone', 'cell', 'donation'])

####################            
# CSV File FINAL EDITS
####################
# merge_edits('all_edits', 'accf',                     ['first', 'last', 'association', 'phone', 'phone2', 'email', 'email2'])
# merge_edits('all_edits', 'comments_2023',            ['event', 'date', 'topic', 'first', 'last', 'email', 'procon', 'comment', 'location', 'position', 'phone', 'phone2', 'address'])
# merge_edits('all_edits', 'comments_2019_22',         ['event', 'date', 'topic', 'first', 'last', 'email', 'procon', 'comment', 'location', 'position', 'phone', 'phone2'])
# merge_edits('all_edits', 'arlington_2018_',          ['first', 'last', 'email', 'phone', 'phone2', 'address', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'supporters',               ['volunteer', 'first', 'last', 'email', 'phone', 'phone2', 'address', 'city', 'state', 'zip', 'poll'])
# merge_edits('all_edits', 'mm',                       ['first', 'last', 'email', 'phone', 'donation', 'date'])
# merge_edits('all_edits', 'arnac',                    ['first', 'last', 'location', 'email', 'email2', 'phone', 'phone2'])
# merge_edits('all_edits', 'neighbors_for_nottingham', ['first', 'middle', 'last', 'email', 'phone', 'donation', 'date'])
# merge_edits('all_edits', 'arlington_press',          ['first', 'last', 'email'])
# merge_edits('all_edits', 'old_supporters',           ['volunteer', 'first', 'last', 'email', 'phone', 'phone2', 'address', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'environmentalists',        ['first', 'last', 'email', 'phone', 'phone2'])
# merge_edits('all_edits', 'fb',                       ['first', 'last', 'email'])
# merge_edits('all_edits', 'ca_2022',                  ['association', 'officer', 'first', 'last', 'title', 'phone', 'email', 'email2', 'email3'])
# merge_edits('all_edits', 'ecoaction_arlington',      ['first', 'last', 'position', 'location', 'phone', 'phone2', 'email', 'email2'])
# merge_edits('all_edits', 'fairlington',              ['association', 'officer', 'first', 'last', 'title', 'phone', 'email', 'email2', 'email3'])
# merge_edits('all_edits', 'awaiting_confirmation',    ['email', 'first', 'last', 'email2'])
# merge_edits('all_edits', 'acrc',                     ['office', 'first', 'last', 'email'])
# merge_edits('all_edits', 'commissions',              ['first', 'last', 'email'])
# merge_edits('all_edits', 'fgp_greens',               ['last', 'first', 'email', 'phone', 'cd', 'hod', 'senate', 'vounteer'])
# merge_edits('all_edits', 'crystal_city',             ['first', 'last', 'email', 'phone', 'cell', 'address', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'ca_2021f',                 ['association', 'officer', 'first', 'last', 'office', 'phone', 'email', 'website'])
# merge_edits('all_edits', 'ca_2021',                  ['association', 'officer', 'first', 'last', 'office', 'phone', 'email', 'website'])
# merge_edits('all_edits',  'ca_2020',                 ['association', 'first', 'last', 'office', 'address', 'email', 'phone', 'email2'])
# merge_edits('all_edits', 'ca_2019',                  ['first', 'last', 'office', 'association', 'address', 'location', 'phone', 'email', 'website'])
# merge_edits('all_edits', 'ca_2018',                  ['association', 'officer', 'first', 'last', 'location', 'phone', 'email'])
# merge_edits('all_edits', 'ca_2018_pike',             ['first', 'last', 'email', 'association'])
# merge_edits('all_edits', 'ca_2017',                  ['association', 'first', 'last', 'office', 'address', 'location', 'phone', 'fax', 'email', 'website'])
# merge_edits('all_edits', 'ca_2016',                  ['association', 'first', 'last', 'office', 'address', 'location', 'phone', 'email', 'website'])
# merge_edits('all_edits', 'ca_2015',                  ['association', 'first', 'last', 'office', 'address', 'location', 'phone', 'email', 'website'])
# merge_edits('all_edits', 'ca_2014',                  ['association', 'name', 'first', 'last', 'office', 'address', 'location', 'phone', 'email', 'website'])
# merge_edits('all_edits', 'accf_2019',                ['office', 'first', 'last', 'email'])
# merge_edits('all_edits', 'ora',                      ['first', 'last', 'email'])
# merge_edits('all_edits', 'md_greens',                ['first', 'last', 'email', 'phone', 'cell', 'address', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'w_l_boosters',             ['first', 'last', 'email'])
# merge_edits('all_edits', 'arlington_hotmail',        ['first', 'last', 'email', 'phone', 'street', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'opera_nova',               ['first', 'last', 'email'])
# merge_edits('all_edits', 'arlington_parks',          ['first', 'last', 'email'])
# merge_edits('all_edits', 'save_the_bridge',          ['first', 'last', 'email'])
# merge_edits('all_edits', 'rosslyn_2015',             ['first', 'last', 'email'])
# merge_edits('all_edits', 'boulevard_manor_2015',     ['first', 'last', 'email', 'phone', 'cell', 'address', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'arlington_2017',           ['first', 'last', 'email', 'phone', 'address', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'arlington_2016',           ['first', 'last', 'email', 'phone', 'address', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'arlington_2015',           ['first', 'last', 'email', 'phone', 'address', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'arlington_2014',           ['first', 'last', 'email', 'address', 'city', 'state', 'zip'])
# merge_edits('all_edits', 'bluemont_2017',            ['first', 'last', 'email'])
# merge_edits('all_edits', 'ballston',                 ['first', 'last', 'email', 'phone'])
# merge_edits('all_edits', 'attendance_2018',          ['event', 'date', 'topic', 'first', 'last', 'email', 'comment', 'position', 'phone', 'phone2'])
# merge_edits('all_edits', 'ccpta_2018',               ['first', 'last', 'email', 'office'])
# merge_edits('all_edits', 'ccpta_2016',               ['first', 'last', 'office', 'email'])
# merge_edits('all_edits', 'pta_2014',                 ['school', 'office', 'first', 'last', 'email'])
# merge_edits('all_edits', 'nova_greens',              ['first', 'last', 'email', 'phone'])
# merge_edits('all_edits', 'sierra_club',              ['first', 'last', 'email'])
# merge_edits('all_edits', 'acst',                     ['first', 'last', 'email'])
# merge_edits('all_edits', 'sanders_supporters',       ['first', 'last', 'email'])
# merge_edits('all_edits', 'jsp_va_donors',            ['first', 'last', 'email', 'phone', 'cell', 'donation'])
# merge_edits('all_edits', 'deceased',            ['first', 'last', 'email'])

def augment_table_from_csv(tablename, suffix):
    """augment_table applies additional information from MySQL csv tables
        to another table by source and seqno.
    """

    db = open_db_connection("localhost", "arclement", "Kontiki2!", "mydatabase")
    cursor = db.cursor()

    delete_tables(tablename + suffix + '%', cursor)
    #
    #   Merge updated .csv files with master list and all_edits to pull off additional information by source and seqno
    #
    sql = "CREATE TABLE " + tablename + suffix + " AS " + \
          " SELECT a.*," + \
          " COALESCE(b.email2, h.email2, l.email2, m.email2, n.email2, o.email2, t.email2) AS email2," + \
          " COALESCE(b.phone, c.phone, d.phone, e.phone, f.phone, g.phone, h.phone, i.phone, j.phone, k.phone, l.phone, p.phone," + \
          " q.phone, r.phone, s.phone, t.phone, u.phone, v.phone, w.phone, x.phone, y.phone, z.phone," + \
          " a1.phone, b1.phone, c1.phone, d1.phone, e1.phone, f1.phone, h1.phone, i1.phone, j1.phone, k1.phone)" + \
          " AS phone," + \
          " COALESCE(b.phone2, c.phone2, d.phone2, e.phone2, f.phone2, h.phone2, j.phone, k.phone2, m.phone2, i1.phone2," + \
          " q.cell, a1.cell, c1.cell, k1.cell) AS phone2," + \
          " COALESCE(c.address, e.address, f.address, j.address, q.address, u.address, w.address, y.address, z.address," + \
          " a1.address, c1.address, d1.address, e1.address, f1.address, g1.address) AS address" + \
          " FROM " + tablename + " a" + \
          " LEFT JOIN (SELECT source, seqno, email2, phone, phone2 FROM accf) b" + \
          " ON a.source = b.source AND a.seqno = b.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, phone2, address FROM comments_2023) c" + \
          " ON a.source = c.source AND a.seqno = c.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, phone2 FROM comments_2019_22) d" + \
          " ON a.source = d.source AND a.seqno = d.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, phone2, address FROM arlington_2018_) e" + \
          " ON a.source = e.source AND a.seqno = e.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, phone2, address FROM supporters) f" + \
          " ON a.source = f.source AND a.seqno = f.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone FROM mm) g" + \
          " ON a.source = g.source AND a.seqno = g.seqno" + \
          " LEFT JOIN (SELECT source, seqno, email2, phone, phone2 FROM arnac) h" + \
          " ON a.source = h.source AND a.seqno = h.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone FROM neighbors_for_nottingham) i" + \
          " ON a.source = i.source AND a.seqno = i.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, phone2, address FROM old_supporters) j" + \
          " ON a.source = j.source AND a.seqno = j.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, phone2 FROM environmentalists) k" + \
          " ON a.source = k.source AND a.seqno = k.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, email2 FROM ca_2022) l" + \
          " ON a.source = l.source AND a.seqno = l.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, phone2, email2 FROM ecoaction_arlington) m" + \
          " ON a.source = m.source AND a.seqno = m.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, email2 FROM fairlington) n" + \
          " ON a.source = n.source AND a.seqno = n.seqno" + \
          " LEFT JOIN (SELECT source, seqno, email2 FROM awaiting_confirmation) o" + \
          " ON a.source = o.source AND a.seqno = o.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone FROM fgp_greens) p" + \
          " ON a.source = p.source AND a.seqno = p.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, cell, address FROM crystal_city) q" + \
          " ON a.source = q.source AND a.seqno = q.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone FROM ca_2021f) r" + \
          " ON a.source = r.source AND a.seqno = r.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone FROM ca_2021) s" + \
          " ON a.source = s.source AND a.seqno = s.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, email2 FROM ca_2020) t" + \
          " ON a.source = t.source AND a.seqno = t.seqno" + \
          " LEFT JOIN (SELECT source, seqno, address, phone FROM ca_2019) u" + \
          " ON a.source = u.source AND a.seqno = u.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone FROM ca_2018) v" + \
          " ON a.source = v.source AND a.seqno = v.seqno" + \
          " LEFT JOIN (SELECT source, seqno, address, phone FROM ca_2017) w" + \
          " ON a.source = w.source AND a.seqno = w.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone FROM ca_2016) x" + \
          " ON a.source = x.source AND a.seqno = x.seqno" + \
          " LEFT JOIN (SELECT source, seqno, address, phone FROM ca_2015) y" + \
          " ON a.source = y.source AND a.seqno = y.seqno" + \
          " LEFT JOIN (SELECT source, seqno, address, phone FROM ca_2014) z" + \
          " ON a.source = z.source AND a.seqno = z.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, cell, address FROM md_greens) a1" + \
          " ON a.source = a1.source AND a.seqno = a1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, street FROM arlington_hotmail) b1" + \
          " ON a.source = b1.source AND a.seqno = b1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, cell, address FROM boulevard_manor_2015) c1" + \
          " ON a.source = c1.source AND a.seqno = c1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, address FROM arlington_2017) d1" + \
          " ON a.source = d1.source AND a.seqno = d1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, address FROM arlington_2016) e1" + \
          " ON a.source = e1.source AND a.seqno = e1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, address FROM arlington_2015) f1" + \
          " ON a.source = f1.source AND a.seqno = f1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, address FROM arlington_2014) g1" + \
          " ON a.source = g1.source AND a.seqno = g1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone FROM ballston) h1" + \
          " ON a.source = h1.source AND a.seqno = h1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, phone2 FROM attendance_2018) i1" + \
          " ON a.source = i1.source AND a.seqno = i1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone FROM nova_greens) j1" + \
          " ON a.source = j1.source AND a.seqno = j1.seqno" + \
          " LEFT JOIN (SELECT source, seqno, phone, cell FROM jsp_va_donors) k1" + \
          " ON a.source = k1.source AND a.seqno = k1.seqno" + \
          " ORDER BY last, first, email"

    print("\n" + tablename + suffix + " sql")
    print(sql)
    cursor.execute(sql)
    db.commit()

    cursor.close()

    print("Done\n")


def build_master_from_all_edits_plus(tablelist, tablename):
    """
    """
    db = open_db_connection("localhost", "arclement", "Kontiki2!", "mydatabase")
    cursor = db.cursor()

    show_tables(cursor)

    for ii in range(1, 4):
        sql = "DROP TABLE IF EXISTS temp" + str(ii)
        cursor.execute(sql)

    sql = "CREATE TABLE temp1 as SELECT b.tableorder, a.source, a.seqno, a.first, a.last, a.email from all_edits_plus a" + \
          " LEFT JOIN (SELECT tableorder, tablename FROM csv_ordered) b ON a.source = b.tablename" + \
          " ORDER BY last, first, email desc;"
    cursor.execute(sql)
    db.commit()
    sql = "SELECT * FROM temp1 limit 10"
    cursor.execute(sql, params=None)
    results = cursor.fetchall()
    print("\ntemp1 results\n", results)
    
    #
    #   Dedupe concatented table on lowest tableorder within last, first, email
    #
    sql = "CREATE TABLE temp2 AS SELECT tableorder, source, seqno, a.first, a.last, a.email FROM temp1" + \
          " a INNER JOIN (SELECT last, first, email, min(tableorder) AS toporder FROM temp1" + \
          " GROUP BY last, first, email) b" \
          " ON a.last = b.last AND a.first = b.first AND a.email = b.email AND a.tableorder = b.toporder" + \
          " WHERE a.last != '' OR a.first != '' OR a.email != ''"
    cursor.execute(sql)
    db.commit()

    sql = "SELECT * FROM temp2 limit 10"
    cursor.execute(sql, params=None)
    results = cursor.fetchall()
    print("\ntemp2 results\n", results)
    #
    #   Dedupe concatented table on highest seqno within last, first, email
    #
    sql = "CREATE TABLE temp3 AS SELECT tableorder, source, seqno, a.first, a.last, a.email FROM temp2" + \
          " a INNER JOIN (SELECT last, first, email, max(seqno) as topseqno FROM temp2" + \
          " GROUP BY last, first, email) b" + \
          " ON a.last = b.last AND a.first = b.first AND a.email = b.email AND a.seqno = b.topseqno" + \
          " ORDER BY last, first, email ; "
    cursor.execute(sql)
    db.commit()
    #
    #   Delete records of deceased
    #
    sql = "DELETE FROM temp3 WHERE email IN (SELECT email FROM deceased) OR email = '' ;"
    cursor.execute(sql)
    db.commit()

    sql = "SELECT * FROM temp3 limit 10"
    cursor.execute(sql, params=None)
    results = cursor.fetchall()
    print("\ntemp3 results\n", results)

    sql = "DROP TABLE IF EXISTS " + tablename
    cursor.execute(sql)

    sql = "CREATE TABLE " + tablename + " AS SELECT * FROM temp3 ;"
    cursor.execute(sql)
    db.commit()
    
    sql = "SELECT * FROM " + tablename + " limit 10"
    cursor.execute(sql, params=None)
    results = cursor.fetchall()
    print("\n" + tablename + " results\n", results)
    #
    #   Output list to .csv file
    #

    fieldlist = ['tableorder', 'source', 'seqno', 'first', 'last', 'email']
    dump_table(tablename, fieldlist, cursor)

    """
    fieldcom = ""
    fieldlist = ['tableorder', 'source', 'seqno', 'first', 'last', 'email']
    tablecol = len(fieldlist)
    for f in range(tablecol):
        if f < tablecol - 1:
            fieldcom = fieldcom + fieldlist[f] + ", "
        else:
            fieldcom = fieldcom + fieldlist[f]
    print("fieldcom:", fieldcom)

    sql = "SELECT " + fieldcom + " FROM " + tablename
    cursor.execute(sql, params=None)
    results = cursor.fetchall()
    values = []

    for row in results:
        list = []
        for ii in range(tablecol):
            list.append(row[ii])
        tupl = tuple(list)
        values.append(tupl)

    csvWrite(tablename + ".csv", fieldlist, values)
    cursor.close()
    """
    print("Done\n")


def augment_table_from_all_edits_plus(tablename, suffix):
    """
    augment_table applies additional information from MySQL all_edits_plus
    to master_list by last, first email.
    """
    db = open_db_connection("localhost", "arclement", "Kontiki2!", "mydatabase")
    cursor = db.cursor()

    delete_tables(tablename + suffix + '%', cursor)
    #   
    #   Merge updated .csv files with master list and all_edits to pull off additional information by source and seqno
    #
    
    sql = "SELECT COUNT(*) FROM all_edits_plus"
    print("\n" + sql)
    cursor.execute(sql, params=None)
    results = cursor.fetchall()
    print(results)

    sql = "SELECT * FROM all_edits_plus limit 10"
    print("\n" + sql)
    cursor.execute(sql, params=None)
    results = cursor.fetchall()
    print(results)

    sql = "DROP TABLE IF EXISTS agg_edits_plus"
    cursor.execute(sql)
    #
    #   Get the maximum value in all_edits_plus for each email2, phone, phone2 and address by last, first and email
    #
    sql = "CREATE TABLE agg_edits_plus AS" + \
          " SELECT last, first, email, max(email2) AS email2, max(phone) AS phone, max(phone2) AS phone2, max(address) AS address" + \
          " FROM all_edits_plus" + \
          " WHERE first IS NOT NULL and last IS NOT NULL and email IS NOT NULL" + \
          " GROUP BY last, first, email;"

    print("\nagg_edits_plus sql")
    print(sql)
    cursor.execute(sql)

    fieldlist = ['first', 'last', 'email', 'email2', 'phone', 'phone2', 'address']
    dump_table('agg_edits_plus', fieldlist, cursor)
    #
    #   Merge master list with augmented all_edits_plus by last, first, email to apply information
    #   to master list
    #
    sql = "CREATE TABLE " + tablename + suffix + " AS" + \
          " SELECT a.tableorder, a.source, a.seqno, a.first, a.last, a.email," + \
          " b.email2, b.phone, b.phone2, b.address" + \
          " FROM " + tablename + " a" + \
          " LEFT JOIN (SELECT first, last, email, email2, phone, phone2, address" + \
          " FROM agg_edits_plus) b" + \
          " ON a.last = b.last and a.first = b.first and a.email = b. email" + \
          " ORDER BY last, first, email;"

    print("\n" + tablename + suffix + " sql")
    print(sql)
    cursor.execute(sql)

    fieldlist = ['tableorder', 'source', 'seqno', 'first', 'last', 'email', 'email2', 'phone', 'phone2', 'address']
    dump_table(tablename + suffix , fieldlist, cursor)

    fieldlist = ['tableorder', 'source', 'seqno', 'first', 'last', 'email', 'email2', 'phone', 'phone2', 'address']

    show_tables(cursor)
    
    etablename = "arlington" 
    sql = "DROP TABLE IF EXISTS " + etablename
    cursor.execute(sql)
    sql = "CREATE TABLE " + etablename + " AS" + \
           " SELECT a.* FROM " + tablename + suffix + " a" + \
           " inner join arlington_sources b" + \
           " ON a.source = b.source"

    print("\n" + etablename + " sql")
    print(sql)  
    cursor.execute(sql)
    dump_table(etablename, fieldlist, cursor)
    
    etablename = "delray"
    sql = "DROP TABLE IF EXISTS " + etablename
    cursor.execute(sql)
    sql = "CREATE TABLE " + etablename + " AS" + \
          " SELECT a.* FROM " + tablename + suffix + " a" + \
          " WHERE source = 'delray'"
    print("\n" + etablename + " sql")
    print(sql)   
    cursor.execute(sql)
    dump_table(etablename, fieldlist, cursor)

    etablename = "greens" 
    sql = "DROP TABLE IF EXISTS " + etablename
    cursor.execute(sql)
    sql = "CREATE TABLE " + etablename + " AS" + \
          " SELECT a.* FROM " + tablename + suffix + " a" + \
          " WHERE source = 'fgp_greens' or source = 'jsp_va_donors' or source = 'md_greens' or source = 'nova_greens'"
    print("\n" + etablename + " sql")
    print(sql)   
    cursor.execute(sql)
    dump_table(etablename, fieldlist, cursor)
    
    etablename = "sanders_supporters"
    sql = "DROP TABLE IF EXISTS " + etablename
    cursor.execute(sql)
    sql = "CREATE TABLE " + etablename + " AS" + \
          " SELECT a.* FROM " + tablename + suffix + " a" + \
          " WHERE source = 'sanders_supporters'"
    print("\n" + etablename + " sql")
    print(sql)   
    cursor.execute(sql)
    dump_table(etablename, fieldlist, cursor)
    
    etablename = "sierra_club"
    sql = "DROP TABLE IF EXISTS " + etablename
    cursor.execute(sql)
    sql = "CREATE TABLE " + etablename + " AS" + \
          " SELECT a.* FROM " + tablename + suffix + " a" + \
          " WHERE source = 'sierra_club'"
    print("\n" + etablename + " sql")
    print(sql)   
    cursor.execute(sql)
    dump_table(etablename, fieldlist, cursor)
    
    etablename = "w_l_boosters"
    sql = "DROP TABLE IF EXISTS " + etablename
    cursor.execute(sql)
    sql = "CREATE TABLE " + etablename + " AS" + \
          " SELECT a.* FROM " + tablename + suffix + " a" + \
          " WHERE source = 'w_l_boosters'"
    print("\n" + etablename + " sql")
    print(sql)   
    cursor.execute(sql)
    dump_table(etablename, fieldlist, cursor)
    
    cursor.close()
    print("Done\n")

#concatenate_tables('csv_ordered.txt', 'all_csv')
#load_csv('all_edits_plus', ['source', 'seqno', 'first', 'last', 'email', 'email2', 'phone', 'phone2', 'address']) 
#load_csv('arlington_sources', ['source'])

#load_csv('accf', ['first', 'last', 'email', 'address', 'phone', 'phone2', 'email2'],path='c:\\users\\owner\\dropbox\\arc_candidate\\outreach\\contacts\\source\\')
#load_csv('all_edits_plus', ['source', 'seqno', 'first', 'last', 'email', 'email2', 'phone', 'phone2', 'address'], sheet='accf', sheetlist=['first', 'last', 'email', 'email2', 'phone', 'phone2', 'address', 'source', 'seqno']) 

#load_csv('comments_2024',['event', 'date', 'topic', 'first', 'last', 'email', 'procon', 'comment', 'location', 'position', 'phone', 'phone2', 'address'],path='c:\\users\\owner\\dropbox\\arc_candidate\\outreach\\contacts\\source\\')
#load_csv('all_edits_plus', ['source', 'seqno', 'first', 'last', 'email', 'email2', 'phone', 'phone2', 'address'], sheet='comments_2024', sheetlist=['first', 'last', 'email', 'phone', 'phone2', 'address', 'source', 'seqno']) 

#load_csv('supporters',['volunteer', 'first', 'last', 'email', 'phone', 'phone2', 'address', 'city', 'state', 'zip', 'poll'],path='c:\\users\\owner\\dropbox\\arc_candidate\\outreach\\contacts\\source\\')
#load_csv('all_edits_plus', ['source', 'seqno', 'first', 'last', 'email', 'email2', 'phone', 'phone2', 'address'], sheet='supporters', sheetlist=['first', 'last', 'email', 'phone', 'phone2', 'address', 'source', 'seqno'])

#load_csv('environmentalists',['first', 'last', 'email', 'address', 'phone', 'phone2', 'organization'],path='c:\\users\\owner\\dropbox\\arc_candidate\\outreach\\contacts\\source\\')
#load_csv('all_edits_plus', ['source', 'seqno', 'first', 'last', 'email', 'email2', 'phone', 'phone2', 'address'], sheet='environmentalists', sheetlist=['first', 'last', 'email', 'address', 'phone', 'phone2', 'source', 'seqno'])

load_csv('comments_2025',['event', 'date', 'topic', 'first', 'last', 'email', 'procon', 'comment', 'location', 'position', 'phone', 'phone2', 'address'],path='c:\\users\\owner\\dropbox\\arc_candidate\\outreach\\contacts\\source\\')
load_csv('all_edits_plus', ['source', 'seqno', 'first', 'last', 'email', 'email2', 'phone', 'phone2', 'address'], sheet='comments_2025', sheetlist=['first', 'last', 'email', 'phone', 'phone2', 'address', 'source', 'seqno']) 

load_csv('deceased', ['first', 'last', 'email'])
load_csv('csv_ordered', ['tableorder', 'tablename'])

build_master_from_all_edits_plus('csv_ordered', 'master_list')
augment_table_from_all_edits_plus('master_list', '_plus')
