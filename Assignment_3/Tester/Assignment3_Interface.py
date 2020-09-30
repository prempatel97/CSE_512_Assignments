#
# Assignment3 Interface
#
import threading
import psycopg2
import os
import sys

def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    try:
            cur = openconnection.cursor()
            inter_sort, range_Min = Range(InputTable, SortingColumnName, openconnection)


            cur.execute(
                "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable + "';")
            schema = cur.fetchall()

            for i in range(0,5):

                table_Name = "range_part" + str(i)
                cur.execute("DROP TABLE IF EXISTS " + table_Name + "")
                cur.execute("CREATE TABLE " + table_Name + " (" + schema[0][0] + " " + schema[0][1] + ")")

                for j in range(1, len(schema)):
                    cur.execute("ALTER TABLE " + table_Name + " ADD COLUMN " + schema[j][0] + " " + schema[j][1] + ";")

            thread = [0, 0, 0, 0, 0]
            for i in range(0,5):

                if i == 0:
                    low_Value = range_Min
                    upper_Value = range_Min + inter_sort
                else:
                    low_Value = upper_Value
                    upper_Value = upper_Value + inter_sort

                thread[i] = threading.Thread(target=range_insert_sort, args=(
                InputTable, SortingColumnName, i, low_Value, upper_Value, openconnection))

                thread[i].start()

            for j in range(0, 5):
                thread[j].join()

            cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
            cur.execute("CREATE TABLE " + OutputTable + " (" + schema[0][0] + " " + schema[0][1] + ")")

            for i in range(1, len(schema)):
                cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema[i][0] + " " + schema[i][1] + ";")

            for i in range(0, 5):
                query = "INSERT INTO " + OutputTable + " SELECT * FROM " + "range_part" + str(i) + ""
                cur.execute(query)

    except Exception as message:
            print("Exception :", message)

    openconnection.commit()

def Range(InputTable, SortingColumnName, openconnection):
        current = openconnection.cursor()

        current.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + "")
        min_Val = current.fetchone()
        range_min_val = (float)(min_Val[0])

        current.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + "")
        max_Val = current.fetchone()
        range_max_val = (float)(max_Val[0])

        interval = (range_max_val - range_min_val) / 5
        current.close()
        return interval, range_min_val

def range_insert_sort(InputTable, SortingColumnName, index, min_val, max_val, openconnection):

        current = openconnection.cursor()
        table_name = "range_part" + str(index)
        if index == 0:
            query = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">=" + str(
                min_val) + " AND " + SortingColumnName + " <= " + str(
                max_val) + " ORDER BY " + SortingColumnName + " ASC"
        else:
            query = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">" + str(
                min_val) + " AND " + SortingColumnName + " <= " + str(
                max_val) + " ORDER BY " + SortingColumnName + " ASC"

        current.execute(query)
        current.close()
        return

def Min_max(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection):
    current = openconnection.cursor()

    current.execute("SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    minimum1 = current.fetchone()
    Min1 = (float)(minimum1[0])

    current.execute("SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    minimum2 = current.fetchone()
    Min2 = (float)(minimum2[0])

    current.execute("SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    maximum1 = current.fetchone()
    Max1 = (float)(maximum1[0])

    current.execute("SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    maximum2 = current.fetchone()
    Max2 = (float)(maximum2[0])

    if Max1 > Max2:
        rangeMax = Max1
    else:
        rangeMax = Max2

    if Min1 > Min2:
        rangeMin = Min2
    else:
        rangeMin = Min1

    inter = (rangeMax - rangeMin) / 5
    current.close()
    return inter, rangeMin


def OutputRangeTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, schema1, schema2, interval,
                     range_min_value, openconnection):
    cur = openconnection.cursor();
    for i in range(5):

        range_table1_name = "table1_range" + str(i)
        range_table2_name = "table2_range" + str(i)

        if i == 0:
            lower_Value = range_min_value
            upper_Value = range_min_value + interval
        else:
            lower_Value = upper_Value
            upper_Value = upper_Value + interval

        cur.execute("DROP TABLE IF EXISTS " + range_table1_name + ";")
        cur.execute("DROP TABLE IF EXISTS " + range_table2_name + ";")

        if i == 0:
            cur.execute(
                "CREATE TABLE " + range_table1_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " >= " + str(
                    lower_Value) + ") AND (" + Table1JoinColumn + " <= " + str(upper_Value) + ");")
            cur.execute(
                "CREATE TABLE " + range_table2_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " >= " + str(
                    lower_Value) + ") AND (" + Table2JoinColumn + " <= " + str(upper_Value) + ");")

        else:
            cur.execute(
                "CREATE TABLE " + range_table1_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " > " + str(
                    lower_Value) + ") AND (" + Table1JoinColumn + " <= " + str(upper_Value) + ");")
            cur.execute(
                "CREATE TABLE " + range_table2_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " > " + str(
                    lower_Value) + ") AND (" + Table2JoinColumn + " <= " + str(upper_Value) + ");")

        output_range_table = "output_table" + str(i)

        cur.execute("DROP TABLE IF EXISTS " + output_range_table + "")
        cur.execute("CREATE TABLE " + output_range_table + " (" + schema1[0][0] + " " + schema2[0][1] + ")")

        for j in range(1, len(schema1)):
            cur.execute(
                "ALTER TABLE " + output_range_table + " ADD COLUMN " + schema1[j][0] + " " + schema1[j][1] + ";")

        for j in range(len(schema2)):
            cur.execute(
                "ALTER TABLE " + output_range_table + " ADD COLUMN " + schema2[j][0] + "1" + " " + schema2[j][1] + ";")


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    try:
        cur = openconnection.cursor()

        interval, range_min_Value = Min_max(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection)

        cur.execute(
            "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable1 + "'")
        schema1 = cur.fetchall()

        cur.execute(
            "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable2 + "'")
        schema2 = cur.fetchall()

        # output table
        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        cur.execute("CREATE TABLE " + OutputTable + " (" + schema1[0][0] + " " + schema2[0][1] + ")")

        for i in range(1, len(schema1)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema1[i][0] + " " + schema1[i][1] + ";")

        for i in range(len(schema2)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema2[i][0] + "1" + " " + schema2[i][1] + ";")


        OutputRangeTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, schema1, schema2, interval,
                         range_min_Value, openconnection)

        thread = [0, 0, 0, 0, 0]

        for i in range(5):
            thread[i] = threading.Thread(target=range_insert_join,
                                         args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))

            thread[i].start()

        for j in range(0, 5):
            thread[j].join()

        # Inserts in output table
        for i in range(5):
            cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM output_table" + str(i))

    except Exception as message:
        print
        "Exception in ParallelJoin is ==>>", message

    openconnection.commit()

def range_insert_join(Table1JoinColumn, Table2JoinColumn, openconnection, TempTableId):
    current = openconnection.cursor()

    query = "INSERT INTO output_table" + str(TempTableId) + " SELECT * FROM table1_range" + str(
        TempTableId) + " INNER JOIN table2_range" + str(TempTableId) + " ON table1_range" + str(
        TempTableId) + "." + Table1JoinColumn + "=" + "table2_range" + str(TempTableId) + "." + Table2JoinColumn + ";"

    current.execute(query)
    return


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


