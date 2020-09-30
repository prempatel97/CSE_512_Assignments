import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    current = openconnection.cursor()
    data_part0 = 0
    current.execute("CREATE TABLE " + ratingstablename + " (row_id INTEGER, userid INTEGER, movieid INTEGER, rating FLOAT)")
    #current.execute('ALTER TABLE'+ ratingstablename+ 'DROP COLUMN timest')
    with open(ratingsfilepath, 'r') as file:
        for line in file:
            fields = line.split('::')
            data_part0 += 1
            data_part1 = fields[0]
            data_part2 = fields[1]
            data_part3 = fields[2]
            current.execute("INSERT INTO "+ ratingstablename + " VALUES {}".format("("+ str(data_part0)+ ',' + data_part1 + ',' + data_part2 + ',' +data_part3 +")"))
    openconnection.commit()
    current.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    current = openconnection.cursor()
    interval = 5.0/numberofpartitions
    x = 0
    inter = 0
    global nop
    nop = numberofpartitions

    while(inter < 5.0):
        current.execute("DROP TABLE IF EXISTS range_part" + str(x))
        if inter == 0:
            current.execute("CREATE TABLE range_part" +str(x)+ " AS SELECT * FROM " +ratingstablename+ " WHERE rating>="+str(inter)+" AND rating<="+str(inter+interval)+";")
        else:
            current.execute("CREATE TABLE range_part" + str(x) + " AS SELECT * FROM " +ratingstablename+ " WHERE rating>" + str(inter) + " AND rating<=" + str(inter + interval) + ";")
        x = x + 1
        inter = inter + interval
    openconnection.commit()
    current.close()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):

    current = openconnection.cursor()
    #RROBIN_TABLE_PREFIX = 'rrobin_part'
    for i in range(numberofpartitions):
        #table_name = 'rrobin_part' + str(i)
        current.execute('CREATE TABLE rrobin_part'+ str(i) +' (userid integer, movieid integer, rating float);')
        current.execute(
            'INSERT INTO rrobin_part' + str(i) +
            ' (userid, movieid, rating) SELECT userid, movieid, rating FROM '
            + '(SELECT userid, movieid, rating, ROW_NUMBER() over() as row_no FROM '
            + ratingstablename + ') as t where mod(t.row_no-1, 5) = '
            + str(i) + ';')
    current.close()
    openconnection.commit()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):

    current = openconnection.cursor()
    #RROBIN_TABLE_PREFIX = 'rrobin_part'
    current.execute('INSERT INTO ' + ratingstablename + ' (userid, movieid, rating) values (' + str(userid) + ',' + str(
        itemid) + ',' + str(rating) + ');')
    current.execute('SELECT COUNT(*) FROM ' + ratingstablename + ';');
    rows = (current.fetchall())[0][0]
    numberofpartitions = count_parts('rrobin_part', openconnection)
    part_no = (rows - 1) % numberofpartitions
    #table_name = RROBIN_TABLE_PREFIX + str(index)
    current.execute('INSERT INTO rrobin_part' + str(part_no) + ' (userid, movieid, rating) VALUES (' + str(userid) + ',' + str(
        itemid) + ',' + str(rating) + ');')
    current.close()
    openconnection.commit()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    current = openconnection.cursor()
    global nop
    inter_2 = 5.0/nop
    mini = 0
    interval_no = 0
    maxi = inter_2
    while mini<5.0:
        if mini == 0:
            if rating>=mini and rating<=maxi:
                break
            interval_no = interval_no + 1
            mini = mini + inter_2
            maxi = maxi + inter_2
        else:
            if rating>mini and rating<=maxi:
                break
            interval_no = interval_no + 1
            mini = mini + inter_2
            maxi = maxi + inter_2
    current.execute("INSERT INTO range_part" + str(interval_no) + " (userid,movieid,rating) VALUES (%s, %s, %s)",(userid, itemid, rating))
    openconnection.commit()
    current.close()

def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()

def count_parts(name, openconnection):
    current = openconnection.cursor()
    current.execute('SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE ' + '\'' + name + '%\';')
    cnt = current.fetchone()[0]
    current.close()
    return cnt
