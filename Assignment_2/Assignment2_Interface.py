
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    cursor = openconnection.cursor()
    listOfTuples = []
    listOfTuples_round = []
    cursor.execute('SELECT partitionnum FROM RangeRatingsMetaData WHERE NOT (minrating>='+str(ratingMaxValue)+
                   ' OR maxrating<='+str(ratingMinValue)+');')

    listOfPartition = cursor.fetchall()
    print(listOfPartition)
    # print((l) for l in listOfPartition)
    for j in listOfPartition:
        i = j[0]
        cursor.execute('SELECT \'rangeratingspart' + str(i) +'\',userid,movieid,rating FROM rangeratingspart' + str(i)
                            + ' WHERE rating>=' + str(ratingMinValue) + ' AND rating<=' +str(ratingMaxValue) + ';')
        listOfTuples = cursor.fetchall()
        print(listOfTuples)
        for tup in listOfTuples:
            with open(outputPath, 'a') as fil:
                fil.write(str(tup[0]) + ',' + str(tup[1]) + ',' + str(tup[2]) + ',' + str(tup[3]) + '\n')
        fil.close()

    cursor.execute('SELECT PartitionNum FROM RoundRobinRatingsMetadata')
    roundcnt = int(cursor.fetchone()[0])

    for i in range(roundcnt):
        cursor.execute('SELECT \'roundrobinratingspart' + str(i) +'\',userid,movieid,rating FROM roundrobinratingspart'
                       +str(i)+ ' WHERE rating>=' + str(ratingMinValue) + ' AND rating<=' +str(ratingMaxValue))
        listOfTuples_round = cursor.fetchall()
        print(listOfTuples_round)
        for tup in listOfTuples_round:
            with open(outputPath, 'a') as fil:
                fil.write(str(tup[0]) + ',' + str(tup[1]) + ',' + str(tup[2]) + ',' + str(tup[3]) + '\n')
        fil.close()



def PointQuery(ratingValue, openconnection, outputPath):
    cursor = openconnection.cursor()
    listOfTuples = []
    listOfTuples_round = []
    cursor.execute('SELECT partitionnum FROM RangeRatingsMetaData WHERE ' + str(ratingValue) +'BETWEEN minrating AND maxrating;')

    listOfPartition = cursor.fetchall()
    print(listOfPartition)
    # print((l) for l in listOfPartition)
    for j in listOfPartition:
        i = j[0]
        cursor.execute('SELECT \'rangeratingspart' + str(i) + '\',userid,movieid,rating FROM rangeratingspart' + str(i)
                       + ' WHERE rating =' + str(ratingValue) + ';')
        listOfTuples = cursor.fetchall()
        print(listOfTuples)
        for tup in listOfTuples:
            with open(outputPath, 'a') as fil:
                fil.write(str(tup[0]) + ',' + str(tup[1]) + ',' + str(tup[2]) + ',' + str(tup[3]) + '\n')
        fil.close()

    cursor.execute('SELECT PartitionNum FROM RoundRobinRatingsMetadata')
    roundcnt = int(cursor.fetchone()[0])

    for i in range(roundcnt):
        cursor.execute('SELECT \'roundrobinratingspart' + str(i) + '\',userid,movieid,rating FROM roundrobinratingspart'
                       + str(i) + ' WHERE rating=' + str(ratingValue))
        listOfTuples_round = cursor.fetchall()
        print(listOfTuples_round)
        for tup in listOfTuples_round:
            with open(outputPath, 'a') as fil:
                fil.write(str(tup[0]) + ',' + str(tup[1]) + ',' + str(tup[2]) + ',' + str(tup[3]) + '\n')
        fil.close()
