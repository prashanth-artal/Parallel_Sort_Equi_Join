#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading
import time
import math

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'semi_rating'
SORT_COLUMN_NAME_FIRST_TABLE = 'rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'movieid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'movieid'
DATABASE_NAME='ddsassignment3'
FILE_NAME='ratings.dat'
FILE_NAME_2='movies.dat'
Num_of_rangepartitions=5
##########################################################################################################

sem12=threading.Semaphore(0)
sem23=threading.Semaphore(0)
sem34=threading.Semaphore(0)
sem45=threading.Semaphore(0)
semaphores =[sem12,sem23,sem34,sem45]

def before_test_script_starts_middleware(con,TABLE_NAME):
    # Use it if you want to

    cur = con.cursor()
    cur.execute('''CREATE TABLE '''+str(TABLE_NAME)+
       ''' (USERID INT,
       ran VARCHAR,
       MOVIEID INT,
       ran2 VARCHAR,
       RATING  FLOAT,
       ran3 VARCHAR,
       time_st VARCHAR(20) );''')
    pass
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def InsertToTable( threadName,InputTable,openconnection,num,delay,OutputTable,SortingColumnName):

    cur=openconnection.cursor()
    time.sleep(delay)
    query=""" Select * from range_"""+str(InputTable)+str(num)+""" order by """+str(SortingColumnName)
    cur.execute(query)
    tuples=cur.fetchall()
    insert=""
    for each in tuples:
        insert+="("
        for number in each:
            if(isinstance(number,str)):
               insert+="'"+number+"',"
            else:
               insert+=str(number)+","
        insert =insert[:-1]
        insert+="),"

    insert=insert[:-1]
    if(insert!=''):
     insertion="Insert into "+str(OutputTable)+" values "+insert

     if(threadName=='Thread0'):
        cur.execute(insertion)
        print threadName
        semaphores[0].release()
     elif(threadName=='Thread1'):
        semaphores[0].acquire()
        print threadName
        cur.execute(insertion)
        semaphores[1].release()
     elif(threadName=='Thread2'):
        semaphores[1].acquire()
        print threadName
        cur.execute(insertion)
        semaphores[2].release()
     elif(threadName=='Thread3'):
        semaphores[2].acquire()
        print threadName
        cur.execute(insertion)
        semaphores[3].release()
     elif(threadName=='Thread4'):
        semaphores[3].acquire()
        print threadName
        cur.execute(insertion)

    return
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    #before_test_script_starts_middleware(openconnection,DATABASE_NAME)
    cur= openconnection.cursor()
    query="""Select MIN("""+str(SortingColumnName)+""") from """+str(InputTable)
    cur.execute(query)
    min=cur.fetchone()[0]
    query="""Select MAX("""+str(SortingColumnName)+""") from """+str(InputTable)
    cur.execute(query)
    max=cur.fetchone()[0]
    equipartition(InputTable,5,SortingColumnName,min,max,openconnection)
    # cur = openconnection.cursor()
    query="""SELECT * INTO """+str(OutputTable)+""" FROM """+str(InputTable)+""" WHERE 1 = 0"""
    cur.execute(query)
    thread_list=[]
    delay=[0,2,4,6,8]
    for i in range(5):
      t = threading.Thread(target=InsertToTable, args=('Thread'+str(i),InputTable,openconnection,i,delay[i],OutputTable,SortingColumnName))
      t.start()
      thread_list.append(t)
    for t in thread_list:
      t.join()
    pass #Remove this once you are done with implementation

def join(Thread_Name,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,openconnection,num,delay,OutputTable):
   cur= openconnection.cursor()
   time.sleep(delay)
   joinquery="""Insert INTO """+str(OutputTable)+""" Select * from equi_"""+str(InputTable1)+str(num)+""" INNER JOIN equi_"""+str(InputTable2)+str(num)+""" ON equi_"""+str(InputTable1)+str(num)+"""."""+Table1JoinColumn+""" = equi_"""+str(InputTable2)+str(num)+"""."""+str(Table2JoinColumn)
   cur.execute(joinquery)
   print Thread_Name
   pass

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cur= openconnection.cursor()
    query="""Select count(*) from """+str(InputTable1)
    cur.execute(query)
    No_Of_rows_Inputtable1=cur.fetchone()[0]
    print No_Of_rows_Inputtable1
    query="""Select count(*) from """+str(InputTable2)
    cur.execute(query)
    No_Of_rows_Inputtable2=cur.fetchone()[0]
    print No_Of_rows_Inputtable2
    query="""Select MIN("""+str(Table1JoinColumn)+""") from """+str(InputTable1)
    cur.execute(query)
    min_tab1=cur.fetchone()[0]
    query="""Select MAX("""+str(Table1JoinColumn)+""") from """+str(InputTable1)
    cur.execute(query)
    max_tab1=cur.fetchone()[0]
    query="""Select MIN("""+str(Table2JoinColumn)+""") from """+str(InputTable2)
    cur.execute(query)
    min_tab2=cur.fetchone()[0]
    query="""Select MAX("""+str(Table2JoinColumn)+""") from """+str(InputTable2)
    cur.execute(query)
    max_tab2=cur.fetchone()[0]
    if(No_Of_rows_Inputtable1>No_Of_rows_Inputtable2):
      each_partition=math.ceil((max_tab1-min_tab1)/float(5))
    else:
      each_partition=math.ceil((max_tab2-min_tab2)/float(5))

    equipartitionforjoin(InputTable1,5,each_partition,Table1JoinColumn,min_tab1,max_tab1,openconnection)
    equipartitionforjoin(InputTable2,5,each_partition,Table2JoinColumn,min_tab2,max_tab2,openconnection)
    query="""SELECT column_name,data_type from INFORMATION_SCHEMA.COLUMNS where table_name='"""+InputTable1+"""'"""
    cur.execute(query)
    value1=cur.fetchall()
    query="""SELECT column_name,data_type from INFORMATION_SCHEMA.COLUMNS where table_name='"""+InputTable2+"""'"""
    cur.execute(query)
    value2=cur.fetchall()
    print value2
    query="""SELECT count(*) from INFORMATION_SCHEMA.COLUMNS where table_name='"""+InputTable2+"""'"""
    cur.execute(query)
    value3=cur.fetchone()[0]
    query="""SELECT * INTO """+str(OutputTable)+""" FROM """+str(InputTable1)+""" WHERE 1 = 0"""
    cur.execute(query)
    prefix1="Table2"
    for i in range(value3):
        if(value2[i][0]== value1[i][0]):
            query="""ALTER TABLE """+str(OutputTable)+""" ADD """+str(prefix1)+"_"+str(value2[i][0])+" "+str(value2[i][1])
            cur.execute(query)
        else:
            query="""ALTER TABLE """+str(OutputTable)+""" ADD """+str(value2[i][0])+" "+str(value2[i][1])
            cur.execute(query)
    delay=[0,2,4,6,8]
    thread_list=[]
    for i in range(5):
      t = threading.Thread(target=join, args=('Thread'+str(i),InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,openconnection,i,delay[i],OutputTable,))
      t.start()
      thread_list.append(t)
    for t in thread_list:
      t.join()
    pass # Remove this once you are done with implementation



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
        print "Created Database successfully"
    else:
        print 'A database named {0} already exists'.format(dbname)

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
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

def loadratings(ratingstablename, ratingsfilepath, openconnection):

    print "open the file"
    cur = openconnection.cursor()
    file_name=open(ratingsfilepath,'r')
    cur.copy_from(file_name,ratingstablename,sep=':')
    file_name.close()
    query="""ALTER TABLE """+str(ratingstablename)+""" DROP COLUMN ran, DROP COLUMN ran2,DROP COLUMN ran3, DROP COLUMN time_st """
    cur.execute(query)
    cur.close()
    print "  Insert Completed "
    pass
def equipartition(ratingstablename, numberofpartitions,SortingColumnName,min,max, openconnection):

    print " Equi Partition Open"
    cur=openconnection.cursor()
    if(numberofpartitions>0 and isinstance(numberofpartitions, int)):
     for i in range(numberofpartitions):
         query="""SELECT * INTO range_"""+str(ratingstablename)+ str(i) +""" FROM """+ str(ratingstablename)+""" WHERE 1 = 0"""
         cur.execute(query)

     print max
     print min
     each_partition=math.ceil((max-min)/float(numberofpartitions))
     print each_partition
     low=min
     part=each_partition
     cnt=0
     while cnt<numberofpartitions:
         print low
         up= part+low
         print up
         if(cnt==0):
            query ="""INSERT INTO range_"""+str(ratingstablename)+str(cnt)+""" SELECT * FROM """+ str(ratingstablename)+""" WHERE """+str(SortingColumnName)+""" >= """+str(low) +"""and """+str(SortingColumnName)+""" <= """+str(up)
         else:
            query ="""INSERT INTO range_"""+str(ratingstablename)+str(cnt)+""" SELECT * FROM """+  str(ratingstablename)+""" WHERE """+str(SortingColumnName)+""" > """+str(low) +"""and """+str(SortingColumnName)+""" <= """+str(up)

         cur.execute(query)
         low=up
         each_partition=each_partition + part
         cnt = cnt + 1
    else:
        print "Only Positive Partitions is considered"
    print "Equi Partition End"
    cur.close()
    pass
def equipartitionforjoin(ratingstablename, numberofpartitions,each_partition,SortingColumnName,min,max, openconnection):

    print " Equi Partition For Join Open"
    cur=openconnection.cursor()
    if(numberofpartitions>0 and isinstance(numberofpartitions, int)):
     for i in range(numberofpartitions):
         query="""SELECT * INTO equi_"""+str(ratingstablename)+ str(i) +""" FROM """+ str(ratingstablename)+""" WHERE 1 = 0"""
         cur.execute(query)
     print each_partition
     low=min
     part=each_partition
     cnt=0
     while cnt<numberofpartitions:
         print low
         up= part+low
         print up
         if(cnt==0):
            query ="""INSERT INTO equi_"""+str(ratingstablename)+str(cnt)+""" SELECT * FROM """+ str(ratingstablename)+""" WHERE """+str(SortingColumnName)+""" >= """+str(low) +"""and """+str(SortingColumnName)+""" <= """+str(up)
         else:
            query ="""INSERT INTO equi_"""+str(ratingstablename)+str(cnt)+""" SELECT * FROM """+  str(ratingstablename)+""" WHERE """+str(SortingColumnName)+""" > """+str(low) +"""and """+str(SortingColumnName)+""" <= """+str(up)

         cur.execute(query)
         low=up
         each_partition=each_partition + part
         cnt = cnt + 1
    else:
        print "Only Positive Partitions is considered"
    print "Equi Partition For Join End"
    cur.close()
    pass
if __name__ == '__main__':
 try: # Creating Database ddsassignment2
	#print "Creating Database named as ddsassignment2
     createDB(DATABASE_NAME)
     with getOpenConnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
         before_test_script_starts_middleware(con,FIRST_TABLE_NAME)
         loadratings(FIRST_TABLE_NAME,FILE_NAME, con)
         # before_test_script_starts_middleware(con,SECOND_TABLE_NAME)
         # loadratings(SECOND_TABLE_NAME,FILE_NAME_2, con)
         print "Performing Parallel Sort"
         ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);
         # print "Parallel sort ends"
         # print "Performing Parallel Join"
         # ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
         # print "Parallel join ends"

         #Saving parallelSortOutputTable and parallelJoinOutputTable on two files
         saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
         # saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

         # Deleting parallelSortOutputTable and parallelJoinOutputTable
         deleteTables('parallelSortOutputTable', con);
         # deleteTables('parallelJoinOutputTable', con);
     if con:
        con.close()

 except Exception as detail:
        print "OOPS! This is the error ==> ", detail

