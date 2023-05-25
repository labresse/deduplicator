import logging
from multiprocessing import connection
import time
import apsw
from itertools import groupby
from models.file import file

FILE = """
    CREATE TABLE IF NOT EXISTS File (
        locationobjectid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
        modifiedtime INT UNSIGNED NULL,
        ishashed BOOLEAN DEFAULT false NOT NULL
    ) WITHOUT ROWID;
"""

def init_table(connection):
    try:
        with connection:
            connection.execute(FILE)
    except Exception as e:
        logging.error(e)
        
def getUnresolvedFilePaths(connection):
    sql = ''' 
            SELECT	
                *
            FROM File f
            WHERE
                f.modifiedtime IS NULL
            LIMIT 1024
            '''
    try:
        files = list(map(lambda f: file(f), connection.execute(sql)))
        return files
    except Exception as err:
        logging.exception(err)
        return []

def getUnhashedFile(connection):
    sql = ''' 
    SELECT	
        *
    FROM File f
    WHERE
        f.ishashed = 0
    LIMIT 12288
            '''
    try:
        files = list(map(lambda f: (file(f)), connection.execute(sql)))
        return files
    except Exception as err:
        logging.exception(err)
        
def updateFileInfos(connection, files):
    sql = ''' 
            UPDATE File
            SET
                modifiedtime = ?,
                ishashed = ?
            WHERE
                locationobjectid = ?
            '''
    try:
        c = connection.cursor()
        c.execute("BEGIN TRANSACTION;")
        args = [(#file.path,
                file.modifiedtime,
                file.ishashed,
                file.locationobjectid) for file in files]
        connection.executemany(sql, args)
        c.execute("END")
    except Exception as err:
        if connection.in_transaction:
            c.execute("ROLLBACK")
        logging.exception(err) 

def deleteByInode(connection, inode):
    tries = 10
    for i in range(tries):
        try:
            sql = ''' 
                DELETE FROM File
                WHERE
                    locationobjectid = ?
                '''
            c = connection.cursor()
            c.execute("BEGIN TRANSACTION;")
            c.execute(sql, (inode,))
            c.execute("END")
        except Exception as e:
            if connection.in_transaction:
                c.execute("ROLLBACK")
            if i < tries - 1: # i is zero indexed
                logging.exception('Retry attemp {} for setishashed'.format(i)) 
                time.sleep(3)
                continue
            else:
                logging.exception(e) 
                raise
        else:
            break
        
def setishashed(connection, locationobjectid):
    tries = 10
    for i in range(tries):
        try:
            sql = ''' 
                    UPDATE File
                    SET
                        ishashed = 1
                    WHERE
                        locationobjectid = ?
                    '''
            c = connection.cursor()
            c.execute("BEGIN TRANSACTION;")
            c.execute(sql, (locationobjectid,))
            c.execute("END")
        except Exception as e:
            if connection.in_transaction:
                c.execute("ROLLBACK")
            if i < tries - 1: # i is zero indexed
                logging.exception('Retry attemp {} for setishashed'.format(i)) 
                time.sleep(3)
                continue
            else:
                logging.exception(e) 
                raise
        else:
            break

def getById(connection, locationobjectid) -> file:
    sql = ''' 
                SELECT
                    *
                FROM File
                WHERE
                    locationobjectid = ?
                '''
    try:
        for row in connection.execute(sql, (locationobjectid,)):
            return file(row)
        return None
    except Exception as err:
        logging.exception(err)
        
def getNotPunchedFiles(connection):
    sql = ''' 
            SELECT	
                *
            FROM File f
            WHERE
                f.modifiedtime IS NOT NULL
            LIMIT 1024
            '''
    try:
        files = list(map(lambda f: file(f), connection.execute(sql)))
        return files
    except Exception as err:
        logging.exception(err)
        return []
        
