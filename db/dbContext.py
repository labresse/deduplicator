import logging
import apsw
from db.database import database
import db.fileextentitemdao as fileextentitem
import db.filedao as file
import db.extenthashdao as extenthash
from models.extenthash import HASH_SIZES
from db.database import database

class dbContext:
    dbPath: str
    db: database
    
    def __init__(self, dbPath):
        self.dbPath = dbPath
        self.db = database(dbPath)
        
        with self.db.initmain() as connection:
            fileextentitem.init_table(connection)
            file.init_table(connection)
            extenthash.init_table(connection)
        with self.db.inittemp() as connection:
            fileextentitem.init_table(connection)
            file.init_table(connection)
            extenthash.init_table(connection)

    def insertNewFiles(self, connection):
        try:      
            sql = '''
    INSERT INTO File(locationobjectid)
    SELECT DISTINCT
        fei.keyobjectid
    FROM FileExtentItem fei
    LEFT JOIN File f ON
        f.locationobjectid = fei.keyobjectid
    LEFT JOIN store.File f2 ON
        f2.locationobjectid = fei.keyobjectid
        AND f2.ishashed = 1 
    WHERE
        f.locationobjectid is null
        AND f2.locationobjectid is null;
                        '''
            connection.execute(sql)  
        except Exception as err:
            logging.exception(err)

    def clearexistingfiles(self, connection):
        try:      
            sql = ''' 
                SELECT
                    fei.*
                from FileExtentItem fei
                INNER JOIN store.File f ON
                    f.locationobjectid = fei.keyobjectid
                LEFT JOIN store.FileExtentItem fei2 on
                    fei2.keyobjectid = fei.keyobjectid
                WHERE
                    fei2.keyobjectid IS NULL
                '''
            res = connection.execute(sql).fetchone()
            if res == None:
                return
            
            sql = '''
                INSERT INTO store.FileExtentItem(keyobjectid, keyoffset)
                SELECT
                    max(fei.keyobjectid), keyoffset
                from FileExtentItem fei
                INNER JOIN store.File f ON
                    f.locationobjectid = fei.keyobjectid;
                '''
            connection.execute(sql)
        except Exception as err:
            logging.exception(err)

        try:    
            sql = '''
                DELETE FROM FileExtentItem
                WHERE keyobjectid IN (
                    SELECT
                        f.locationobjectid
                    FROM store.File f
                )
                        '''
            connection.execute(sql)
        except Exception as err:
            logging.exception(err)

    def deleteFile(self, connection, inode):
        file.deleteByInode(connection, inode)
        for size in HASH_SIZES:
            extenthash.deleteByInode(connection, inode, size)

    def requeueFile(self, connection, locationobjectid):
        try:      
            sql = ''' 
                INSERT INTO File(locationobjectid)
                VALUES (?)
                '''
            connection.execute(sql, (locationobjectid,))
        except Exception as err:
            logging.exception(err)       
            
    def sync(self, connection: apsw.Connection):
        print("sync started")
               
        try:
            self.clearexistingfiles(connection)

            sql = ''' 
                SELECT
                    fei.*
                from FileExtentItem fei
                INNER JOIN File f ON
                    f.locationobjectid = fei.keyobjectid
                WHERE
                    f.ishashed = 1;
                '''
            res = connection.execute(sql).fetchone()
            
            if res != None:
                sql = '''
                DELETE FROM store.FileExtentItem
                WHERE keyobjectid != (SELECT max(keyobjectid) FROM store.FileExtentItem);

                INSERT INTO store.FileExtentItem(keyobjectid, keyoffset)
                SELECT
                    max(fei.keyobjectid), keyoffset
                from FileExtentItem fei
                INNER JOIN File f ON
                    f.locationobjectid = fei.keyobjectid
                WHERE
                    f.ishashed = 1;

                DELETE FROM FileExtentItem
                WHERE keyobjectid IN (
                    SELECT
                        f.locationobjectid
                    FROM File f
                    WHERE
                        f.ishashed = 1
                )
                        '''
                connection.execute(sql)

            for size in HASH_SIZES:
                sql = '''
            REPLACE INTO store.ExtentHash{}(locationobjectid, `offset`, numbytes, hash)
            SELECT eh.locationobjectid, eh.offset, eh.numbytes, eh.hash
            FROM ExtentHash{} eh
            INNER JOIN File f ON
                f.locationobjectid = eh.locationobjectid
            WHERE
                f.ishashed = 1;
                
            DELETE FROM ExtentHash{}
            WHERE locationobjectid IN (
                SELECT
                    f.locationobjectid
                FROM File f
                WHERE
                    f.ishashed = 1
            )
                '''
                connection.execute(sql.format(size, size, size))
                
            sql = '''
            REPLACE INTO store.File(locationobjectid, modifiedtime, ishashed)
            SELECT locationobjectid, modifiedtime, ishashed
            FROM File
            WHERE
                ishashed = 1;
                
            DELETE FROM File
            WHERE locationobjectid IN (
                SELECT
                    f.locationobjectid
                FROM File f
                WHERE
                    f.ishashed = 1)
                    '''
            connection.execute(sql)
            
            print("sync finished")
        except Exception as e:
            logging.exception(e)