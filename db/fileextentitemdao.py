import logging
import time
import apsw
from btrfs.ctree import FileExtentItem
from typing import List

# FILE_EXTENT_ITEM = """
#     CREATE TABLE IF NOT EXISTS FileExtentItem (
#         keyobjectid BIGINT UNSIGNED NOT NULL,
#         keytype TINYINT NOT NULL,
#         keyoffset BIGINT UNSIGNED NOT NULL,
#         logical_offset BIGINT UNSIGNED NOT NULL,
#         logical_bytes BIGINT UNSIGNED NOT NULL,
#         generation BIGINT UNSIGNED NOT NULL,
#         ram_bytes BIGINT UNSIGNED NOT NULL,
#         compression TINYINT NOT NULL,
#         type TINYINT NOT NULL,
#         disk_bytenr BIGINT UNSIGNED NULL,
#         disk_num_bytes BIGINT UNSIGNED NULL,
#         `offset` BIGINT UNSIGNED NULL,
#         num_bytes BIGINT UNSIGNED NULL,
#         inline_encoded_nbytes BIGINT UNSIGNED NULL,
#         PRIMARY KEY(keyobjectid, keytype, keyoffset)
#     );
# """
FILE_EXTENT_ITEM = """
    CREATE TABLE IF NOT EXISTS FileExtentItem (
        keyobjectid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
        keyoffset BIGINT UNSIGNED NOT NULL
    ) WITHOUT ROWID;
"""
    
def init_table(connection):
    try:
        with connection:
            connection.execute(FILE_EXTENT_ITEM)
    except Exception as e:
        logging.error(e)

def insertmany(connection, files: List[FileExtentItem]):
    tries = 10
    for i in range(tries):
        try:
            # sql = ''' REPLACE INTO FileExtentItem(keyobjectid,keytype,keyoffset,
            #                                                 logical_offset,logical_bytes,generation,ram_bytes,compression,
            #                                                 type,disk_bytenr,disk_num_bytes,`offset`,
            #                                                 num_bytes,inline_encoded_nbytes)
            #                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
            sql = ''' REPLACE INTO FileExtentItem(keyobjectid,keyoffset)
                    VALUES(?,?) '''
            c = connection.cursor()
            c.execute("BEGIN TRANSACTION;")
            args = [(file.key.objectid,
                # file.key.type,
                file.key.offset,
                # file.logical_offset,
                # file.logical_bytes,
                # file.generation,
                # file.ram_bytes,
                # file.compression,
                # file.type,
                # file.disk_bytenr,
                # file.disk_num_bytes,
                # file.offset,
                # file.num_bytes,
                # file._inline_encoded_nbytes
                ) for file in files]
            c.executemany(sql, args)
            c.execute("END")
        except Exception as err:
            if connection.in_transaction:
                c.execute("ROLLBACK")
            if i < tries - 1: # i is zero indexed
                logging.exception('Retry attemp {} for dirindexdao.insertMany'.format(i)) 
                time.sleep(3)
                continue
            else:
                logging.exception(err) 
                raise
        else:
            break

def getMaxId(connection):
    try:
        sql = ''' 
    SELECT
        max(f.keyobjectid)
    from FileExtentItem f
    '''
        for row in connection.execute(sql):
            if row[0] != None:
                return row[0] 
        return 0
    except Exception as err:
        logging.exception(err)

def deleteByInode(connection, inode):
    tries = 10
    for i in range(tries):
        try:
            sql = ''' 
                    DELETE FROM FileExtentItem
                    WHERE
                        keyobjectid = ?
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
        
def truncate(connection):
    tries = 10
    for i in range(tries):
        try:
            sql = '''
                DELETE FROM FileExtentItem
            '''
            c = connection.cursor()
            c.execute(sql)
        except Exception as e:
            if i < tries - 1: # i is zero indexed
                logging.exception('Retry attemp {} for setishashed'.format(i)) 
                time.sleep(3)
                continue
            else:
                logging.exception(e) 
                raise
        else:
            break