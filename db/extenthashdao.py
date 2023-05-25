import logging
import time
import apsw
from itertools import groupby
from models.extenthash import extenthash, extenthashes, HASH_SIZES
from models.duplicationinfo import duplicationinfo
from typing import List

EXTENT_HASH = """
    CREATE TABLE IF NOT EXISTS ExtentHash{} (
        locationobjectid BIGINT UNSIGNED NOT NULL,
        `offset` BIGINT UNSIGNED NOT NULL,
        numbytes MEDIUMINT UNSIGNED NOT NULL,
        hash VARCHAR(32) NOT NULL,
        PRIMARY KEY(locationobjectid, `offset`)
    ) WITHOUT ROWID
"""

def init_table(connection):
    try:
        with connection:
            for size in HASH_SIZES:
                connection.execute(EXTENT_HASH.format(size))
    except Exception as e:
        logging.error(e)
            
def bulkinsert(connection, extenthashes: extenthashes):
    for size in HASH_SIZES:
        hashes = getattr(extenthashes, "extenthashes{}".format(size))
        if len(hashes) == 0:
            continue
        
        for batch in [hashes[i:i + 1024] for i in range(0, len(hashes), 1024)]:
            tries = 10
            for i in range(tries):
                try:
                    sql = '''
        REPLACE INTO ExtentHash{}(locationobjectid, `offset`, numbytes, hash)
        VALUES (?,?,?,?)
                '''.format(size)
                    c = connection.cursor()
                    c.execute("BEGIN TRANSACTION;")
                    args = [(hash.locationobjectid,hash.offset,hash.numbytes,hash.hash) for hash in batch]
                    connection.executemany(sql, args)
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
                        
def deleteByInode(connection, inode, size):
    tries = 10
    for i in range(tries):
        try:
            sql = ''' 
                    DELETE FROM ExtentHash{}
                    WHERE
                        locationobjectid = ?
                    '''.format(size)
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

def delete(connection, eh: extenthash, size):
    tries = 10
    for i in range(tries):
        try:
            sql = ''' 
                    DELETE FROM ExtentHash{}
                    WHERE
                        locationobjectid = ?
                        AND `offset` = ?
                    '''.format(size)
            c = connection.cursor()
            c.execute("BEGIN TRANSACTION;")
            c.execute(sql, (eh.locationobjectid, eh.offset))
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

def deleteManyHashes(connection: apsw.Connection, hashes: List[extenthash], currentSize):
    sql = ''' 
WITH toDelete AS (
    SELECT
        eh.locationobjectid,
        eh.`offset`
    FROM ExtentHash{} eh
    INNER JOIN HashesToDelete d ON
        eh.locationobjectid = d.locationobjectid
    WHERE
        eh.`offset` >= d.`offset` AND eh.`offset` < (d.`offset` + d.numbytes)
)
DELETE FROM ExtentHash{}
WHERE (locationobjectid, `offset`) IN (
    SELECT
        locationobjectid,
        `offset`
    FROM toDelete
)
        '''
    c = connection.cursor()
    c.execute('''
CREATE TEMP TABLE IF NOT EXISTS HashesToDelete(
    locationobjectid BIGINT UNSIGNED NOT NULL,
    `offset` BIGINT UNSIGNED NOT NULL,
    numbytes MEDIUMINT UNSIGNED NOT NULL,
    PRIMARY KEY(locationobjectid, `offset`)
) WITHOUT ROWID;
DELETE FROM HashesToDelete;
    ''')
    tries = 10
    for i in range(tries):
        try:
            for batch in [hashes[i:i + 64] for i in range(0, len(hashes), 64)]:
                args = [(hash.locationobjectid,hash.offset,hash.numbytes) for hash in batch]
                connection.executemany('''
REPLACE INTO HashesToDelete(locationobjectid, `offset`, numbytes)
VALUES (?,?,?)
                ''', args)
                
            sizes = reversed(HASH_SIZES)
            for size in sizes:
                if size > currentSize:
                    continue    
                c.execute(sql.format(size, size))
            
            c.execute("DELETE FROM HashesToDelete;")
        except Exception as e:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            if i < tries - 1: # i is zero indexed
                logging.exception('Retry attemp {} for setishashed'.format(i)) 
                time.sleep(3)
                continue
            else:
                logging.exception(e) 
                raise
        else:
            break
    
def getduplicationinfo(connection: apsw.Connection, size) -> List[duplicationinfo]:
    try:
        sql = ''' 
CREATE TEMP TABLE IF NOT EXISTS Hashes(
  numbytes MEDIUMINT UNSIGNED NOT NULL, 
  hash VARCHAR(32) NOT NULL,
  Occurrence MEDIUMINT UNSIGNED NOT NULL
);
DELETE FROM Hashes;

INSERT INTO Hashes(numbytes, hash, Occurrence)
SELECT DISTINCT
    eh.numbytes,
    eh.hash,
    count(1) as Occurrence
FROM ExtentHash{} eh
INNER JOIN File f ON
    f.locationobjectid = eh.locationobjectid
    AND f.ishashed = 1
GROUP BY
    eh.hash,
    eh.numbytes
HAVING 
    count(1) > 1
LIMIT 4096;

SELECT 
    eh.*
FROM ExtentHash{} eh
INNER JOIN Hashes h ON
    eh.hash = h.hash
    AND eh.numbytes = h.numbytes
                '''.format(size, size)
        c = connection.cursor()
        res = c.execute(sql)
        hashes = []
        for row in res:
            hashes.append(extenthash.fromDbObj(row))
        if len(hashes) == 0:
            return []
        groups = [(k, list(g)) for k, g in groupby(sorted(hashes, key=lambda he: (he.hash)), lambda he: (he.hash, he.numbytes))]
        infos = []
        for group in groups:
            ghe = sorted(group[1], key=lambda he: (he.locationobjectid, -he.offset), reverse=True)
            src = ghe[0]
            dests = sorted(ghe[1:], key=lambda he: -he.offset)
            if len(dests) > 0:
                info = duplicationinfo(src, dests)
                infos.append(info)
        infos.sort(key=lambda i: i.source.offset)
        return infos
    except Exception as err:
        logging.exception(err)

def hasDuplicates(connection, size):
    try:
        sql = ''' 
Select 1
WHERE 
    EXISTS (SELECT count(1) as Occurrence FROM ExtentHash{} eh GROUP BY eh.hash, eh.numbytes HAVING count(1) > 1 LIMIT 1)
            '''.format(size)
        for row in connection.execute(sql):
            return row[0] == 1
    except Exception as err:
        logging.exception(err)