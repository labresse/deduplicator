import apsw
import logging
import time
from threading import Thread

class database:
    dbPath: str
    
    def __init__(self, dbPath):
        self.dbPath = dbPath
    
    def inittemp(self) -> apsw.Connection:
        tries = 10
        for i in range(tries):
            try:
                connection = apsw.Connection(
                    filename= '/dev/shm/' + 'dedup.db',
                    flags=apsw.SQLITE_OPEN_READWRITE|apsw.SQLITE_OPEN_CREATE|apsw.SQLITE_OPEN_FULLMUTEX)
                connection.execute('pragma busy_timeout=2147483647;')
                connection.execute('PRAGMA synchronous = NORMAL')
                connection.execute('PRAGMA cache_size = 65536')
                connection.execute('PRAGMA secure_delete = OFF')
                connection.execute('PRAGMA journal_mode = WAL2')
                connection.execute('PRAGMA mmap_size = 1073741824')
                connection.execute("PRAGMA cache_size=65536")
                connection.execute("PRAGMA page_size=65536")
                connection.execute('PRAGMA secure_delete = OFF')
                return connection;
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
    
    def sync(self) -> apsw.Connection:
        tries = 10
        for i in range(tries):
            try:
                connection = apsw.Connection(
                    filename= '/dev/shm/' + 'dedup.db',
                    flags=apsw.SQLITE_OPEN_READWRITE|apsw.SQLITE_OPEN_FULLMUTEX)
                connection.execute('pragma busy_timeout=60000;')
                connection.execute('PRAGMA synchronous = NORMAL')
                connection.execute('PRAGMA cache_size = 65536')
                connection.execute('PRAGMA journal_mode = WAL2')
                connection.execute('PRAGMA mmap_size = 1073741824')
                connection.execute("PRAGMA cache_size=65536")
                connection.execute("PRAGMA page_size=65536")
                connection.execute('PRAGMA secure_delete = OFF')
                connection.execute("ATTACH DATABASE '{}dedup.db' AS 'store'".format(self.dbPath))
                connection.execute('pragma store.busy_timeout=60000;')
                connection.execute('PRAGMA store.synchronous = NORMAL')
                connection.execute('PRAGMA store.journal_mode = WAL2')
                connection.execute('PRAGMA store.temp_store = MEMORY')
                connection.execute('PRAGMA store.mmap_size = 1073741824')
                connection.execute("PRAGMA store.cache_size=-65536")
                connection.execute("PRAGMA store.page_size=65536")
                connection.execute('PRAGMA store.auto_vacuum = FULL')
                connection.execute('PRAGMA store.secure_delete = OFF')
                return connection;
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
    
    def initmain(self) -> apsw.Connection:
        tries = 10
        for i in range(tries):
            try:
                connection = apsw.Connection(
                    filename= self.dbPath + 'dedup.db',
                    statementcachesize=0,
                    flags=apsw.SQLITE_OPEN_READWRITE|apsw.SQLITE_OPEN_CREATE|apsw.SQLITE_OPEN_NOMUTEX)
                connection.execute('pragma busy_timeout=2147483647;')
                connection.execute('PRAGMA synchronous = NORMAL')
                connection.execute('PRAGMA journal_mode = WAL2')
                connection.execute('PRAGMA temp_store = MEMORY')
                connection.execute('PRAGMA mmap_size = 1073741824')
                connection.execute("PRAGMA cache_size=-65536")
                connection.execute("PRAGMA page_size=65536")
                connection.execute('PRAGMA secure_delete = OFF')
                return connection;
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
        
            
    
