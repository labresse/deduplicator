import btrfs
import logging
import time
import os
from threading import Thread, Event
from multiprocessing.pool import ThreadPool
from db.database import database
import db.filedao as filedao
import db.fileextentitemdao as fileextentitemdao
from btrfs.ctree import Key, DirItemList
from btrfs.ctree import FS_TREE_OBJECTID, EXTENT_DATA_KEY, EXTENT_TREE_OBJECTID, DIR_ITEM_KEY, ULLONG_MAX
import dedupContext
from models.file import file


class crawler:
    ctx: dedupContext
    crawlerevent = Event()
    fileevent = Event()
    fileinfoevent = Event()
    db: database
    
    def __init__(self,ctx):
        self.ctx = ctx
        self.db = database(self.ctx.dbPath)
        self.ctx.sync.crawlerevent = self.crawlerevent
        self.ctx.sync.fileevent = self.fileevent
        self.ctx.sync.fileinfoevent = self.fileinfoevent
        
    def crawlContinouslyAsync(self) -> Thread:
        return Thread(target=self.crawlContinously, name="crawler")
    
    def crawlContinously(self):
        counter = 0
        while not self.ctx.cancellationToken.kill_now:
            try:
                if counter == 100:
                    with self.db.initmain() as connection:
                        fileextentitemdao.truncate(connection)    
                    self.crawlFileExtentItem(0)
                    counter = 0 
                else:
                    maxfileid = 0
                    with self.db.initmain() as connection:
                        maxfileid = fileextentitemdao.getMaxId(connection)
                    self.crawlFileExtentItem(maxfileid)
                    counter += 1
            except Exception as e:
                logging.error(e)
            time.sleep(10)

    def crawlFileExtentItem(self, minKeyId, maxKeyId = ULLONG_MAX):
        min_key = Key(minKeyId, EXTENT_DATA_KEY, 0)
        max_key = Key(maxKeyId, EXTENT_DATA_KEY, ULLONG_MAX)
        with btrfs.FileSystem(self.ctx.fsPath) as fs:
            rows = []
            connection = self.db.inittemp()
            for data in fs.search(FS_TREE_OBJECTID, min_key, max_key):
                if(type(data) == btrfs.btrfs.ctree.FileExtentItem and data.type == 1):
                    rows.append(data)
                    if len(rows) % 1024 == 0:
                        self.crawlerevent.clear()
                        fileextentitemdao.insertmany(connection, rows)
                        self.crawlerevent.set()
                        self.ctx.sync.synceventstep1.wait()
                        rows = []
            
            self.crawlerevent.clear()     
            fileextentitemdao.insertmany(connection, rows)
            self.crawlerevent.set()
            self.ctx.sync.synceventstep1.wait()
            connection.close()
        
    def crawlFileContinouslyAsync(self) -> Thread:
        return Thread(target=self.crawlFileContinously, name="file_crawler")

    def crawlFileContinously(self):
        try:
            while not self.ctx.cancellationToken.kill_now:
                self.fileevent.clear()
                with self.db.sync() as connection:
                    self.ctx.db.insertNewFiles(connection)

                self.fileevent.set()
                self.ctx.sync.synceventstep1.wait()
                time.sleep(10)
        except Exception as e:
            logging.error(e)
            
    def crawlFileInfoContinouslyAsync(self) -> Thread:
        return Thread(target=self.crawlFileInfoContinously, name="fileinfo_crawler")

    def crawlFileInfoContinously(self):
        while not self.ctx.cancellationToken.kill_now:
            try:
                self.crawlFileInfo()
                time.sleep(10)
            except Exception as e:
                logging.error(e)
            
    def crawlFileInfo(self):
        self.fileinfoevent.clear()
        connection = self.db.inittemp()
        files = filedao.getUnresolvedFilePaths(connection)
        if len(files) == 0:
            self.fileinfoevent.set()
            self.ctx.sync.synceventstep1.wait()
            return
            
        while len(files) != 0:
            self.fileinfoevent.clear()
            
            args = [(p, ) for p in files]
            with ThreadPool(6) as pool:
                results = pool.starmap(self.resolveFileInfo, args)
                toUpdate = []
                todelete = []
                for r in results:
                    if r[1] == True:
                        toUpdate.append(r[0])
                    elif r[1] == False:
                        todelete.append(r[0])
                filedao.updateFileInfos(connection, toUpdate)
                for d in todelete:
                    self.ctx.db.deleteFile(connection, d.locationobjectid)
            files = filedao.getUnresolvedFilePaths(connection)
            
            self.fileinfoevent.set()
            self.ctx.sync.synceventstep1.wait()
        connection.close()

    def resolveFileInfo(self, file: file):
        try:
            with btrfs.FileSystem(self.ctx.fsPath) as fs:
                fileinfo = btrfs.btrfs.ioctl.ino_lookup(fs.fd, FS_TREE_OBJECTID, file.locationobjectid)
                path = os.path.join(self.ctx.fsPath, str(fileinfo.name_bytes, 'utf-8').rstrip('/'))
                stats = os.stat(path)
                file.modifiedtime = int(stats.st_ctime)
                return (file, True)
        except FileNotFoundError:
            return (file, False)
        except Exception as e:
            logging.error(e)