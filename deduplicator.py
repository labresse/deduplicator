import logging
import time
import btrfs
import os
import datetime
from multiprocessing.pool import ThreadPool, Pool
from itertools import groupby
from threading import Thread, Event
from db.database import database
import db.extenthashdao as extenthashdao
import db.filedao as filedao
from btrfs.ctree import ULL
from btrfs.ctree import FS_TREE_OBJECTID
from btrfs.ioctl import FILE_DEDUPE_RANGE_SAME, FILE_DEDUPE_RANGE_DIFFERS
import dedupContext as dedupContext
from models.extenthash import HASH_SIZES
from models.duplicationinfo import duplicationinfo

class deduplicator:
    ctx: dedupContext
    deduplicatorevent = Event()
    db: database
    
    def __init__(self,ctx):
        self.ctx = ctx
        self.db = database(self.ctx.dbPath)
        self.ctx.sync.deduplicatorevent = self.deduplicatorevent
        
    def deduplicateContinouslyAsync(self) -> Thread:
        return Thread(target=self.deduplicateContinously, name="deduplicator")

    def deduplicateContinously(self):
        while not self.ctx.cancellationToken.kill_now:
            try:
                self.deduplicate()
            except Exception as e:
                logging.error(e)

    def deduplicate(self):
        try:
            self.deduplicatorevent.clear()
            connection = self.db.initmain()
            # time.sleep(10)
            # return
            
            sizes = list(reversed(HASH_SIZES))
            sizes.insert(0, 4096)
            for previous, size in zip(sizes, sizes[1:]):                
                fileInfos = extenthashdao.getduplicationinfo(connection, size)
                if len(fileInfos) == 0:
                    continue
                
                self.deduplicatorevent.clear()
                previousHasDuplicate = False
                
                while len(fileInfos) != 0:
                    print("deduping {}".format(size))
                    
                    files = [(k, list(g)) for k, g in groupby(fileInfos, lambda fi: fi.source.locationobjectid)]
                    with ThreadPool(2) as pool:
                        hashesToDelete = []

                        for (file, infos) in files:
                            args = [(info, size, ) for info in infos]
                            res = pool.starmap(self.deduplicateOneFile, args)
                            for r in res:
                                for eh in r:
                                    if eh[0] == FILE_DEDUPE_RANGE_SAME:
                                        hashesToDelete.append(eh[1])
                                    
                        pool.close()   
                        pool.join()

                        extenthashdao.deleteManyHashes(connection, hashesToDelete, size)
                    
                    self.deduplicatorevent.set()
                    self.ctx.sync.synceventstep1.wait()
                    
                    previousHasDuplicate = extenthashdao.hasDuplicates(connection, previous)
                    if previousHasDuplicate == True:
                        break
                    fileInfos = extenthashdao.getduplicationinfo(connection, size)
                if previousHasDuplicate == True:
                    break
                
            connection.close()    
            self.deduplicatorevent.set()
            self.ctx.sync.synceventstep1.wait()
        except Exception as err:
            logging.error(err)

    def deduplicateOneFile(self, info: duplicationinfo, size):
        result = []
        connection = self.db.initmain()
        with btrfs.FileSystem(self.ctx.fsPath) as fs:
            try:
                fileinfo = btrfs.btrfs.ioctl.ino_lookup(fs.fd, FS_TREE_OBJECTID, info.source.locationobjectid)
                info.source.filePath = os.path.join(self.ctx.fsPath, str(fileinfo.name_bytes, 'utf-8').rstrip('/'))
            except FileNotFoundError as err:
                self.ctx.db.deleteFile(connection, info.source.locationobjectid)
                return
            
            deleted = []
            for hash in info.destinations:
                try:
                    fileinfo = btrfs.btrfs.ioctl.ino_lookup(fs.fd, FS_TREE_OBJECTID, ULL(int(hash.locationobjectid)))
                    hash.filePath = os.path.join(self.ctx.fsPath, str(fileinfo.name_bytes, 'utf-8').rstrip('/'))
                except FileNotFoundError as err:
                    deleted.append(hash)                
            for hash in deleted:
                self.ctx.db.deleteFile(connection, hash.locationobjectid)
                info.destinations.remove(hash)
            if len(info.destinations) == 0:
                return
            
            info.destinations.sort(key=lambda d: d.offset)
            for chunk in self.chunks(info.destinations, 32):
                src = info.source.filePath
                src_offset = info.source.offset
                src_length = info.source.numbytes
                dest_names = []
                range_infos = []
                try:
                    for dest in chunk:
                        try:
                            dest_fd = os.open(dest.filePath, os.O_RDONLY)
                            dest_offset = dest.offset
                            dedupinfo = btrfs.ioctl.FileDedupeRangeInfo(dest_fd, dest_offset)
                            dest_names.append(dest)
                            range_infos.append(dedupinfo)
                        except FileNotFoundError as e:
                            return
                    fd = None
                    try:
                        fd = os.open(src, os.O_RDONLY)
                    except FileNotFoundError as e:
                        return
                        
                    btrfs.ioctl.fideduperange(fd, src_offset, src_length, range_infos)
                    for (destinfo, hash) in zip(range_infos, chunk):
                        if destinfo.status == FILE_DEDUPE_RANGE_SAME:
                            result.append([FILE_DEDUPE_RANGE_SAME, hash])
                            #extenthashdao.deleteSmaller(connection, hash, size)
                            # print("Same {}, hash size {}, {} {}".format(destinfo, size, hash.locationobjectid, hashoffset))
                        elif destinfo.status == FILE_DEDUPE_RANGE_DIFFERS:
                            if self.resetifmodified(connection, hash, destinfo, size) == False:
                                print("Possible hash collision source {}, hash size {}, {} {}".format(info.source.locationobjectid, info.source.numbytes, info.source.locationobjectid, info.source.offset))
                                print("Possible hash collision dest {}, hash size {}, {} {}".format(hash.locationobjectid, hash.numbytes, hash.locationobjectid, hash.offset))
                        else:
                            if self.resetifmodified(connection, hash, destinfo, size) == False:
                                print("Error source {}, hash size {}, {} {}".format(info.source.locationobjectid, info.source.numbytes, info.source.locationobjectid, info.source.offset))   
                                print("Error dest {}, hash size {}, {} {}".format(hash.locationobjectid, hash.numbytes, hash.locationobjectid, hash.offset))
                except OSError as e:
                    if self.resetifmodified(connection, hash, src, size) == False:    
                        print("OSError source {}, hash size {}, {} {}".format(info.source.locationobjectid, info.source.numbytes, info.source.locationobjectid, info.source.offset))
                except Exception as e:
                    logging.error(e)
                finally:
                    os.close(fd)
                    for rangeInfo in range_infos:
                        os.close(rangeInfo.dest_fd)
        connection.close()
        return result
    
    def resetifmodified(self, connection, hash, destinfo, size):
        file = filedao.getById(connection, hash.locationobjectid)
        if file == None:
            return 
        
        stats = os.stat(hash.filePath)
        modifiedtime = int(stats.st_ctime)
        if file.modifiedtime == modifiedtime:
            return False
        
        print("Differs {}, hash size {}, {} {}".format(destinfo, size, hash.locationobjectid, hash.offset))
        self.ctx.db.deleteFile(connection, hash.locationobjectid)
        self.ctx.db.requeueFile(connection, hash.locationobjectid)
        return True

    def chunks(self, data, rows=16):
        for i in range(0, len(data), rows):
            yield data[i:i+rows]