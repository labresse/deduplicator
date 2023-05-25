from pydoc import doc
from threading import Thread, Event
from multiprocessing.pool import ThreadPool, Pool
import time
import logging
import btrfs
from btrfs.ctree import FS_TREE_OBJECTID
import os
import xxhash
from db.database import database
from db.dbContext import dbContext
import db.filedao as filedao
import db.extenthashdao as extenthashdao
from models.extenthash import extenthash, extenthashes, HASH_SIZES
import dedupContext
from models.file import file
from lib.punch import punch

class filehasher:
    ctx: dedupContext
    hasherevent = Event()
    db: database
    
    def __init__(self,ctx):
        self.ctx = ctx
        self.db = database(self.ctx.dbPath)
        self.ctx.sync.hasherevent = self.hasherevent
        
    def hashContinouslyAsync(self) -> Thread:
        return Thread(target=self.hashContinously, name="hasher_main")

    def hashContinously(self):
        while not self.ctx.cancellationToken.kill_now:
            try:
                self.hasherevent.clear()
                files = []
                connection = self.db.inittemp()
                files = filedao.getUnhashedFile(connection)
                if len(files) == 0:
                    self.hasherevent.set()
                    self.ctx.sync.synceventstep1.wait()
                    time.sleep(10)
                    continue
                
                while len(files) != 0:
                    self.hasherevent.clear()

                    with Pool(6) as pool:
                        batches = [files[i:i + 1024] for i in range(0, len(files), 1024)]
                        args = [(self.ctx.fsPath, self.db, f,) for f in batches]
                        pool.starmap_async(hashthreaded, args)                            
                        pool.close()
                        pool.join()
                        
                    self.hasherevent.set()
                    self.ctx.sync.synceventstep2.wait()
                    
                    files = filedao.getUnhashedFile(connection)
                time.sleep(10)
            except Exception as e:
                logging.error(e)
    
def hashthreaded(fsPath, db, files):
    with ThreadPool(1) as pool:
        args = [(fsPath, db, f,) for f in files]
        pool.starmap_async(hashFile, args)                            
        pool.close()
        pool.join()
                    
def hashFile(fsPath, db: database, fileInfo: file):
    try:
        hashes: extenthashes
        hashes = hash(fsPath, fileInfo)
        fileInfo.hashes = hashes
        connection = db.inittemp()
        extenthashdao.bulkinsert(connection, hashes)
        filedao.setishashed(connection, fileInfo.locationobjectid)
        connection.close()
    except FileNotFoundError:
        connection = db.inittemp()
        ctx = dbContext(db.dbPath)
        ctx.deleteFile(connection, fileInfo.locationobjectid)
        connection.close()
    except Exception as e:
        logging.error(e)

def hash(fsPath, fileInfo: file):
    hashes = extenthashes()
    with btrfs.FileSystem(fsPath) as fs:
        ino = btrfs.btrfs.ioctl.ino_lookup(fs.fd, FS_TREE_OBJECTID, fileInfo.locationobjectid)
        path = os.path.join(fsPath, str(ino.name_bytes, 'utf-8').rstrip('/'))
        with open(path, "rb") as in_file:
            index = 0
            index2 = 0
            index4 = 0
            index8 = 0
            index16 = 0
            index32 = 0
            index4096 = 0
            while True:
                chunk = in_file.read(4096)
                if chunk == b"":
                    break # end of file
                
                index = hashBlock(hashes, fileInfo.locationobjectid, index, chunk, 1)
                index2 = hashBlock(hashes, fileInfo.locationobjectid, index2, chunk, 2)
                index4 = hashBlock(hashes, fileInfo.locationobjectid, index4, chunk, 4)
                index8 = hashBlock(hashes, fileInfo.locationobjectid, index8, chunk, 8)
                index16 = hashBlock(hashes, fileInfo.locationobjectid, index16, chunk, 16)
                index32 = hashBlock(hashes, fileInfo.locationobjectid, index32, chunk, 32)
                index4096 = hashBlock(hashes, fileInfo.locationobjectid, index4096, chunk, 4096)
    return hashes

def hashBlock(hashes, locationobjectid, index, chunk, blocksize):
    getattr(hashes, "hashdata{}".format(blocksize)).append(chunk)
    length = len(getattr(hashes, "hashdata{}".format(blocksize)))
    if  (length % blocksize) != 0:
        return index
    
    chunkbytes = getattr(hashes, "hashdata{}".format(blocksize))
    data = b"".join(chunkbytes)
    if blocksize > 1 and len(data) != blocksize * 4096:
        return index
    
    eh = extenthash(
        locationobjectid = locationobjectid,
        offset = index * blocksize * 4096,
        numbytes=len(data),
        hash = xxhash.xxh128(data).hexdigest() 
    )
    getattr(hashes, "extenthashes{}".format(blocksize)).append(eh)
    getattr(hashes, "hashdata{}".format(blocksize)).clear()
    index += 1
    return index