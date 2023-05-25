import logging
from multiprocessing import synchronize
import sys
import time
import btrfs
from crawler import crawler
from db.dbContext import dbContext
from filehasher import filehasher
from deduplicator import deduplicator
from sync import sync
from lib.GracefulKiller import GracefulKiller

class dedupContext:
    dbPath: str
    fsPath: btrfs.FileSystem
    cancellationToken: GracefulKiller
    db: dbContext
    crawler: crawler
    filehasher: filehasher
    deduplicator: deduplicator
    sync: sync
    
    def __init__(self, dbPath, fsPath):
        self.dbPath = dbPath
        self.fsPath = fsPath
        self.cancellationToken = GracefulKiller()
        self.sync = sync(self)
        self.db = dbContext(dbPath)
        self.crawler = crawler(self)
        self.filehasher = filehasher(self)
        self.deduplicator = deduplicator(self)
        
        logger = logging.Logger('AmazeballsLogger')
        #Stream/console output
        logger.handler = logging.StreamHandler(sys.stdout)
        logger.handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        logger.handler.setFormatter(formatter)
        #File output
        fh = logging.FileHandler("test.log")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(fh)
    
    def start(self):
        fs_crawler = self.crawler.crawlContinouslyAsync()
        fs_filer = self.crawler.crawlFileContinouslyAsync()
        file_crawler = self.crawler.crawlFileInfoContinouslyAsync()
        file_hasher = self.filehasher.hashContinouslyAsync()
        synchronizer = self.sync.syncContinouslyAsync()
        dedup = self.deduplicator.deduplicateContinouslyAsync()
        
        fs_crawler.start()
        fs_filer.start()
        file_crawler.start()
        file_hasher.start()
        dedup.start()
        synchronizer.start()
        
    