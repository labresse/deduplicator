import time
import logging
from threading import Thread, Event
from db.database import database
from db.dbContext import dbContext
import dedupContext
from typing import List, Dict

class sync:
    ctx: dedupContext
    synceventstep1 = Event()
    synceventstep2 = Event()
    db: database
    crawlerevent: Event
    fileevent: Event
    fileinfoevent: Event
    hasherevent: Event
    deduplicatorevent: Event

    def __init__(self,ctx):
        self.ctx = ctx
        self.db = database(self.ctx.dbPath)
        self.synceventstep1.set()
        self.synceventstep2.set()
        
    def syncContinouslyAsync(self) -> Thread:
        return Thread(target=self.syncFileContinously, name="sync")

    def syncFileContinously(self):
        i = 0
        while not self.ctx.cancellationToken.kill_now:
            try:
                self.synceventstep1.clear()
                time.sleep(1)
                self.deduplicatorevent.wait()
                self.crawlerevent.wait()
                self.fileevent.wait()
                self.fileinfoevent.wait()
                self.synceventstep2.clear()
                time.sleep(1)
                self.hasherevent.wait()
                
                i += 1
                connection = self.db.sync()
                self.ctx.db.sync(connection)
                connection.execute("vacuum")
                connection.close()
                connection = database(self.ctx.dbPath).initmain()
                if i == 10:
                    connection.execute("vacuum")
                    print("vacuumed")
                    i = 0
                connection.close()

                self.synceventstep1.set()
                self.synceventstep2.set()

                time.sleep(300)
            except Exception as e:
                logging.error(e)