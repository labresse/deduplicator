import sys
import time
import os
import multiprocessing
from dedupContext import dedupContext

if len(sys.argv) < 2:
    sys.exit(1)

if __name__ == '__main__':
    try:
        multiprocessing.set_start_method("spawn")
        os.system("taskset -p 0xff %d" % os.getpid())
        ctx = dedupContext("/dedupe/.dedupe/", sys.argv[1])
        ctx.start()
    except RuntimeError:
        pass
    while not ctx.cancellationToken.kill_now:
        time.sleep(10)
    