from mpi4py.futures import MPIPoolExecutor
import mpi4py
import argparse
import os
import time
import traceback
from datetime import datetime
from subprocess import Popen, PIPE
import json
from threading import Thread
from queue import Queue
import logging
import socket
from sys import argv

SHELL = "/bin/bash"

def wraprun(cmd, args, job):
    """
    Wrap the runproc function, catching any exceptions so as
    to avoid crashing the scheduler if something goes wrong
    """
    try:
        status = runproc(cmd, args, job)
    except Exception as e:
        status = {
            "host": socket.gethostname(),
            "process": mpi4py.MPI.COMM_WORLD.rank,
            "job": job,
            "command": cmd,
            "error": traceback.format_exc()
        }

    return status

def runproc(cmd, args, job):
    """
    Set up the environment and run the job, returning a
    dictionary describing how it went
    """
    scratch = os.path.join(args.scratch, str(mpi4py.MPI.COMM_WORLD.rank), str(job))

    try:
        os.makedirs(args.work)
    except FileExistsError as e:
        pass

    env = os.environ.copy()
    env["SCRATCH"] = scratch
    env["WORK"] = args.work

    os.chdir(args.cwd)

    os.makedirs(scratch)

    cmdf = "%s.job" % cmd
    start = datetime.utcnow()
    proc = Popen([SHELL, cmdf], env=env, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate() ## xxx some sort of timeout
    end = datetime.utcnow()

    os.removedirs(scratch)

    outf = "%s.out" % cmd
    with open(outf, "wb") as fp:
        fp.write(out)
    errf = "%s.err" % cmd
    with open(errf, "wb") as fp:
        fp.write(err)

    status = {
        "host": socket.gethostname(),
        "process": mpi4py.MPI.COMM_WORLD.rank,
        "job": job,
        "command": cmd,
        "start": str(start),
        "end": str(end),
        "elapsed": str(end-start),
        "stdout": outf,
        "stderr": errf,
        "status": proc.returncode
    }

    statf = "%s.status" % cmd
    with open(statf, "w") as fp:
        fp.write(json.dumps(status))

    return status

def dequeue(args, queue):
    """
    Perpetually walk the configured queue directory and put jobs
    onto the queue for running.
    """
    job = 0
    while True:
        for dirname, _, files in os.walk(args.queue):
            for filename in files:
                filepath = os.path.join(dirname, filename)

                ## copy job file into the output directory
                cmdbase = os.path.basename(filepath[:-4] if filepath.endswith(".job") else filepath) + ".%d" % job
                cmdf = os.path.join(args.output, "%s.job" % cmdbase)
                with open(filepath) as fi:
                    with open(cmdf, "w") as fo:
                        fo.write(fi.read())
                ## and remove from the input queue directory
                os.unlink(filepath)

                queue.put((os.path.join(args.output, cmdbase), args, job))
                job += 1
        time.sleep(1)

def finish(av):
    """
    Finalise a job if it has finished -- read its output and return
    false so that it gets filtered out of the active job list. If it
    is not finished, return true.
    """
    (cmd, _, job), future = av
    if future.done():
        log = logging.getLogger("finish")
        result = future.result()
        if "status" in result:
            log.info("job %(job)s status %(status)s" % result)
        else:
            log.error("job %(job)s %(error)s" % result)
        return False
    else:
        return True

def main():
    """
Command line arguments set the working directory of the spawned process,
the queue directory for jobs and the scratch and persistent work directories.

The queue directory is scanned continually for files describing jobs to 
run. They are expected to be Bourne-Again Shell scripts. Their standard
output, standard error and a status file is written into the output
directory. These are read into memory so do not try to use standard output
and standard error for significant I/O.

Jobs are responsible for their own recovery of partial data if they are
ended prematurely. The lifetime of the scratch directory is the lifetime
of the job. Persistent data should be (perhaps atomically) written
or moved into the working directory. The job can find out what these
directories are by examining the SCRATCH and WORK environment variables.

Example:

    ## copy the example jobs to a queue directory
    mkdir queuedir; cp example_queue/* queuedir

    ## run 12 concurrent jobs, taking jobs from queuedir
    ## write job output and data to the current directory
    mpirun -n 12 python3 -m mpi4py.futures upi/upid.py \\
        -o . -w . -s /tmp -q queuedir

This program needs to be run under the `mpi4py.futures` module.
"""

    parser = argparse.ArgumentParser(argv[0], description="Scheduler for uniprocessor jobs on an MPI cluster",
                                     epilog=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-c", "--cwd", type=str, default=os.getcwd(), help="Job working directory")
    parser.add_argument("-o", "--output", type=str, required=True, help="Job output directory")
    parser.add_argument("-s", "--scratch", type=str, required=True, help="Scratch data directory")
    parser.add_argument("-w", "--work", type=str, required=True, help="Working data directory")
    parser.add_argument("-q", "--queue", type=str, required=True, help="Queue directory")
    args = parser.parse_args()

    FORMAT = '%(asctime)-15s %(name)s %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    queue = Queue(maxsize=1)
    qrunner = Thread(target=dequeue, args=(args, queue))
    qrunner.start()
    jobs = []

    log = logging.getLogger("main")
    log.info("starting executor")

    with MPIPoolExecutor() as executor:
        while True:
             jobs = list(filter(finish, jobs))
             if len(jobs) >= mpi4py.MPI.COMM_WORLD.size - 1:
                 time.sleep(0.1)
                 continue
             if queue.empty():
                 time.sleep(0.1)
                 continue

             jobspec = queue.get()
             log.info("submit job %d %s" % (jobspec[2], jobspec[0]))
             future = executor.submit(wraprun, *jobspec)
             jobs.append((jobspec, future))

if __name__ == "__main__":
    main()
