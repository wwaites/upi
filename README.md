Scheduler for uniprocessor jobs on an MPI cluster
=================================================

    usage: upid.py [-h] [-c CWD] -o OUTPUT -s SCRATCH -w WORK -q QUEUE
    
    optional arguments:
      -h, --help            show this help message and exit
      -c CWD, --cwd CWD     Job working directory
      -o OUTPUT, --output OUTPUT
                            Job output directory
      -s SCRATCH, --scratch SCRATCH
                            Scratch data directory
      -w WORK, --work WORK  Working data directory
      -q QUEUE, --queue QUEUE
                            Queue directory
    
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
        mpirun -n 12 python3 -m mpi4py.futures upi/upid.py \
            -o . -w . -s /tmp -q queuedir

Requires mpi4py version 3 or greater.
