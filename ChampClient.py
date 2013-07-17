"""  

  http://github.com/sfcta/champ-client

  Timesheet, Copyright 2013 San Francisco County Transportation Authority
                            San Francisco, CA, USA
                            http://www.sfcta.org/
                            info@sfcta.org

  This file is part of Champ-Client.

  Champ-client is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  Timesheet is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Timesheet.  If not, see <http://www.gnu.org/licenses/>.
  
"""

from time import time,localtime,strftime
import os, sys, random, time, subprocess, threading
import Pyro.core


def threaded(f):
    def wrapper(*args):
        t = threading.Thread(target=f, args=args)
        t.start()
    return wrapper

class HelpRequest(Pyro.core.ObjBase):
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)

    def help(self, dispatcher=None,single=False):
        print "\r" + strftime("%x %X", localtime()) + " Request for help from:",dispatcher[10:]
        
        SpawnAllHelpers(dispatcher,single)

# -------------------------------------------------------------------------
# This next function always spawns a new thread.  

@threaded
def SpawnAllHelpers(dispatcher,single):
    if (single):
        SpawnHelper(dispatcher)
    else:
        for i in range(NUMCPU):
            SpawnHelper(dispatcher)
            # We wait a few secs to help distribute jobs across more machines
            # before we distribute across multi-CPU's.
            time.sleep(3)
        
# -------------------------------------------------------------------------
# This next function always spawns a new thread.  

@threaded 
def SpawnHelper(dispatcher):
    # print "SpawnHelper"
    runner = CPURunner.acquire()  # block, until a CPU is free
    # print "CPU Acquired"
    
    # Now we have a CPU.  Let's try and pull a Job.
    try:
        joblist = Pyro.core.getProxyForURI(dispatcher)
        while True:
            # Pull job from dispatcher 
            jobnum, job = joblist.get()
            if job==None:
                break
            print "\n# " + strftime("%x %X", localtime()) + " Starting:  ",job.cmd,"\n# ("+job.workdir+")"
            rtncode, logname = RunJob(job,joblist,"source")

            # Job completed. Notify dispatcher this one's finished.
            print "\n# " + strftime("%x %X", localtime()) + " Finished:",job.cmd,"\n### "+job.workdir
            joblist.alldone(jobnum, job, rtncode, logname)
            # And, loop back and try to get another.

    except:   # Code drops to here when dispatcher daemon drops connection.
        pass
        
    # All done with this jobqueue, let's give the CPU back and exit.
    CPURunner.release()

    if (threading.activeCount() == 3): # 3 means no more jobs are using the CPU!
        print "\n---------------------------------\nWaiting...",
    return


def RunJob(job, joblist, source):
    # Set up job attributes -- working dir, environment, killstatus

    # Open logfile
    logname = ""
    log = ""
    
    try:
        logname = os.path.join(job.workdir,
            "dispatch-"+str(random.randint(100000000,999999999))+".log")
        log = open(logname, "w")
    except:
        # Couldn't cd to workdir, or couldn't open logfile.  Die.
        return 3, None
    
    
    # Set up the environment
    envvars = os.environ.copy()
    for key,val in job.env.iteritems():
        if key=="PATH_PREFIX":
            envvars['PATH'] = val + envvars['PATH']
        else:
            envvars[key] = val
    print "\n#   Environment PATH:", envvars['PATH']

    # Spawn the process
    child = subprocess.Popen(job.cmd,cwd=job.workdir, env=envvars, shell=True,
        stdout=log, stderr=log)

    wait = 0
    rtncode = None
    
    while (rtncode == None):
        try:
            time.sleep(1)
            rtncode = child.poll()
            
            # Check for kill request
            if (wait % 10 == 0):
                wait = 0

                if (joblist.killMe() == True): 
                    print "Got Kill Request"
                    kill = subprocess.Popen("taskkill /F /T /PID %i" % child.pid, shell=True)
                    rtncode = 0
                    break
            wait += 1
        except:
            print "Lost connection:  Killing job!"
            kill = subprocess.Popen("taskkill /F /T /PID %i" % child.pid, shell=True)
            rtncode = 0
            break
        
    # Done!  Close things out.
    # Concatenate logfiles (bug in python.. drat!)
    log.close()
    log = open(logname, "r")

    # Using a threadsafe lock function so that multiple threads can append output
    # to the logfile without tripping on each other.
    LOGGERLOCK.acquire()

    logfile = open(os.path.join(job.workdir,LOGGERNAME),"a")
    for line in log:
        logfile.write(line)
    logfile.write("======= Finished "+time.asctime()+" ==============================\n")

    # Close out the logfiles and set to null so windows can delete them.
    log.flush()
    log.close()
    logfile.flush()
    logfile.close()
    log=None
    logfile=None

    try:
        os.remove(logname)
    except:
        pass # print sys.exc_info()  sometimes Windows doesn't release the logfile... :-(

    LOGGERLOCK.release()
    return rtncode, logname


# ------------------------------------------------------------
# Empty class, just to hold a structure of public variables:
# Job: cmd, dir, env, cmdwithargs

class Job:
    pass
    

# ------------------------------------------------------------
# Initialization!
if (__name__ == "__main__"):
    LOGGERNAME = os.getenv("COMPUTERNAME")+".log"
    LOGGERLOCK = threading.Lock()

    NUMCPU = int(os.getenv("NUMBER_OF_PROCESSORS"))
    if len(sys.argv) > 1:
        NUMCPU = int(sys.argv[1])

    CPURunner = threading.Semaphore(NUMCPU)

    # --- Infrastructure now ready; let's fire up the Pyro listener.
    try:
        Pyro.core.initServer(banner=0)
        daemon = Pyro.core.Daemon()
        print "The daemon runs on port:",daemon.port        
        uri = daemon.connect(HelpRequest(),"help")
        print "\nSF-CHAMP Dispatcher Client:",NUMCPU,"CPU\n---------------------------------"
        print strftime("%x %X", localtime()) + " Waiting...",
        daemon.requestLoop()
    except:
        print sys.exc_info()
