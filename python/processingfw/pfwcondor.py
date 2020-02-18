# $Id: pfwcondor.py 47226 2018-07-12 20:24:15Z friedel $
# $Rev:: 47226                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-07-12 15:24:15 #$:  # Date of last commit.

# pylint: disable=print-statement

""" Utilities for interactions with Condor """

import subprocess
from datetime import datetime
import shlex
import os
import re
import time
import despymisc.miscutils as miscutils
import processingfw.pfwdefs as pfwdefs

class CondorException(Exception):
    "class for Condor exceptions"
    def __init__(self, txt):
        super().__init__()
        self.txt = txt
    def __str__(self):
        return self.txt


def condor_version():
    """Calls condor_version command and returns the version
       in string format easy to compare"""

    cmd = 'condor_version'

    try:
        process = subprocess.Popen(cmd.split(), shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   text=True)
        process.wait()
        if process.returncode != 0:
            raise CondorException("Problem running condor_version - non-zero exit code")
    except:
        raise CondorException("Error: Could not run condor_version. Check PATH.")

    version = ''
    out = process.communicate()[0]
    #print(out, type(out))
    result = re.search(r'CondorVersion: (\d+)\.(\d+)\.(\d+)', str(out))
    if result:
        version = f"{int(result.group(1)):03d}.{int(result.group(2)):03d}.{int(result.group(3)):03d}"
    else:
        raise CondorException(f"Could not determine condor_version ({out})")

    #print 'version = '%s'\n' % version
    return version


###########################################################################
def compare_condor_version(ver2):
    """Compare running condor version against given version"""
    # similar to strcmp
    # < 0 if current < ver2
    #   0 if current = ver2
    # > 0 if current > ver2

    if isinstance(ver2, float):
        ver2 = str(ver2)
    elif not isinstance(ver2, str):
        print("Invalid ver2 type: ", type(ver2), ver2)
        raise Exception("Invalid ver2 type")

    comp = 0

    # repad numbers to ensure easy comparision
    result = re.search(r'(\d+)\.(\d+)\.(\d+)', ver2)
    if result:
        ver2 = f"{int(result.group(1)):03d}.{int(result.group(2)):03d}.{int(result.group(3)):03d}"
    else:
        result = re.search(r'(\d+)\.(\d+)', ver2)
        if result:
            ver2 = f"{int(result.group(1)):03d}.{int(result.group(2)):03d}.000"
        else:
            raise CondorException("Invalid version format")

    currver = condor_version()
    if currver == ver2:
        comp = 0
    elif currver < ver2:
        comp = -1
    else:
        comp = 1

    return comp



###########################################################################
def condor_submit(submitfile):
    """Call condor_submit on given condor description file"""

    cmd = f"condor_submit {submitfile}"

    try:
        process = subprocess.Popen(cmd.split(), shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   text=True)
        process.wait()
    except:
        raise CondorException("Error: Could not run condor_submit.  Check PATH.")

    return process.returncode, process.communicate()


###########################################################################
def create_resource(info):
    """ Create string for globus_rsl line in condor description file """
    gridresource = ''

    print("\ncreateResource: ", info)

    if 'gridresource' in info:
        gridresource += info['gridresource']
    else:
        # check for needed info to build string
        if 'gridtype' in info:
            gridtype = info['gridtype'].lower()
            if gridtype == 'prews':  # handle deprecated prews keyword
                gridtype = 'gt2'
            elif gridtype not in ['gt2', 'gt5', 'condor-ce']:
                raise CondorException(f"Invalid gridtype {gridtype}")
        else:
            gridtype = 'gt5'

        if 'gridhost' in info:
            gridhost = info['gridhost']
        else:
            raise CondorException("Missing gridhost")

        # create gridresource string
        gridresource = None
        if gridtype in ['gt2', 'gt5']:
            gridresource = gridtype
        elif gridtype == 'condor-ce':
            gridresource = 'condor ' + gridhost

        gridresource += ' ' + gridhost

        if 'gridport' in info:
            gridresource += ':' + info['gridport']

        if gridtype in ['gt2', 'gt5']:
            if 'batchtype' in info:
                batchtype = info['batchtype'].lower()
                gridresource += '/jobmanager-' + batchtype
            else:
                raise CondorException("Missing batchtype")

    return gridresource


###########################################################################
def create_rsl(info):
    """Create RSL for grid job"""
    rslparts = []

    print("info=", info)
    for key in ['stdout', 'stderr']:
        if key in info:
            rslparts.append(f"({key}={info[key]})")

    if 'batchtype' in info:
        batchtype = info['batchtype'].lower()
        if batchtype not in ['fork', 'condor-ce']:
            # used psn to distinguish from DESDM project
            if 'psn' in info:
                rslparts.append(f"(project={info['psn']}")

            batchkeys = ('maxwalltime', 'maxtime', 'queue', 'jobtype',
                         'maxmemory', 'minmemory', 'hostxcount', 'xcount',
                         'hosttypes', 'count', 'reservationid')
            for key in batchkeys:
                if key in info:
                    rslparts.append(f"({key}={info[key]})")

    if 'globusextra' in info:
        rslparts.append(info['globusextra'])

    if 'environment' in info:
        env = ''
        infoenv = info['environment']
        if isinstance(infoenv, dict):
            for (key, val) in infoenv.items():
                env += f"({key.upper()} {val})"
        else:
            env = infoenv
        rslparts.append(f"(environment={env}")

    print("rslparts=", rslparts)
    return ''.join(rslparts)


def create_condor_env(envvars):
    """Create string for environment line in condor description file"""
    # see rules in environment section of condor_submit manual page
    envparts = ["SUBMIT_CONDORID=$(Cluster).$(Process)"]

    if isinstance(envvars, dict):
        for (key, val) in envvars.items():
            # Any literal double quote marks within the string must
            # be escaped by repeating the double quote mark
            val = val.replace('"', '""')

            # To insert a literal single quote mark, repeat the
            # single quote mark anywhere inside of a section surrounded
            # by single quote marks
            result = re.search("'", val)
            if result:
                val = "'%s'" % val.replace("'", "''")

            # Each environment entry has the form <name>=<value>
            # Use white space (space or tab characters) to separate
            #     environment entries.
            envparts.append(f"{key.upper()}={val}")
    elif isinstance(envvars, str):
        envparts.append(envvars)

    # put double quote marks around the entire argument string.
    return '"%s"' % ' '.join(envparts)




def write_condor_descfile(jobname, filename, jobattribs, userattribs=None):
    """Creates <name>.condor description file
       Assumes info contains valid condor key, value"""

    #print 'write_condor_descfile', jobname
    #print jobattribs

    # default some values if not given
    if 'log' not in jobattribs:
        jobattribs['log'] = f"{jobname}.log"

    if 'output' not in jobattribs:
        jobattribs['output'] = f"{jobname}.out"

    if 'error' not in jobattribs:
        jobattribs['error'] = f"{jobname}.err"

    if 'environment' in jobattribs:
        jobattribs['environment'] = create_condor_env(jobattribs['environment'])
    else:
        jobattribs['environment'] = create_condor_env(None)

    if 'universe' not in jobattribs:
        jobattribs['universe'] = 'vanilla'

    condorfh = open(filename, 'w')

    for key, val in sorted(jobattribs.items()):
        condorfh.write(f"{key} = {val}\n")

    if userattribs:
        for key, val in sorted(userattribs.items()):
            if isinstance(val, str) and val.lower() != 'true' and val.lower() != 'false':
                val = '"%s"' % val
            condorfh.write(f"+{key} = {val}\n")

    condorfh.write('queue\n')
    condorfh.close()



def parse_condor_user_log(logfilename):
    """parses a condor log into a dictionary"""

    #print "parse_condor_user_log:  logfilename=", logfilename
    log = open(logfilename)
    lines = log.read().split('\n...\n')
    log.close()

    logmdate = datetime.fromtimestamp((os.path.getmtime(logfilename)))
    logmonth = logmdate.month
    logyear = logmdate.year

    jobinfo = {}
    for line in lines:
        if re.search(r'\S', line):
            splitline = line.split('\n')
            result = re.match(r'(\d+)\s+\((\d+).\d+.\d+\)\s+(\d+\/\d+\s+\d+:\d+:\d+)\s+(.+)',
                              splitline[0])
            if result:
                code = result.group(1)
                jobnum = result.group(2)
                eventtime = result.group(3)
                eventdate = datetime.strptime(eventtime, '%m/%d %H:%M:%S')
                if eventdate.month == logmonth:
                    eventdate = eventdate.replace(year=logyear)
                else:
                    eventdate = eventdate.replace(year=logyear - 1)

                #desc = result.group(4)

                if code == '000':
                    jobinfo[jobnum] = {'jobid': jobnum,
                                       'clusterid': jobnum,
                                       'machine': '',
                                       'jobstat': 'UNSUB',
                                       'submittime': eventdate,
                                       'csubmittime': eventdate}
                    if len(splitline) > 1:
                        result = re.match(r'\s*DAG Node:\s+(\S+)\s*', splitline[1])
                        if result:
                            jobinfo[jobnum]['jobname'] = result.group(1)
                elif code == '001':
                    jobinfo[jobnum]['jobstat'] = 'RUN'
                    jobinfo[jobnum]['starttime'] = eventdate
                #elif code == '002':
                #    pass  # Error in executable
                #elif code == '003':
                #    pass  # Job was checkpointed
                #elif code == '004':
                #    pass  # Job evicted from machine
                elif code == '005':
                    jobinfo[jobnum]['jobstat'] = 'DONE'
                    jobinfo[jobnum]['endtime'] = eventdate
                    result = re.search(r'return value (\d+)', splitline[1])
                    if result:
                        jobinfo[jobnum]['retval'] = result.group(1)
                #elif code == '006':
                #    pass  # Image size of job updated
                #elif code == '007':
                #    pass  # Shadow threw an exception
                #elif code == '008':
                #    pass  # Generic Log Event
                elif code == '009':  # aborted
                    jobinfo[jobnum]['jobstat'] = 'FAIL'
                    jobinfo[jobnum]['endtime'] = eventdate
                    if len(splitline) > 1:
                        jobinfo[jobnum]['abortreason'] = splitline[1].strip()
                    else:
                        jobinfo[jobnum]['abortreason'] = None
                #elif code == '010':
                #    pass  # Job was suspended
                #elif code == '011':
                #    pass  # Job was unsuspended
                elif code == '012':
                    jobinfo[jobnum]['jobstat'] = 'HOLD'
                    #result = re.search(r'(\S+)', splitline[1])
                    #if result:
                    #    jobinfo[jobnum]['holdreason'] = result.group(1)
                    jobinfo[jobnum]['holdreason'] = splitline[1].strip()
                    if len(splitline) > 2:
                        result = re.search(r'Code (\d+) Subcode (\d+)', splitline[2])
                        if result:
                            jobinfo[jobnum]['holdcode'] = result.group(1)
                            jobinfo[jobnum]['holdsubcode'] = result.group(2)
                        else:
                            jobinfo[jobnum]['holdcode'] = None
                            jobinfo[jobnum]['holdsubcode'] = None
                elif code == '013':
                    jobinfo[jobnum]['jobstat'] = 'UNSUB'
                #elif code == '014':
                #    pass  # Parallel Node executed
                #elif code == '015':
                #    pass  # Parallel Node terminated
                elif code == '016':
             #016 (471.000.000) 04/11 11:48:08 POST Script terminated.
             #        (1) Normal termination (return value 100)
             #    DAG Node: fail
             #...
                    jobinfo[jobnum]['endtime'] = eventdate
                    result = re.search(r'return value (\d+)', splitline[1])
                    if result:
                        retval = result.group(1)
                        if retval == 100:
                            jobinfo[jobnum]['jobstat'] = 'FAIL'
                        else:
                            jobinfo[jobnum]['jobstat'] = 'DONE'
                elif code == '017':  #  Job submitted to Globus
                    #  Beware of out of order log entries
                    if ('starttime' not in jobinfo[jobnum] or
                            (jobinfo[jobnum]['starttime'] != eventdate)):
                        jobinfo[jobnum]['jobstat'] = 'PEND'
                    result = re.search(r'RM-Contact:\s+(\S+)', splitline[1])
                    if result:
                        jobinfo[jobnum]['gridresource'] = result.group(1)
                #elif code == '018':
                #    pass  # Globus Submit failed
                #elif code == '019':
                #    pass  # Globus Resource Up
                #elif code == '020':
                #    pass  # Globus Resource Down
                #elif code == '021':
                #    pass  # Remote Error
                elif code == '027':
                    jobinfo[jobnum]['gsubmittime'] = eventdate
                else:
                    jobinfo[jobnum]['jobstat'] = f"U{code}"
            else:
                print(f"warning unknown line: {line}")


    return jobinfo

def remote_condor_q(server, timeout, args_str=''):
    """Given condor_q args, calls condor_q -l [args] on remote machine and parses output into dictionary"""
    timeout = float(timeout)
    qjobs = {}
    job = {}
    condorid = -9999

    args_str = str(args_str)
    condorq_cmd = ['ssh', server, 'condor_q', '-l']
    condorq_cmd.extend(shlex.split(args_str))
    if miscutils.fwdebug_check(1, "PFWCONDOR_DEBUG"):
        miscutils.fwdebug_print(f"condorq_cmd  = {condorq_cmd}")

    process = None
    try:
        starttime = time.time()
        process = subprocess.Popen(condorq_cmd,
                                   shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   stdin=subprocess.PIPE,
                                   text=True)
        out = ""
        buf = os.read(process.stdout.fileno(), 5000).decode()

        if miscutils.fwdebug_check(6, "PFWCONDOR_DEBUG"):
            miscutils.fwdebug_print(buf)
        while process.poll() is None or buf:
            out += buf
            buf = os.read(process.stdout.fileno(), 5000).decode()
            if time.time() - starttime > timeout:
                process.kill()
                print(f"\nTimed out contacting {server}\n")
                return {}
            if miscutils.fwdebug_check(6, "PFWCONDOR_DEBUG"):
                miscutils.fwdebug_print(buf)
    except Exception as err:
        raise CondorException("Error: Could not run condor_q. Check PATH.\n" + str(err))

    if process.returncode != 0:
        if "All queues are empty" in out:
            out = ""
        else:
            print("Problem running condor_q - non-zero exit code")
            print("Cmd = ", ' '.join(condorq_cmd))

            print(process.communicate()[1])
            raise CondorException("Problem running condor_q - non-zero exit code")


    lines = out.split('\n')
    for line in lines:
        if re.match('--', line):  # skip condor_q line starting with --
            pass
        elif not re.search(r'\S', line):
            if job:   # blank lines separate jobs
                qjobs[condorid] = dict(job)
                job.clear()
                condorid = -9999
        else:
            # divide line into key/value pair
            result = re.search(r'(\S+)\s*=\s*(.+)$', line)
            key = result.group(1).lower()
            value = re.sub('"', '', result.group(2))

            # there are 2 args, make sure to appropriately store condor args
            if re.search('args', key) and re.match('-f', value):
                key = 'condorargs'
            job[key] = value
            if re.match('clusterid', key):
                condorid = value   # save clusterid as key for qjobs dict
    # don't forget to save the last job into big hash table
    if job:
        qjobs[condorid] = dict(job)
        job.clear()

    return qjobs



def condor_q(args_str=''):
    """ Given condor_q args, calls condor_q -l [args] and parses output into dictionary"""

    qjobs = {}
    job = {}
    condorid = -9999

    args_str = str(args_str)
    condorq_cmd = ['condor_q', '-l']
    condorq_cmd.extend(shlex.split(args_str))
    if miscutils.fwdebug_check(1, "PFWCONDOR_DEBUG"):
        miscutils.fwdebug_print(f"condorq_cmd  = {condorq_cmd}")

    process = None
    try:
        process = subprocess.Popen(condorq_cmd,
                                   shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   text=True)
        out = ""
        buf = os.read(process.stdout.fileno(), 5000).decode()
        if miscutils.fwdebug_check(6, "PFWCONDOR_DEBUG"):
            miscutils.fwdebug_print(buf)
        while process.poll() is None or buf:
            out += buf
            buf = os.read(process.stdout.fileno(), 5000).decode()
            if miscutils.fwdebug_check(6, "PFWCONDOR_DEBUG"):
                miscutils.fwdebug_print(buf)
    except Exception as err:
        raise CondorException("Error: Could not run condor_q. Check PATH.\n" + str(err))


    if process.returncode != 0:
        if "All queues are empty" in out:
            out = ""
        else:
            print("Problem running condor_q - non-zero exit code")
            print("Cmd = ", ' '.join(condorq_cmd))
            #print process.communicate()[0]
            print(process.communicate()[1])
            raise CondorException("Problem running condor_q - non-zero exit code")


    lines = out.split('\n')
    for line in lines:
        if re.match('--', line):  # skip condor_q line starting with --
            pass
        elif not re.search(r'\S', line):
            if job:   # blank lines separate jobs
                qjobs[condorid] = dict(job)
                job.clear()
                condorid = -9999
        else:
            # divide line into key/value pair
            result = re.search(r'(\S+)\s*=\s*(.+)$', line)
            if result is None:
                continue
            key = result.group(1).lower()
            value = re.sub('"', '', result.group(2))

            # there are 2 args, make sure to appropriately store condor args
            if re.search('args', key) and re.match('-f', value):
                key = 'condorargs'
            job[key] = value
            if re.match('clusterid', key):
                condorid = value   # save clusterid as key for qjobs dict

    # don't forget to save the last job into big hash table
    if job:
        qjobs[condorid] = dict(job)
        job.clear()

    return qjobs

def condorq_dag_many(servers, timeout, args_str=''):
    """ get condor jobs from remote machines"""
    qjobs = {}
    top_jobs = []  # top dagman jobs
    orphan_jobs = []  # jobs whose parents aren't in queue or non-dagman jobs

    for server in servers:
        tqjobs = remote_condor_q(server, timeout, args_str)

        for jobid, jobinfo in tqjobs.items():
            if not 'children' in jobinfo:
                jobinfo['children'] = []

            if 'dagmanjobid' in jobinfo: # should have parent
                if jobinfo['dagmanjobid'] in tqjobs:  # if have parent
                    if 'children' in tqjobs[jobinfo['dagmanjobid']]:
                        tqjobs[jobinfo['dagmanjobid']]['children'].append(server + '-' + str(jobid))
                    else:
                        tqjobs[jobinfo['dagmanjobid']]['children'] = [server+'-'+str(jobid)]
                else:
                    orphan_jobs.append(server+'-'+str(jobid))  # lost parent
            else:
                if 'dagman' in os.path.basename(tqjobs[jobid]['cmd']):
                    top_jobs.append(server+'-'+str(jobid))
                else:  # either saveruntime job or operator manually running job
                    orphan_jobs.append(server+'-'+str(jobid))
        for key in tqjobs.keys():
            tqjobs[server + '-' + str(key)] = tqjobs.pop(key)
        qjobs.update(tqjobs)

    return qjobs, top_jobs, orphan_jobs




def condorq_dag(args_str=''):
    """ Call condor_q and return in dag trees """

    qjobs = condor_q(args_str)

    top_jobs = []  # top dagman jobs
    orphan_jobs = []  # jobs whose parents aren't in queue or non-dagman jobs

    for jobid, jobinfo in qjobs.items():
        if not 'children' in jobinfo:
            jobinfo['children'] = []

        if 'dagmanjobid' in jobinfo: # should have parent
            if jobinfo['dagmanjobid'] in qjobs:  # if have parent
                if 'children' in qjobs[jobinfo['dagmanjobid']]:
                    qjobs[jobinfo['dagmanjobid']]['children'].append(jobid)
                else:
                    qjobs[jobinfo['dagmanjobid']]['children'] = [jobid]
            else:
                orphan_jobs.append(jobid)  # lost parent
        else:
            if 'dagman' in os.path.basename(qjobs[jobid]['cmd']):
                top_jobs.append(jobid)
            else:  # either saveruntime job or operator manually running job
                orphan_jobs.append(jobid)

    return qjobs, top_jobs, orphan_jobs



######################################################################
def add2dag(dagfile, cmdopts, attributes, initialdir, debugfh):
    """ Create the condor description file for a DAG with added attributes """
    print("add2dag: cwd =", os.getcwd())
    cmd = "condor_submit_dag -f -no_submit -notification never "

    assert isinstance(cmdopts, dict)
    assert isinstance(attributes, dict)

    if compare_condor_version('7.6.0') >= 0:
        cmd += " -autorescue 0 -no_recurse "
    elif compare_condor_version('7.1.0') >= 0:
        cmd += " -oldrescue 1 -autorescue 0 -no_recurse "
    else:
        raise Exception('Using condor that is too old')

    if 'dagman_max_pre' in cmdopts:
        cmd += f" -MaxPre {cmdopts['dagman_max_pre']}"

    if 'dagman_max_post' in cmdopts:
        cmd += f" -MaxPost {cmdopts['dagman_max_post']}"

    if 'dagman_max_jobs' in cmdopts:
        cmd += f" -maxjobs {cmdopts['dagman_max_jobs']}"

    if 'dagman_max_idle' in cmdopts:
        cmd += f" -maxidle {cmdopts['dagman_max_idle']}"

    # write additional lines to file and ask condor_submit_dag to include
    #    note: insert_sub_file works with empty file
    if compare_condor_version("7.1") > 0:
        addfile = dagfile + '.add.txt'
        with open(addfile, 'w') as addfh:
            if initialdir:
                addfh.write(f"initialdir={initialdir}\n")
            for key, val in attributes.items():
                if val.lower() != 'true' and val.lower() != 'false':
                    val = '"%s"' % val
                addfh.write(f"+{key}={val}\n")
        cmd += ' -insert_sub_file ' + addfile

    cmd += ' ' + dagfile

    debugfh.write(f"cmd> {cmd}\n")
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=debugfh,
                               stderr=debugfh,
                               text=True)
    process.wait()
    stat = process.returncode
    print("stat = ", stat)
    debugfh.write(f"condor_submit_dag exit code: {stat}\n")

    if stat == 0:
        dagfile += '.condor.sub'
        debugfh.write(os.getcwd() + '\n')
        condorfh = open(dagfile, 'r')
        condorstr = condorfh.read()
        condorfh.close()

        # Work around condor_submit_dag bug (6.7.20, 6.8.0-6.8.3, 6.9.1)
        # 'The OnExitRemove expression generated for DAGMan by
        # condor_submit_dag evaluated to UNDEFINED for some values
        # of ExitCode, causing condor_dagman to go on hold.'
        result = re.search(r'on_exit_remove\s*=\s*\(\s*ExitSignal\s*==\s*11\s*||\s*\(ExitCode\s*>=0\s*&&\s*ExitCode\s*<=\s*2\)\)', condorstr)
        if result:
            condorstr.replace(r'on_exit_remove\s+=[^\n]+\n',
                              'on_exit_remove = ( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))\n')

#        if attributes and len(attributes) > 0:
#            add2condor(condorstr, attributes, debugfh)

        condorfh = open(dagfile, 'w')
        condorfh.write(condorstr)
        condorfh.close()
    else:
        raise CondorException("condor_submit_dag failed")


######################################################################
def add2condor(condorstr, attributes, debugfh):
    """add some attributes to condor submit file"""

    debugfh.write('add2condor')
    debugfh.write('Pre-change\n')
    debugfh.write('============\n')
    debugfh.write(condorstr)
    debugfh.write('\n============\n')

    # add attributes to condor submit file
    print(attributes)
    info = ''
    for key, val in attributes.items():
        info += '+' + key + '="' + val + '"\n'
    info += '\nqueue\n'
    condorstr.replace('\nqueue', info)

    debugfh.write('Post-change\n')
    debugfh.write('============\n')
    debugfh.write(condorstr)
    debugfh.write('\n============\n')




def check_condor(minver):
    """ Check for Condor in path as well as daemons running """

    # checking condor executables are in path
    cmd = "condor_submit notthere.condor"
    try:
        process = subprocess.Popen(cmd.split(), shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   text=True)
        if miscutils.fwdebug_check(1, "PFWCONDOR_DEBUG"):
            miscutils.fwdebug_print(f"\t\tTrying {cmd}")
        process.wait()
    except OSError as exc:
        raise CondorException(f"Could not find condor_submit\nMake sure Condor binaries are in your path ({str(exc)})")

    if miscutils.fwdebug_check(1, "PFWCONDOR_DEBUG"):
        miscutils.fwdebug_print(f"\t\tFinished {cmd}")

    # checking running on this machine
    cmd = 'condor_q'
    try:
        process = subprocess.Popen(cmd.split(), shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   text=True)
        if miscutils.fwdebug_check(1, "PFWCONDOR_DEBUG"):
            miscutils.fwdebug_print(f"\t\tTrying {cmd}")

        # must read from pipe or process hangs when condor_q output is long
        out = ""
        buf = os.read(process.stdout.fileno(), 5000).decode()
        if miscutils.fwdebug_check(6, "PFWCONDOR_DEBUG"):
            miscutils.fwdebug_print(buf)
        while process.poll() is None or buf:
            out += buf
            buf = os.read(process.stdout.fileno(), 5000).decode()
            if miscutils.fwdebug_check(6, "PFWCONDOR_DEBUG"):
                miscutils.fwdebug_print(buf)
        if process.returncode:
            raise CondorException("Problems running condor_q.   Condor might not be running on this machine.   " +
                                  "Contact your condor administrator.")
    except OSError as exc:
        raise CondorException("Could not find condor_q\n" +
                              f"Make sure Condor binaries are in your path ({str(exc)})")

    if miscutils.fwdebug_check(1, "PFWCONDOR_DEBUG"):
        miscutils.fwdebug_print(f"\t\tFinished {cmd}")

    # check have new enough version of condor
    if compare_condor_version(minver) < 0:
        raise CondorException("Condor version must be at least " + minver)



def get_grid_proxy_timeleft():
    """ Check timeleft on grid proxy """

    cmd = "grid-proxy-info -timeleft"
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               text=True)
    process.wait()

    out = process.communicate()[0]
    timeleft = -1
    if process.returncode == 0:
        result = re.search('ERROR', out)
        if not result:
            timeleft = int(out)
    return timeleft


def get_job_status_str(jobnum, qjobs):
    """ Return a status string for a particular condor job """
    statusstr = "UNK"

    # Condor Job Status:
    #    1 = Idle, 2 = Running, 3 = Removed, 4 = Completed, and 5 = Held
    condorstatus = {'1':"PEND", '2':"RUN", '3':"DEL", '4':"DONE", '5':"HOLD"}
    # Grid job status:
    #    1 = Pend, 2 = Running, 32 = Unsub
    gridstatus = {'1':"PEND", '2':"RUN", '32':"UNSUB"}

    statusnum = 0
    if jobnum in qjobs and 'jobstatus' in qjobs[jobnum]:
        statusnum = qjobs[jobnum]['jobstatus']
        if statusnum in condorstatus:
            statusstr = condorstatus[statusnum]

        # if grid job, use remote status
        if statusnum == 1:
            if 'jobuniverse' in qjobs[jobnum] and \
                qjobs[jobnum]['jobuniverse'] == 9 and \
                'globusstatus' in qjobs[jobnum]:

                if qjobs[jobnum]['globusstatus'] in gridstatus:
                    statusstr = gridstatus[qjobs[jobnum]['globusstatus']]

    return statusstr

def condor_rm(args_str=''):
    """ Given condor_rm args, calls condor_rm [args]"""

    args_str = str(args_str)    # make sure string

    condorrm_cmd = ['condor_rm']
    condorrm_cmd.extend(args_str.split())

    try:
        process = subprocess.Popen(condorrm_cmd,
                                   shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   text=True)
        out = ""
        buf = os.read(process.stdout.fileno(), 5000).decode()
        while process.poll() is None or buf:
            out += buf
            buf = os.read(process.stdout.fileno(), 5000).decode()

        if process.returncode != 0:
            print("Cmd = ", condorrm_cmd)
            raise CondorException("Problem running condor_rm - non-zero exit code" + process.communicate()[0])
    except Exception as err:
        raise CondorException("Error: Could not run condor_rm. Check PATH.\n" + str(err))


#######################################################################
def status_target_jobs(job, qjobs):
    """ Convert condor/grid status """

    numtjobs = 'UNK'
    if f"{pfwdefs.ATTRIB_PREFIX}numjobs" in qjobs[qjobs[job]['children'][0]]:
        numtjobs = qjobs[qjobs[job]['children'][0]][f"{pfwdefs.ATTRIB_PREFIX}numjobs"]
    else:
        print(f"Could not find {pfwdefs.ATTRIB_PREFIX}numjobs in qjobs for job {job}")

    chstat = {'PEND': 0, 'UNSUB': 0, 'RUN': 0, 'HOLD': 0}
    for childjob in qjobs[job]['children']:
        jobstat = get_job_status_str(childjob, qjobs)
        if jobstat in chstat:  # ignore other status, e.g. DONE
            chstat[jobstat] += 1
    status = f"({chstat['HOLD']}/{chstat['PEND'] + chstat['UNSUB']}/{chstat['RUN']}/{numtjobs})"
    return status


#######################################################################
def get_attempt_info(topjob, qjobs):
    """ Massage condor_q dag information into attempt information """

    info = {}
    if f"{pfwdefs.ATTRIB_PREFIX}operator" not in qjobs[topjob]:
        if 'owner' in qjobs[topjob]:
            qjobs[topjob][f"{pfwdefs.ATTRIB_PREFIX}operator"] = qjobs[topjob]['owner'].replace('"', '')
        else:
            qjobs[topjob][f"{pfwdefs.ATTRIB_PREFIX}operator"] = "UNK"

    # find innermost dag job
    jobid = topjob
    while len(qjobs[jobid]['children']) == 1 and \
          (f"{pfwdefs.ATTRIB_PREFIX}block" not in qjobs[jobid] or
           'pipe' not in qjobs[jobid][f"{pfwdefs.ATTRIB_PREFIX}block"]):
        jobid = qjobs[jobid]['children'][0]

    # grab DESDM from job attributes
    for key in ['project', 'pipeline', 'run', 'runsite', 'block', 'subblock', 'operator', 'campaign']:
        info[key] = ""
        if pfwdefs.ATTRIB_PREFIX + key in qjobs[jobid]:
            info[key] = qjobs[jobid][pfwdefs.ATTRIB_PREFIX + key]
    if 'globaljobid' in qjobs[jobid]:
        info['submitsite'] = qjobs[jobid]['globaljobid'].split('.')[0]
    else:
        info['submitsite'] = ""

    info['status'] = get_job_status_str(jobid, qjobs)

    # If pipeline mngr, count number of pending, running, etc target jobs
    if qjobs[jobid]['children']:
        info['status'] = status_target_jobs(jobid, qjobs)
        info['subblock'] = "pipelines"
        info['block'] = qjobs[qjobs[jobid]['children'][0]][f"{pfwdefs.ATTRIB_PREFIX}block"]

    return info
