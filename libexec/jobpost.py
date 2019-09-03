#!/usr/bin/env python
# $Id: jobpost.py 48056 2019-01-08 19:57:20Z friedel $
# $Rev:: 48056                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-01-08 13:57:20 #$:  # Date of last commit.

""" Steps executed submit-side after job success or failure """

import sys
import re
import os
import tempfile
import traceback
from datetime import datetime

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwcondor as pfwcondor
import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
from processingfw.pfwlog import log_pfw_event
import qcframework.Messaging as Messaging

def parse_job_output(config, jobnum, dbh=None, retval=None):
    """ Search stdout/stderr for timing stats as well as eups setup
        or DB connection error messages and insert them into db """
    jobbase = config.get_filename('job',
                                  {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:jobnum,
                                                         'flabel': 'runjob',
                                                         'fsuffix':''}})

    tjobinfo = {}
    tjobinfo_task = {}
    for jobfile in ['%sout'%jobbase, '%serr'%jobbase]:
        if os.path.exists(jobfile):
            with open(jobfile, 'r') as jobfh:
                for no, line in enumerate(jobfh):
                    line = line.strip()
                    if line.startswith('PFW:'):
                        parts = line.split()
                        if parts[1] == 'batchid':
                            if parts[2] == '=':   # older pfwrunjob.py
                                tjobinfo['target_job_id'] = parts[3]
                            else:
                                tjobinfo['target_job_id'] = parts[2]
                        elif parts[1] == 'condorid':
                            tjobinfo['condor_job_id'] = parts[2]
                        elif parts[1] == 'job_shell_script':
                            print "parts[2]", parts[2]
                            print "parts[3]", parts[3]
                            if parts[2] == 'exechost:':
                                #tjobinfo['target_exec_host'] = parts[3]
                                tjobinfo_task['exec_host'] = parts[3]
                            elif parts[2] == 'starttime:':
                                tjobinfo_task['start_time'] = datetime.fromtimestamp(float(parts[3]))
                            elif parts[2] == 'endtime:':
                                #tjobinfo['target_end_time'] = datetime.fromtimestamp(float(parts[3]))
                                tjobinfo_task['end_time'] = datetime.fromtimestamp(float(parts[3]))
                            elif parts[2] == 'exit_status:':
                                tjobinfo_task['status'] = parts[3]
                    # skip ORA messages as they are caught by the QCF earlier, and not all are fatal
                    #elif 'ORA-' in line:
                        #print "Found:", line
                        #if not 'DBD' in line:
                        #    print "Setting retval to failure"
                        #    tjobinfo_task['status'] = pfwdefs.PF_EXIT_FAILURE
                        #else:
                        #    print " Ignoring QCF perl error message."
                    #    if dbh:
                    #        Messaging.pfw_message(dbh, config['pfw_attempt_id'],
                    #                              config['task_id']['job'][jobnum],
                    #                              line, pfwdefs.PFWDB_MSG_ERROR, jobfile, no)
                    elif 'No such file or directory: ' in line and \
                          config.getfull('target_des_services') in line:
                        #print "Found:", line
                        if dbh:
                            Messaging.pfw_message(dbh, config['pfw_attempt_id'],
                                                  config['task_id']['job'][jobnum],
                                                  line, pfwdefs.PFWDB_MSG_ERROR, jobfile, no)
                    elif 'Error: eups setup' in line:
                        #print "Found:", line
                        print "Setting retval to failure"
                        tjobinfo_task['status'] = pfwdefs.PF_EXIT_EUPS_FAILURE
                        if dbh:
                            Messaging.pfw_message(dbh, config['pfw_attempt_id'],
                                                  config['task_id']['job'][jobnum],
                                                  line, pfwdefs.PFWDB_MSG_ERROR, jobfile, no)
                    elif 'Exiting with status' in line:
                        lmatch = re.search(r'Exiting with status (\d+)', line)
                        if lmatch:
                            if int(lmatch.group(1)) != 0 and retval == 0:
                                #print "Found:", line
                                msg = "Info:  Job exit status was %s, but retval was %s." % \
                                      (lmatch.group(1), retval)
                                msg += "Setting retval to failure."
                                #print msg
                                tjobinfo['status'] = pfwdefs.PF_EXIT_FAILURE
                                if dbh:
                                    Messaging.pfw_message(dbh, config['pfw_attempt_id'],
                                                          config['task_id']['job'][jobnum],
                                                          msg, pfwdefs.PFWDB_MSG_ERROR, jobfile, no)
                    elif 'Could not connect to database'in line:
                        #print "Found:", line
                        if dbh:
                            Messaging.pfw_message(dbh, config['pfw_attempt_id'],
                                                  config['task_id']['job'][jobnum],
                                                  line, pfwdefs.PFWDB_MSG_INFO, jobfile, no)

    return tjobinfo, tjobinfo_task



def jobpost(argv=None):
    """ Performs steps needed after a pipeline job """

    condor2db = {'jobid': 'condor_job_id',
                 'csubmittime': 'condor_submit_time',
                 'gsubmittime': 'target_submit_time',
                 'starttime': 'condor_start_time',
                 'endtime': 'condor_end_time'}

    if argv is None:
        argv = sys.argv

    debugfh = tempfile.NamedTemporaryFile(prefix='jobpost_', dir='.', delete=False)
    tmpfn = debugfh.name
    sys.stdout = debugfh
    sys.stderr = debugfh

    miscutils.fwdebug_print("temp log name = %s" % tmpfn)
    print 'cmd>', ' '.join(argv)  # print command line for debugging

    if len(argv) < 7:
        # open file to catch error messages about command line
        print 'Usage: jobpost.py configfile block jobnum inputtar outputtar retval'
        debugfh.close()
        return pfwdefs.PF_EXIT_FAILURE

    configfile = argv[1]
    blockname = argv[2]
    jobnum = argv[3]
    inputtar = argv[4]
    outputtar = argv[5]
    retval = pfwdefs.PF_EXIT_FAILURE
    if len(argv) == 7:
        retval = int(sys.argv[6])

    if miscutils.fwdebug_check(3, 'PFWPOST_DEBUG'):
        miscutils.fwdebug_print("configfile = %s" % configfile)
        miscutils.fwdebug_print("block = %s" % blockname)
        miscutils.fwdebug_print("jobnum = %s" % jobnum)
        miscutils.fwdebug_print("inputtar = %s" % inputtar)
        miscutils.fwdebug_print("outputtar = %s" % outputtar)
        miscutils.fwdebug_print("retval = %s" % retval)


    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    if miscutils.fwdebug_check(3, 'PFWPOST_DEBUG'):
        miscutils.fwdebug_print("done reading config file")


    # now that have more information, rename output file
    if miscutils.fwdebug_check(3, 'PFWPOST_DEBUG'):
        miscutils.fwdebug_print("before get_filename")
    blockname = config.getfull('blockname')
    blkdir = config.getfull('block_dir')
    tjpad = pfwutils.pad_jobnum(jobnum)

    os.chdir("%s/%s" % (blkdir, tjpad))
    new_log_name = config.get_filename('job', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM: jobnum,
                                                                     'flabel': 'jobpost',
                                                                     'fsuffix':'out'}})
    new_log_name = "%s" % (new_log_name)
    miscutils.fwdebug_print("new_log_name = %s" % new_log_name)

    debugfh.close()
    os.chmod(tmpfn, 0666)
    os.rename(tmpfn, new_log_name)
    dbh = None
    if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
        if config.dbh is None:
            dbh = pfwdb.PFWDB(config.getfull('submit_des_services'),
                              config.getfull('submit_des_db_section'))
        else:
            dbh = config.dbh

    if 'use_qcf' in config and config['use_qcf']:
        debugfh = Messaging.Messaging(new_log_name, 'jobpre.py', config['pfw_attempt_id'], dbh=dbh, mode='a+', usedb=dbh is not None)

    else:
        debugfh = open(new_log_name, 'a+')


    sys.stdout = debugfh
    sys.stderr = debugfh


    if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
        # get job information from the job stdout if exists
        (tjobinfo, tjobinfo_task) = parse_job_output(config, jobnum, dbh, retval)

        if dbh and len(tjobinfo) > 0:
            print "tjobinfo: ", tjobinfo
            dbh.update_tjob_info(config['task_id']['job'][jobnum], tjobinfo)

        # get job information from the condor job log
        logfilename = 'runjob.log'
        if os.path.exists(logfilename) and os.path.getsize(logfilename) > 0:  # if made it to submitting/running jobs
            try:
                # update job info in DB from condor log
                print "Updating job info in DB from condor log"
                condorjobinfo = pfwcondor.parse_condor_user_log(logfilename)
                if len(condorjobinfo.keys()) > 1:
                    print "More than single job in job log"
                j = condorjobinfo.keys()[0]
                cjobinfo = condorjobinfo[j]
                djobinfo = {}
                for ckey, dkey in condor2db.items():
                    if ckey in cjobinfo:
                        djobinfo[dkey] = cjobinfo[ckey]
                print djobinfo
                dbh.update_job_info(config, cjobinfo['jobname'], djobinfo)

                if 'holdreason' in cjobinfo and cjobinfo['holdreason'] is not None:
                    msg = "Condor HoldReason: %s" % cjobinfo['holdreason']
                    print msg
                    if dbh:
                        Messaging.pfw_message(dbh, config['pfw_attempt_id'],
                                              config['task_id']['job'][jobnum],
                                              msg, pfwdefs.PFWDB_MSG_WARN, logfilename, 0)

                if 'abortreason' in cjobinfo and cjobinfo['abortreason'] is not None:
                    tjobinfo_task['start_time'] = cjobinfo['starttime']
                    tjobinfo_task['end_time'] = cjobinfo['endtime']
                    if 'condor_rm' in cjobinfo['abortreason']:
                        tjobinfo_task['status'] = pfwdefs.PF_EXIT_OPDELETE
                    else:
                        tjobinfo_task['status'] = pfwdefs.PF_EXIT_CONDOR
                else:
                    pass
            except Exception:
                (extype, exvalue, trback) = sys.exc_info()
                traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
        else:
            print "Warning:  no job condor log file"

        if dbh:
            # update job task
            if 'status' not in tjobinfo_task:
                tjobinfo_task['status'] = pfwdefs.PF_EXIT_CONDOR
            if 'end_time' not in tjobinfo_task:
                tjobinfo_task['end_time'] = datetime.now()
            wherevals = {'id': config['task_id']['job'][jobnum]}
            dbh.basic_update_row('task', tjobinfo_task, wherevals)
            dbh.commit()


    log_pfw_event(config, blockname, jobnum, 'j', ['posttask', retval])


    # input wcl should already exist in untar form
    if os.path.exists(inputtar):
        print "found inputtar: %s" % inputtar
        os.unlink(inputtar)
    else:
        print "Could not find inputtar: %s" % inputtar

    # untar output wcl tar and delete tar
    if os.path.exists(outputtar):
        print "Size of output wcl tar:", os.path.getsize(outputtar)
        if os.path.getsize(outputtar) > 0:
            print "found outputtar: %s" % outputtar
            pfwutils.untar_dir(outputtar, '..')
            os.unlink(outputtar)
        else:
            msg = "Warn: outputwcl tarball (%s) is 0 bytes." % outputtar
            print msg
            if dbh:
                Messaging.pfw_message(dbh, config['pfw_attempt_id'],
                                      config['task_id']['job'][jobnum],
                                      msg, pfwdefs.PFWDB_MSG_WARN, '')
    else:
        msg = "Warn: outputwcl tarball (%s) does not exist." % outputtar
        print msg
        if dbh:
            Messaging.pfw_message(dbh, config['pfw_attempt_id'],
                                  config['task_id']['job'][jobnum],
                                  msg, pfwdefs.PFWDB_MSG_WARN, '')

    if retval != pfwdefs.PF_EXIT_SUCCESS:
        miscutils.fwdebug_print("Setting failure retval")
        retval = pfwdefs.PF_EXIT_FAILURE

    miscutils.fwdebug_print("Returning retval = %s" % retval)
    miscutils.fwdebug_print("jobpost done")
    debugfh.close()
    return int(retval)


if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = jobpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    miscutils.fwdebug_print("Exiting with = %s" % exitcode)
    sys.exit(exitcode)
