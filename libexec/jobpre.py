#!/usr/bin/env python
# $Id: jobpre.py 48056 2019-01-08 19:57:20Z friedel $
# $Rev:: 48056                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-01-08 13:57:20 #$:  # Date of last commit.

""" Steps executed submit-side prior to target job being submitted """

import sys
import os
import tempfile
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils
import despymisc.miscutils as miscutils
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb
from qcframework import Messaging

def jobpre(argv=None):
    """ Program entry point """
    if argv is None:
        argv = sys.argv

    debugfh = tempfile.NamedTemporaryFile(prefix='jobpre_', dir='.', delete=False)
    tmpfn = debugfh.name
    sys.stdout = debugfh
    sys.stderr = debugfh

    print ' '.join(sys.argv) # command line for debugging
    print os.getcwd()

    if len(argv) < 3:
        print 'Usage: jobpre configfile jobnum'
        debugfh.close()
        return pfwdefs.PF_EXIT_FAILURE

    configfile = sys.argv[1]
    jobnum = sys.argv[2]    # could also be uberctrl

    # read wcl file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    blockname = config.getfull('blockname')
    blkdir = config.get('block_dir')
    tjpad = pfwutils.pad_jobnum(jobnum)

    # now that have more information, can rename output file
    miscutils.fwdebug_print("getting new_log_name")
    new_log_name = config.get_filename('job', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:jobnum,
                                                                     'flabel': 'jobpre',
                                                                     'fsuffix':'out'}})
    new_log_name = "%s/%s/%s" % (blkdir, tjpad, new_log_name)
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
        ctstr = dbh.get_current_timestamp_str()
        dbh.update_job_info(config, tjpad, {'condor_submit_time': ctstr,
                                            'target_submit_time': ctstr})

    log_pfw_event(config, blockname, tjpad, 'j', ['pretask'])

    miscutils.fwdebug_print("jobpre done")
    debugfh.close()
    return pfwdefs.PF_EXIT_SUCCESS

if __name__ == "__main__":
    sys.exit(jobpre(sys.argv))
