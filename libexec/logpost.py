#!/usr/bin/env python
# $Id: logpost.py 48056 2019-01-08 19:57:20Z friedel $
# $Rev:: 48056                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-01-08 13:57:20 #$:  # Date of last commit.

""" Bookkeeping steps executed submit-side after certain submit-side tasks """

import sys
import os

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils
import processingfw.pfwconfig as pfwconfig
from processingfw.pfwlog import log_pfw_event
from qcframework import Messaging


def logpost(argv=None):
    """ Program entry point """
    if argv is None:
        argv = sys.argv

    # open file to catch error messages about command line
    debugfh = open('logpost.out', 'w', 0)
    sys.stdout = debugfh
    sys.stderr = debugfh

    print ' '.join(argv)  # print command line for debugging

    if len(argv) < 5:
        print 'Usage: logpost configfile block subblocktype subblock retval'
        debugfh.close()
        return pfwdefs.PF_EXIT_FAILURE

    configfile = argv[1]
    blockname = argv[2]
    subblocktype = argv[3]
    subblock = argv[4]
    retval = pfwdefs.PF_EXIT_FAILURE
    if len(argv) == 6:
        retval = int(sys.argv[5])

    if miscutils.fwdebug_check(3, 'PFWPOST_DEBUG'):
        miscutils.fwdebug_print("configfile = %s" % configfile)
        miscutils.fwdebug_print("block = %s" % blockname)
        miscutils.fwdebug_print("subblock = %s" % subblock)
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
    new_log_name = config.get_filename('block',
                                       {pfwdefs.PF_CURRVALS: {'flabel': '${subblock}_logpost',
                                                              'subblock': subblock,
                                                              'fsuffix':'out'}})
    new_log_name = "%s/%s" % (blkdir, new_log_name)
    miscutils.fwdebug_print("new_log_name = %s" % new_log_name)

    debugfh.close()
    os.chmod('logpost.out', 0666)
    os.rename('logpost.out', new_log_name)
    if 'use_qcf' in config and config['use_qcf']:
        if config.dbh is None:
            if 'submit_des_services' in config:
                os.environ['DES_SERVICES'] = config.getfull('submit_des_services')
            os.environ['DES_DB_SECTION'] = config.getfull('submit_des_db_section')

            debugfh = Messaging.Messaging(new_log_name, 'logpost.py', config['pfw_attempt_id'], mode='a+')
        else:
            debugfh = Messaging.Messaging(new_log_name, 'logpost.py', config['pfw_attempt_id'], dbh=config.dbh, mode='a+')

    else:
        debugfh = open(new_log_name, 'a+')

    sys.stdout = debugfh
    sys.stderr = debugfh


    log_pfw_event(config, blockname, subblock, subblocktype, ['posttask', retval])

    # In order to continue, make pipelines dagman jobs exit with success status
    #if 'pipelinesmngr' not in subblock:
    #    retval = pfwdefs.PF_EXIT_SUCCESS

#    # If error at non-manager level, send failure email
#    if retval != pfwdefs.PF_EXIT_SUCCESS and \
#        'mngr' not in subblock:
#        send_subblock_email(config, blockname, subblock, retval)

    if subblock != 'begblock' and retval != pfwdefs.PF_EXIT_SUCCESS:
        miscutils.fwdebug_print("Setting failure retval")
        retval = pfwdefs.PF_EXIT_FAILURE

    miscutils.fwdebug_print("returning retval = %s" % retval)
    miscutils.fwdebug_print("logpost done")
    debugfh.close()
    return int(retval)

if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = logpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    miscutils.fwdebug_print("Exiting with = %s" % exitcode)
    sys.exit(exitcode)
