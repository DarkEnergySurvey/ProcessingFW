#!/usr/bin/env python3
# $Id: logpre.py 48056 2019-01-08 19:57:20Z friedel $
# $Rev:: 48056                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-01-08 13:57:20 #$:  # Date of last commit.

""" Bookkeeping steps executed submit-side prior to certain submit-side tasks """

import sys
import os
import despymisc.miscutils as miscutils
import processingfw.pfwdefs as pfwdefs
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwconfig as pfwconfig
from qcframework import Messaging

def logpre(argv=None):
    """ Program entry point """
    if argv is None:
        argv = sys.argv

    default_log = 'logpre.out'
    debugfh = open(default_log, 'w')
    outorig = sys.stdout
    errorig = sys.stderr
    sys.stdout = debugfh
    sys.stderr = debugfh

    print(' '.join(sys.argv)) # command line for debugging

    if len(argv) < 5:
        print("Usage: logpre configfile block subblocktype subblock")
        debugfh.close()
        return pfwdefs.PF_EXIT_FAILURE

    configfile = sys.argv[1]
    blockname = sys.argv[2]    # could also be uberctrl
    subblocktype = sys.argv[3]
    subblock = sys.argv[4]

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})

    # now that have more information, can rename output file
    miscutils.fwdebug_print("getting new_log_name")
    blockname = config.getfull('blockname')
    blkdir = config.getfull('block_dir')
    new_log_name = config.get_filename('block',
                                       {pfwdefs.PF_CURRVALS: {'subblock': subblock,
                                                              'flabel': '${subblock}_logpre',
                                                              'fsuffix':'out'}})
    new_log_name = f"{blkdir}/{new_log_name}"
    miscutils.fwdebug_print(f"new_log_name = {new_log_name}")
    debugfh.close()

    os.chmod(default_log, 0o666)
    os.rename(default_log, new_log_name)

    #debugfh.close()
    sys.stdout = outorig
    sys.stderr = errorig

    if 'use_qcf' in config and config['use_qcf']:
        if config.dbh is None:
            if 'submit_des_services' in config:
                os.environ['DES_SERVICES'] = config.getfull('submit_des_services')
            os.environ['DES_DB_SECTION'] = config.getfull('submit_des_db_section')

            debugfh = Messaging.Messaging(new_log_name, 'logpre.py', config['pfw_attempt_id'], mode='a+')
        else:
            debugfh = Messaging.Messaging(new_log_name, 'logpre.py', config['pfw_attempt_id'], dbh=config.dbh, mode='a+')

    else:
        debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh


    log_pfw_event(config, blockname, subblock, subblocktype, ['pretask'])

    print("logpre done")
    debugfh.close()
    sys.stdout = outorig
    sys.stderr = errorig
    return pfwdefs.PF_EXIT_SUCCESS

if __name__ == "__main__":
    sys.exit(logpre(sys.argv))
