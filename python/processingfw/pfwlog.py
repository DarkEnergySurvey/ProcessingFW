# $Id: pfwlog.py 41004 2015-12-11 15:49:41Z mgower $
# $Rev:: 41004                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-12-11 09:49:41 #$:  # Date of last commit.

# pylint: disable=print-statement

""" Functions that handle a processing framework execution event """

import os
import time

#######################################################################
def get_timestamp():
    """Create timestamp in a particular format"""
    tstamp = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    return tstamp


#######################################################################
def log_pfw_event(config, block=None, subblock=None,
                  subblocktype=None, info=None):
    """Write info for a PFW event to a log file"""
    if block:
        block = block.replace('"', '')
    else:
        block = ''

    if subblock:
        subblock = subblock.replace('"', '')
    else:
        subblock = ''

    if subblocktype:
        subblocktype = subblocktype.replace('"', '')
    else:
        subblocktype = ''

    runsite = config.getfull('run_site')
    run = config.getfull('submit_run')
    logdir = config.getfull('uberctrl_dir')

    dagid = os.getenv('CONDOR_ID')
    if not dagid:
        dagid = 0

    deslogfh = open(f"{logdir}/{run}.deslog", "ab", 0)
    deslogfh.write(f"{get_timestamp()} {dagid} {run} {runsite} {block} {subblocktype} {subblock}".encode('utf-8'))
    if isinstance(info, list):
        for col in info:
            deslogfh.write(f",{col}".encode('utf-8'))
    else:
        deslogfh.write(f",{info}".encode('utf-8'))

    deslogfh.write("\n".encode('utf-8'))
    deslogfh.close()
