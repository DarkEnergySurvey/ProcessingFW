#!/usr/bin/env python
# $Id: blockpre.py 41004 2015-12-11 15:49:41Z mgower $
# $Rev:: 41004                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-12-11 09:49:41 #$:  # Date of last commit.

""" Steps needed to be performed prior to the block running """
import sys
import os
import socket

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwcondor as pfwcondor


def write_block_condor(config):
    """ Write the condor file for running a block task """
    blockname = config.getfull('blockname')
    blkdir = config.getfull('block_dir')
    filename = 'blocktask.condor'
    submit_machine = socket.gethostname()

    blockbase = config.get_filename('block', {pfwdefs.PF_CURRVALS: {'flabel': '$(jobname)',
                                                                    'fsuffix':''}})
    jstdout = "%s/%sout" % (blkdir, blockbase)   # base ends with .
    jstderr = "%s/%serr" % (blkdir, blockbase)

    premove = '((JobStatus == 5) && (HoldReason =!= "via condor_hold (by user %s)"))' % \
               config.getfull('operator')
    # put jobs that have run once and are back in idle on hold
    phold = '((NumJobStarts > 0) && (JobStatus == 1))'

    jobattribs = {'executable': '$(exec)',
                  'arguments': '$(args)',
                  'initialdir': blkdir,
                  #'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
                  #'transfer_executable': 'True',
                  'notification': 'Never',
                  'output': jstdout,
                  'error': jstderr,
                  'log': 'blocktask.log',
                  'getenv': 'true',
                  'periodic_remove': premove,
                  'periodic_hold': phold
                 }

    userattribs = config.get_condor_attributes(blockname, '$(jobname)')
    reqs = ['NumJobStarts == 0']   # don't want to rerun any job
    jobattribs['universe'] = 'vanilla'
    reqs.append('(machine == "%s")' % submit_machine)
    jobattribs['requirements'] = ' && '.join(reqs)

    pfwcondor.write_condor_descfile('blocktask', filename, jobattribs, userattribs)

    miscutils.fwdebug_print("END\n\n")

    return filename


def blockpre(argv=None):
    """ Program entry point """
    if argv is None:
        argv = sys.argv

    default_log = 'blockpre.out'

    debugfh = open(default_log, 'w', 0)
    sys.stdout = debugfh
    sys.stderr = debugfh

    print ' '.join(sys.argv) # command line for debugging

    if len(argv) < 2 or len(argv) > 3:
        print 'Usage: blockpre configfile'
        debugfh.close()
        return pfwdefs.PF_EXIT_FAILURE

    configfile = sys.argv[1]

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})

    # make sure values which depend upon block are set correctly
    config.set_block_info()
    miscutils.fwdebug_print("blknum = %s" % config[pfwdefs.PF_BLKNUM])

    with open(configfile, 'w') as cfgfh:
        config.write(cfgfh)

    blockname = config.getfull('blockname')
    miscutils.fwdebug_print("blockname = %s" % blockname)

    blkdir = config.getfull('block_dir')

    # now that have more information, can rename output file
    miscutils.fwdebug_print("getting new_log_name")
    new_log_name = config.get_filename('block', {pfwdefs.PF_CURRVALS: {'flabel': 'blockpre',
                                                                       'fsuffix':'out'}})
    new_log_name = "%s/%s" % (blkdir, new_log_name)
    miscutils.fwdebug_print("new_log_name = %s" % new_log_name)

    debugfh.close()
    os.chmod(default_log, 0666)
    os.rename(default_log, new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh

    os.chdir(blkdir)

    write_block_condor(config)

    log_pfw_event(config, blockname, 'blockpre', 'j', ['pretask'])

    miscutils.fwdebug_print("blockpre done")
    debugfh.close()

    return pfwdefs.PF_EXIT_SUCCESS

if __name__ == "__main__":
    sys.exit(blockpre(sys.argv))
