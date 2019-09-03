#!/usr/bin/env python
# $Id: print_job.py 44479 2016-10-21 16:02:19Z mgower $
# $Rev:: 44479                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-10-21 11:02:19 #$:  # Date of last commit.

import argparse
import re
import sys

import ConfigParser
from despymisc import miscutils
from processingfw import pfwdb
from processingfw import pfwutils

######################################################################
def parse_attempt_str(attstr):
    """ Parse attempt string for reqnum, unitname, and attnum """
    amatch = re.search(r"(\S+)_r([^p]+)p([^_]+)", attstr)
    if amatch is None:
        print "Error:  cannot parse attempt string", attstr
        sys.exit(1)

    unitname = amatch.group(1)
    reqnum = amatch.group(2)
    attnum = amatch.group(3)

    return reqnum, unitname, attnum

######################################################################
def parse_args(argv):
    """ Parse command line arguments """
    parser = argparse.ArgumentParser(description='Print task information for a processing attempt')
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', '-s', action='store',
                        help='Must be specified if DES_DB_SECTION is not set in environment')
    parser.add_argument('attempt_str', nargs='?', action='store')
    parser.add_argument('-r', '--reqnum', action='store')
    parser.add_argument('-u', '--unitname', action='store')
    parser.add_argument('-a', '--attnum', action='store')
    parser.add_argument('--verbose', action='store_true')

    args = vars(parser.parse_args(argv))   # convert to dict

    if args['attempt_str'] is None:
        if args['reqnum'] is None:
            print "Error:  Must specify attempt_str or r,u,a"
            sys.exit(1)
        else:
            args['attempt_str'] = '%s_r%sp%02d' % (args['unitname'], args['reqnum'], int(args['attnum']))

    args['runs'] = miscutils.fwsplit(args['attempt_str'], ',')
    return args


######################################################################
def print_single_wrap(wrapnum, numwraps, expnumwrap, jdict, jwdict, wdict, indent='\t'):

    state = "UNK"
    modname = "UNK"
    wrapkeys = ""

    jstate = "UNK"
    jstatus = "UNK"
    if jdict is None or jdict['start_time'] is None:
        jstate = "PRE"
        jstatus = None
    else:
        jstatus = jdict['status']
        if jdict['end_time'] is None: 
            if numwraps == expnumwrap and jwdict['end_time'] is not None:
                jstate = "POST"
            else:
                jstate = "EXEC"
        elif jstatus == 0: 
            jstate = "DONE"
        else:
            jstate = "FAIL"

    if jwdict is None:
        if jdict['end_time'] is None:
            state = "UNK"
            modname = "UNK"
            wrapkeys = ""
            status = "UNK - maybe first wrapper hasn't started yet"
        else: 
            state = "UNK"
            modname = "UNK"
            wrapkeys = ""
            status = "UNK"
    elif jwdict['end_time'] is not None:
        status = jwdict['status']
        if status == 0:
            state = "DONE"
        else:
            state = "FAIL"
        modname = wdict['modname']
        wrapkeys = wdict['wrapkeys']
    elif wdict is None:
        state = "PRE"
        if jwdict['status'] is None:
            status = jdict['status']
        else:
            status = jwdict['status']
    elif wdict['end_time'] is not None and jwdict['end_time'] is None:
        state = "POST"  # after wrapper, but still in job_wrapper
        status = wdict['status']
        modname = wdict['modname']
        wrapkeys = wdict['wrapkeys']
    elif wdict['end_time'] is None and wdict['start_time'] is not None: 
        state = "EXEC"
        status = ""
        modname = wdict['modname']
        wrapkeys = wdict['wrapkeys']
    else:
        print "Didn't fit conditions:"
        print jwdict
        print wdict

    print "%sjob: %s (jk=%s)  %d/%d  %s - %s   wrap: %s %s (wk=%s) - %s %s" % \
          (indent, pfwutils.pad_jobnum(jdict['jobnum']), jdict['jobkeys'], 
           numwraps, expnumwrap, jstate, jstatus,
           wrapnum, modname, wrapkeys,
           state, status)


######################################################################
def print_single_block(blknum, blockinfo, job_byblk, jwrap_byjob, wrap_byjob, verbose=False):
    #print "print_single_block(%s,..." % blknum 
    print blockinfo['name']


    if blknum not in job_byblk:
        print "\tNo jobs for this block"
    else:
        for jtid, jobdict in sorted(job_byblk[blknum].items()):
            #if jtid not in jwrap_byjob:
            #    print "No wrappers found for this job (j=%i, keys=%s), perhaps none have run yet." % (jtid, jobdict['jobkeys'])
            #    continue

            expnumwrap = 0
            if 'expect_num_wrap' in jobdict and jobdict['expect_num_wrap'] is not None:
                expnumwrap = jobdict['expect_num_wrap']
            #print "ENUMW",expnumwrap

            numwraps = 0
            maxwrap = None
            jwdict = None
            wdict = None

            if jtid in jwrap_byjob:
                numwraps = len(jwrap_byjob[jtid])
                maxwrap = max(jwrap_byjob[jtid].keys())
                jwdict = jwrap_byjob[jtid][maxwrap]
                if jtid in wrap_byjob and maxwrap in wrap_byjob[jtid]:
                    wdict = wrap_byjob[jtid][maxwrap]

                #print_single_wrap(maxwrap, numwraps, expnumwrap, jobdict, jwdict, wdict, "\t")
                wcnt = 1
                for wrapnum, jwdict in sorted(jwrap_byjob[jtid].items()):
                    jwdict = jwrap_byjob[jtid][wrapnum]
                    wdict = None
                    if jtid in wrap_byjob and wrapnum in wrap_byjob[jtid]:
                        wdict = wrap_byjob[jtid][wrapnum]
                    if verbose or wrapnum == maxwrap or jwdict['end_time'] is None:
                        print_single_wrap(wrapnum, numwraps, expnumwrap, jobdict, jwdict, wdict, "\t\t")
                    wcnt += 1
            else:
                print_single_wrap(0, 0, expnumwrap, jobdict, None, None, "\t\t")


def print_job_info(argv):
    """    """

    args = parse_args(argv)

    try:
        dbh = pfwdb.PFWDB(args['des_services'], args['section'])
    except ConfigParser.NoSectionError:
        print "Can't determine section of services file to get DB connection info"
        print "\tEither set environment variable DES_DB_SECTION or add command-line option --section"
        sys.exit(1)

    # get the run info
    for run in args['runs']:
        print run
        reqnum, unitname, attnum = parse_attempt_str(run)
        attinfo = dbh.get_attempt_info(reqnum, unitname, attnum)
        if attinfo is None:
            print "No DB information about the processing attempt"
            print "(Double check which DB querying vs which DB the attempt used)"
        else:
            if 'endtime' in attinfo and attinfo['endtime'] is not None:
                print "Note:  run has finished with status %s" % attinfo['status'] 

            # get the block info
            blockinfo = dbh.get_block_info(pfw_attempt_id=attinfo['id'])

            # get job info
            jobinfo = dbh.get_job_info({'pfw_attempt_id':attinfo['id']})
            # index jobinfo by blknum
            job_byblk = pfwutils.index_job_info(jobinfo)

            jobwrapinfo = dbh.get_jobwrapper_info(id=attinfo['id'])
            jwrap_byjob, jwrap_bywrap = pfwutils.index_jobwrapper_info(jobwrapinfo)

            # get wrapper instance information
            wrapinfo = dbh.get_wrapper_info(pfw_attempt_id=attinfo['id'])
            wrap_byjob, wrap_bymod = pfwutils.index_wrapper_info(wrapinfo)

            for blknum in blockinfo.keys():
                print_single_block(blknum, blockinfo[blknum], job_byblk, jwrap_byjob, wrap_byjob, args['verbose'])

if __name__ == "__main__":
    print_job_info(sys.argv[1:])
