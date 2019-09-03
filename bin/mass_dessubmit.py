#!/usr/bin/env python

# $Id: mass_dessubmit.py 42539 2016-06-09 21:00:23Z mgower $
# $Rev:: 42539                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-06-09 16:00:23 #$:  # Date of last commit.

""" Replaces mass submit variables in a template submit file and calls dessubmit
    doing some throttling, spacing out of the submits """

import argparse
import subprocess
import datetime
import time
import sys
import os

import despymisc.miscutils as miscutils
import processingfw.pfwcondor as pfwcondor
import processingfw.pfwdefs as pfwdefs

######################################################################
def tsstr():
    """ Return the current time as a string """
    return datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

######################################################################
def parse_cmdline(argv):
    """ Parse the command line """
    #echo "Usage: submitmassjob.sh desfile tilelist maxjobs site";
    parser = argparse.ArgumentParser(description='Submit multiple runs to the processing framework')
    parser.add_argument('--delimiter', action='store', default=None,
                        help='character separating columns')
    parser.add_argument('--delay', action='store', type=int, default=900,
                        help='seconds between submits')
    parser.add_argument('--delay_check', action='store', type=int, default=300,
                        help='seconds between check ')
    parser.add_argument('--force', action='store_true', default=False,
                        help='resubmit even if previously submitted')
    parser.add_argument('--nosubmit', action='store_true', default=False,
                        help='create submit files but do not run dessubmit')

    parser.add_argument('--maxjobs', action='store', type=int,
                        help='maximum number of jobs submitted at same time')
    parser.add_argument('--site', action='store')
    parser.add_argument('--operator', action='store',
                        help='filter maxjobs on operator')
    parser.add_argument('--reqnum', action='store',
                        help='filter maxjobs on reqnum')
    parser.add_argument('--pipeline', action='store',
                        help='filter maxjobs on pipeline')


    parser.add_argument('--group_submit_id', action='store', type=int, default=1,
                        help='numeric value stored in pfw_attempt table')

    parser.add_argument('--outfilepat', action='store',
                        help='filename pattern for submit wcl and log file (will add suffix)')
    parser.add_argument('--submitfiledir', action='store',
                        help='directory for submit wcl file')
    parser.add_argument('--logdir', action='store',
                        help='directory for submit log file')

    parser.add_argument('templatewcl', action='store',
                        help='template submit wcl filename')
    parser.add_argument('submitlist', action='store',
                        help='file containing 1 row with info per submit')

    args = vars(parser.parse_args(argv))   # convert dict

    if args['logdir'] is not None and len(args['logdir']) != 0:
        if args['logdir'][0] != '/':
            args['logdir'] = "%s/%s" % (os.getcwd(), args['logdir'])

    if args['submitfiledir'] is not None and len(args['submitfiledir']) != 0:
        if args['submitfiledir'][0] != '/':
            args['submitfiledir'] = "%s/%s" % (os.getcwd(), args['submitfiledir'])

    return args

######################################################################
def can_submit(args):
    """ whether can submit another attempt or not """

    print "%s: Checking whether can submit another attempt" % tsstr()
    dosubmit = None

    constraint_str = "-constraint %sisjob " % pfwdefs.ATTRIB_PREFIX
    (qjobs, att_jobs, _) = pfwcondor.condorq_dag(constraint_str)

    jobcnt = 0
    for topjobid in att_jobs:
        info = pfwcondor.get_attempt_info(topjobid, qjobs)
        if (args['site'] is None or args['site'].lower() == info['runsite'].lower()) and \
           (args['operator'] is None or args['operator'].lower() == info['operator'].lower()) and \
           (args['pipeline'] is None or args['pipeline'].lower() == info['pipeline'].lower()) and \
           (args['reqnum'] is None or '_r%sp' % (args['reqnum']) in info['run']):
            jobcnt += 1

    if jobcnt >= args['maxjobs']:
        dosubmit = False
    else:
        dosubmit = True

    print "%s:\tmaxjobs=%s, jobcnt=%s, can_submit=%s" % (tsstr(), args['maxjobs'], jobcnt, dosubmit)
    return dosubmit


######################################################################
def submit(submitfile, logdir):
    """ Call dessubmit on the specific submit file that has mass submit variables replaced """
    print "%s: Submitting %s" % (tsstr(), submitfile)

    cwd = os.getcwd()

    # create log filename
    submitbase = os.path.basename(submitfile)
    submitdir = os.path.dirname(submitfile)
    prefix = os.path.splitext(submitbase)[0]
    logfilename = "%s.log" % prefix

    if logdir is not None and len(logdir) != 0:
        miscutils.coremakedirs(logdir)
        logfilename = "%s/%s" % (logdir, logfilename)

    os.chdir(submitdir)
    print "%s: dessubmit stdout/stderr - %s" % (tsstr(), logfilename)
    cmd = "dessubmit %s" % (submitbase)
    with open(logfilename, 'w') as logfh:
        # call dessubmit
        try:
            process = subprocess.Popen(cmd.split(),
                                       shell=False,
                                       stdout=logfh,
                                       stderr=subprocess.STDOUT)
        except:
            (extype, exvalue, _) = sys.exc_info()
            print "********************"
            print "Unexpected error: %s" % exvalue
            print "cmd> %s" % cmd
            print "Probably could not find %s in path" % cmd.split()[0]
            raise

        process.wait()
        print "%s: dessubmit finished with exit code = %s" % (tsstr(), process.returncode)
        if process.returncode != 0:
            raise Exception("Non-zero exit code from dessubmit")

    os.chdir(cwd)



######################################################################
def main(argv):
    """ Program entry point """
    args = parse_cmdline(argv)

    origtname = args['templatewcl']
    origtwcl = None
    with open(origtname, 'r') as twclfh:
        origtwcl = ''.join(twclfh.readlines())

    with open(args['submitlist'], 'r') as sublistfh:
        for line in sublistfh:
            line = line.split('#')[0].strip()
            if line == "": # skip comments or blank lines
                continue

            info = miscutils.fwsplit(line, args['delimiter'])

            # update name
            newtname = None
            if args['outfilepat'] is not None:
                newtname = args['outfilepat']
            else:
                newtname = origtname

            if args['submitfiledir'] is not None:
                newtname = '%s/%s' % (args['submitfiledir'], newtname)

            logdir = ""
            if args['logdir'] is not None:
                logdir = args['logdir']

            for i in range(0, len(info)):
                newtname = newtname.replace('XXX%dXXX' % (i+1), info[i])
                logdir = logdir.replace('XXX%dXXX' % (i+1), info[i])
            if not newtname.endswith(".des"):
                newtname += ".des"


            if args['force'] or not os.path.exists(newtname):
                submitdir = os.path.dirname(newtname)
                if submitdir != "":
                    miscutils.coremakedirs(submitdir)

                # can I submit?
                if not args['nosubmit']:
                    while not can_submit(args):
                        print "%s: Shouldn't submit, sleeping %s seconds." % \
                              (tsstr(), args['delay_check'])
                        time.sleep(args['delay_check'])

                newwcl = origtwcl

                for i in range(0, len(info)):
                    newwcl = newwcl.replace('XXX%dXXX' % (i+1), info[i])

                newwcl += 'GROUP_SUBMIT_ID = %d\n' % args['group_submit_id']

                print "%s: Writing submit wcl: %s" % (tsstr(), newtname)
                with open(newtname, 'w') as ntwclfh:
                    ntwclfh.write(newwcl)

                # submit it
                if not args['nosubmit']:
                    submit(newtname, logdir)

                    print "%s: Sleeping %s seconds after submit." % (tsstr(), args['delay'])
                    time.sleep(args['delay'])
            else:
                print "skipping %s" % newtname


if __name__ == '__main__':
    main(sys.argv[1:])
