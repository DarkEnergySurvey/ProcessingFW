#!/usr/bin/env python
# $Id: pfwemail.py 47226 2018-07-12 20:24:15Z friedel $
# $Rev:: 47226                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-07-12 15:24:15 #$:  # Date of last commit.

# pylint: disable=print-statement

""" Utilities for sending PFW emails """

import os
import glob
import subprocess
from cStringIO import StringIO

import processingfw.pfwdefs as pfwdefs
import intgutils.intgdefs as intgdefs
from despymisc import miscutils

NUMLINES = 50

def send_email(config, block, status, subject, msg1, msg2, sendit=True):
    """create PFW email and send it"""
    try:
        project = config.getfull('project')
        run = config.getfull('submit_run')

        localmachine = os.uname()[1]

        mailfile = "email_%s.txt" % (block)
        mailfh = open(mailfile, "w")

        mailfh.write("""
*************************************************************
*                                                           *
*  This is an automated message from DESDM.  Do not reply.  *
*                                                           *
*************************************************************
    """)
        mailfh.write("\n")

        mailfh.write("%s\n\n\n" % msg1)

        mailfh.write("operator = %s\n" % config.getfull('operator'))
        mailfh.write("pipeline = %s\n" % config.getfull('pipeline'))
        mailfh.write("project = %s\n" % project)
        mailfh.write("run = %s\n" % run)
        if 'pfw_attempt_id' in config:
            mailfh.write("pfw_attempt_id = %s\n" % config['pfw_attempt_id'])
        if 'task_id' in config and 'attempt' in config['task_id']:
            mailfh.write("pfw_attempt task_id = %s\n" % config['task_id']['attempt'])

        mailfh.write("\n")

        (exists, home_archive) = config.search(pfwdefs.HOME_ARCHIVE, {intgdefs.REPLACE_VARS: True})
        if exists:
            mailfh.write("Home Archive:\n")
            mailfh.write("\t%s = %s\n" % (pfwdefs.HOME_ARCHIVE.lower(), home_archive))
            mailfh.write("\tArchive directory = %s/%s\n" % \
                         (config.getfull('root'),
                          config.getfull(pfwdefs.ATTEMPT_ARCHIVE_PATH)))
            mailfh.write("\n")


        mailfh.write("Submit:\n")
        mailfh.write("\tmachine = %s\n" % localmachine)
        mailfh.write("\tPROCESSINGFW_DIR = %s\n" % os.environ['PROCESSINGFW_DIR'])
        mailfh.write("\torig config = %s/%s\n" % \
                     (config.getfull('submit_dir'), config.getfull('submitwcl')))
        mailfh.write("\tdirectory = %s\n\n" % config.getfull('work_dir'))


        mailfh.write("Target:\n")
        mailfh.write("\tsite = %s\n" % config.getfull('target_site'))
        (exists, target_archive) = config.search(pfwdefs.TARGET_ARCHIVE, {intgdefs.REPLACE_VARS: True})
        if exists:
            mailfh.write("\t%s = %s\n" % (pfwdefs.TARGET_ARCHIVE.lower(), target_archive))
        mailfh.write("\tmetapackage = %s %s\n" % (config.getfull('pipeprod'), config.getfull('pipever')))
        mailfh.write("\tjobroot = %s\n" % (config.getfull(pfwdefs.SW_JOB_BASE_DIR)))
        mailfh.write("\n\n")

        mailfh.write("\n\n")
        mailfh.write("------------------------------\n")

        if msg2:
            mailfh.write("%s\n" % msg2)

        mailfh.close()

        subject = "DESDM: %s %s %s %s" % (project, run, block, subject)
        dryrun = False
        if miscutils.convertBool(config.getfull(pfwdefs.PF_DRYRUN)):
            dryrun = True
            subject += " [DRYRUN]"

        if int(status) != pfwdefs.PF_EXIT_SUCCESS and \
                (not dryrun or int(status) != pfwdefs.PF_EXIT_DRYRUN):
            subject += " [FAILED]"

        (exists, email) = config.search('email', {intgdefs.REPLACE_VARS: True})
        if exists:
            if sendit:
                print "Sending %s as email to %s (block=%s)" % (mailfile, email, block)
                mailfh = open(mailfile, 'r')
                print subprocess.check_output(['/bin/mail', '-s', '%s' % subject, email], stdin=mailfh)
                mailfh.close()
                # don't delete email file as helps others debug as well as sometimes emails are missed
            else:
                print "Not sending %s as email to %s (block=%s)" % (mailfile, email, block)
                print "subject: %s" % subject
        else:
            print block, "No email address.  Not sending email."
    except Exception as ex:
        print "Non fatal ERROR. Could not send email: " + str(ex)
    except:
        print "Non fatal ERROR. Could not send email (unknown exception)"


def send_subblock_email(config, block, subblock, retval):
    """create PFW subblock email and send it"""
    print "send_subblock_email BEG"
    print "send_subblock_email block=%s" % block
    print "send_subblock_email subblock=%s" % subblock
    print "send_subblock_email retval=%s" % retval
    msg1 = "Failed subblock = %s" % subblock
    msg2 = get_subblock_output(subblock)
    send_email(config, block, retval, "[FAILED]", msg1, msg2)
    print "send_subblock_email END"


def get_job_info(block):
    """gather target job status info for email"""
    iostr = StringIO()
    iostr.write("%6s\t%25s\t%7s\t%7s\t%s" % \
            ('JOBNUM', 'MODULE', 'STATUS4', 'STATUS5', 'MSG'))
    filepat = "../%s_*/*.jobinfo.out" % block
    jobinfofiles = glob.glob(filepat)
    for fname in jobinfofiles.sort():
        jobinfofh = open(fname, "r")
        iostr.write(jobinfofh.read())
        jobinfofh.close()
    return iostr.getvalue()



def get_subblock_output(subblock):
    """Grab tail of stdout/stderr to include in email"""
    (path, block) = os.path.split(os.getcwd())

    iostr = StringIO()

    fileout = "%s/%s/%s.out" % (path, block, subblock)
    fileerr = "%s/%s/%s.err" % (path, block, subblock)

    iostr.write("Standard output = %s\n" % fileout)
    iostr.write("Standard error = %s\n" % fileerr)
    iostr.write("\n\n")

    iostr.write("===== Standard error  - Last %s lines =====\n" % NUMLINES)
    if os.path.exists(fileerr):
        cmd = "tail -%s %s" % (NUMLINES, fileerr)
        lines = subprocess.check_output(cmd.split())
        iostr.write(lines)
    else:
        iostr.write("Could not read standard err file for %s\n" % subblock)
    iostr.write("\n\n")

    iostr.write("===== Standard output - Last %s lines =====\n" % NUMLINES)
    if os.path.exists(fileout):
        cmd = "tail -%s %s" % (NUMLINES, fileout)
        lines = subprocess.check_output(cmd.split())
        iostr.write(lines)
    else:
        iostr.write("Could not read standard out file for %s\n" % subblock)

    return iostr.getvalue()
