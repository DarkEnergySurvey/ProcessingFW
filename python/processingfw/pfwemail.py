# $Id: pfwemail.py 47226 2018-07-12 20:24:15Z friedel $
# $Rev:: 47226                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-07-12 15:24:15 #$:  # Date of last commit.

# pylint: disable=print-statement

""" Utilities for sending PFW emails """

import os
import glob
import subprocess
from io import StringIO

import processingfw.pfwdefs as pfwdefs
import intgutils.intgdefs as intgdefs
from despymisc import miscutils

NUMLINES = 50

def send_email(config, block, status, subject, msg1, msg2, sendit=True):
    """ create PFW email and send it"""
    try:
        project = config.getfull('project')
        run = config.getfull('submit_run')

        localmachine = os.uname()[1]

        mailfile = f"email_{block}.txt"
        mailfh = open(mailfile, "w")

        mailfh.write("""
*************************************************************
*                                                           *
*  This is an automated message from DESDM.  Do not reply.  *
*                                                           *
*************************************************************
    """)
        mailfh.write("\n")

        mailfh.write(f"{msg1}\n\n\n")

        mailfh.write(f"operator = {config.getfull('operator')}\n")
        mailfh.write(f"pipeline = {config.getfull('pipeline')}\n")
        mailfh.write(f"project = {project}\n")
        mailfh.write(f"run = {run}\n")
        if 'pfw_attempt_id' in config:
            mailfh.write(f"pfw_attempt_id = {config['pfw_attempt_id']}\n")
        if 'task_id' in config and 'attempt' in config['task_id']:
            mailfh.write(f"pfw_attempt task_id = {config['task_id']['attempt']}\n")

        mailfh.write("\n")

        (exists, home_archive) = config.search(pfwdefs.HOME_ARCHIVE, {intgdefs.REPLACE_VARS: True})
        if exists:
            mailfh.write("Home Archive:\n")
            mailfh.write(f"\t{pfwdefs.HOME_ARCHIVE.lower()} = {home_archive}\n")
            mailfh.write(f"\tArchive directory = {config.getfull('root')}/{config.getfull(pfwdefs.ATTEMPT_ARCHIVE_PATH)}\n")
            mailfh.write("\n")


        mailfh.write("Submit:\n")
        mailfh.write(f"\tmachine = {localmachine}\n")
        mailfh.write(f"\tPROCESSINGFW_DIR = {os.environ['PROCESSINGFW_DIR']}\n")
        mailfh.write(f"\torig config = {config.getfull('submit_dir')}/{config.getfull('submitwcl')}\n")
        mailfh.write(f"\tdirectory = {config.getfull('work_dir')}\n\n")


        mailfh.write("Target:\n")
        mailfh.write(f"\tsite = {config.getfull('target_site')}\n")
        (exists, target_archive) = config.search(pfwdefs.TARGET_ARCHIVE, {intgdefs.REPLACE_VARS: True})
        if exists:
            mailfh.write(f"\t{pfwdefs.TARGET_ARCHIVE.lower()} = {target_archive}\n")
        mailfh.write(f"\tmetapackage = {config.getfull('pipeprod')} {config.getfull('pipever')}\n")
        mailfh.write(f"\tjobroot = {config.getfull(pfwdefs.SW_JOB_BASE_DIR)}\n")
        mailfh.write("\n\n")

        mailfh.write("\n\n")
        mailfh.write("------------------------------\n")

        if msg2:
            mailfh.write(f"{msg2}\n")

        mailfh.close()

        subject = f"DESDM: {project} {run} {block} {subject}"
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
                print(f"Sending {mailfile} as email to {email} (block={block})")
                mailfh = open(mailfile, 'r')
                print(subprocess.check_output(['/bin/mail', '-s', subject, email], stdin=mailfh))
                mailfh.close()
                # don't delete email file as helps others debug as well as sometimes emails are missed
            else:
                print(f"Not sending {mailfile} as email to {email} (block={block})")
                print(f"subject: {subject}")
        else:
            print(block, "No email address.  Not sending email.")
    except Exception as ex:
        print("Non fatal ERROR. Could not send email: " + str(ex))
    except:
        print("Non fatal ERROR. Could not send email (unknown exception)")


def send_subblock_email(config, block, subblock, retval):
    """create PFW subblock email and send it"""
    print("send_subblock_email BEG")
    print(f"send_subblock_email block={block}")
    print(f"send_subblock_email subblock={subblock}")
    print(f"send_subblock_email retval={retval}")
    msg1 = f"Failed subblock = {subblock}"
    msg2 = get_subblock_output(subblock)
    send_email(config, block, retval, "[FAILED]", msg1, msg2)
    print("send_subblock_email END")


def get_job_info(block):
    """gather target job status info for email"""
    iostr = StringIO()
    iostr.write(f"{'JOBNUM':6s}\t{'MODULE':25s}\t{'STATUS4':7s}\t{'STATUS5':7s}\t{'MSG'}")
    filepat = f"../{block}_*/*.jobinfo.out"
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

    fileout = f"{path}/{block}/{subblock}.out"
    fileerr = f"{path}/{block}/{subblock}.err"

    iostr.write(f"Standard output = {fileout}\n")
    iostr.write(f"Standard error = {fileerr}\n")
    iostr.write("\n\n")

    iostr.write(f"===== Standard error  - Last {NUMLINES} lines =====\n")
    if os.path.exists(fileerr):
        cmd = f"tail -{NUMLINES} {fileerr}"
        lines = subprocess.check_output(cmd.split())
        iostr.write(lines)
    else:
        iostr.write(f"Could not read standard err file for {subblock}\n")
    iostr.write("\n\n")

    iostr.write(f"===== Standard output - Last {NUMLINES} lines =====\n")
    if os.path.exists(fileout):
        cmd = f"tail -{NUMLINES} {fileout}"
        lines = subprocess.check_output(cmd.split())
        iostr.write(lines)
    else:
        iostr.write(f"Could not read standard out file for {subblock}\n")

    return iostr.getvalue()
