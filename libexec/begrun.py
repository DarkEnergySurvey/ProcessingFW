#!/usr/bin/env python3
# $Id: begrun.py 47308 2018-07-31 19:42:07Z friedel $
# $Rev:: 47308                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-07-31 14:42:07 #$:  # Date of last commit.

""" Program executed at beginning of processing attempt """

import sys
import os

import despymisc.miscutils as miscutils
import filemgmt.fmutils as fmutils
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils
import processingfw.pfwconfig as pfwconfig
from processingfw.pfwemail import send_email
import qcframework.Messaging as Messaging

######################################################################
def copy_files_home(config, archive_info, filemgmt):
    """ Copy submit files to home archive """

    origwcl = config['origwcl']
    expwcl = config['expwcl']
    fullwcl = config['fullwcl']

    archdir = f"{config.getfull(pfwdefs.ATTEMPT_ARCHIVE_PATH)}/submit"
    if miscutils.fwdebug_check(6, 'BEGRUN_DEBUG'):
        miscutils.fwdebug_print(f"archive rel path = {archdir}")

    # copy the files to the home archive
    files2copy = {origwcl: {'src': origwcl,
                            'dst': f"{archdir}/{origwcl}",
                            'filename': origwcl,
                            'fullname': f"{archdir}/{origwcl}"},
                  expwcl: {'src':expwcl,
                           'dst': f"{archdir}/{expwcl}",
                           'filename': expwcl,
                           'fullname': f"{archdir}/{expwcl}"},
                  fullwcl: {'src':fullwcl,
                            'dst': f"{archdir}/{fullwcl}",
                            'filename': fullwcl,
                            'fullname': f"{archdir}/{fullwcl}"}}

    if miscutils.fwdebug_check(6, 'BEGRUN_DEBUG'):
        miscutils.fwdebug_print(f"files2copy = {files2copy}")

    # load file mvmt class
    submit_files_mvmt = config.getfull('submit_files_mvmt')
    if miscutils.fwdebug_check(6, 'BEGRUN_DEBUG'):
        miscutils.fwdebug_print(f"submit_files_mvmt = {submit_files_mvmt}")
    filemvmt_class = miscutils.dynamically_load_class(submit_files_mvmt)
    valdict = fmutils.get_config_vals(config['job_file_mvmt'], config,
                                      filemvmt_class.requested_config_vals())
    filemvmt = filemvmt_class(archive_info, None, None, None, valdict)

    results = filemvmt.job2home(files2copy)
    if miscutils.fwdebug_check(6, 'BEGRUN_DEBUG'):
        miscutils.fwdebug_print(f"trans results = {results}")

    # save info for files that we just copied into archive
    files2register = []
    problemfiles = {}
    for fname, finfo in results.items():
        if 'err' in finfo:
            problemfiles[fname] = finfo
            print(f"Warning: Error trying to copy file {fname} to archive: {finfo['err']}")
        else:
            files2register.append(finfo)

    # call function to do the register
    if miscutils.fwdebug_check(6, 'BEGRUN_DEBUG'):
        miscutils.fwdebug_print(f"files2register = {files2register}")
        miscutils.fwdebug_print(f"archive = {archive_info['name']}")
    filemgmt.register_file_in_archive(files2register, archive_info['name'])



######################################################################
def begrun(argv):
    """ Performs steps executed on submit machine at beginning of processing attempt """
    pfw_dbh = None
    try:
        configfile = argv[0]
        config = pfwconfig.PfwConfig({'wclfile': configfile})

        if miscutils.fwdebug_check(6, 'BEGRUN_DEBUG'):
            miscutils.fwdebug_print(f"use_home_archive_output = {config.getfull(pfwdefs.USE_HOME_ARCHIVE_OUTPUT)}")

        if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
            if config.dbh is None:
                import processingfw.pfwdb as pfwdb
                pfw_dbh = pfwdb.PFWDB(config.getfull('submit_des_services'),
                                      config.getfull('submit_des_db_section'))
            else:
                pfw_dbh = config.dbh
            pfw_dbh.begin_task(config['task_id']['attempt'], True)

        # the three wcl files to copy to the home archive
        origwcl = config['origwcl']
        expwcl = config['expwcl']
        fullwcl = config['fullwcl']

        # if not a dryrun and using a home archive for output
        if (config.getfull(pfwdefs.USE_HOME_ARCHIVE_OUTPUT) != 'never' and
                'submit_files_mvmt' in config and
                (pfwdefs.PF_DRYRUN not in config or
                 not miscutils.convertBool(config.getfull(pfwdefs.PF_DRYRUN)))):

            # get home archive info
            home_archive = config.getfull('home_archive')
            archive_info = config[pfwdefs.SW_ARCHIVESECT][home_archive]
            if pfw_dbh is not None:
                archive_info['connection'] = pfw_dbh
            # load filemgmt class
            attempt_tid = config['task_id']['attempt']
            filemgmt = pfwutils.pfw_dynam_load_class(pfw_dbh, config, attempt_tid, attempt_tid,
                                                     "filemgmt", archive_info['filemgmt'],
                                                     archive_info)

            # save file information
            filemgmt.register_file_data('wcl', [origwcl, expwcl, fullwcl], config['pfw_attempt_id'],
                                        attempt_tid, False, None, None)
            copy_files_home(config, archive_info, filemgmt)
            filemgmt.commit()

        if pfw_dbh is not None:
            print("Saving attempt's archive path into PFW tables...")
            pfw_dbh.update_attempt_archive_path(config)
            pfw_dbh.commit()
    except Exception as exc:
        msg = f"begrun: {exc.__class__.__name__}: {str(exc)}"
        if pfw_dbh is not None:
            Messaging.pfw_message(pfw_dbh, config['pfw_attempt_id'], config['task_id']['attempt'],
                                  msg, pfwdefs.PFWDB_MSG_ERROR, 'begrun.out', 0)
        send_failed_email(config, msg)
        raise
    except SystemExit as exc:
        msg = f"begrun: SysExit={str(exc)}"
        if pfw_dbh is not None:
            Messaging.pfw_message(pfw_dbh, config['pfw_attempt_id'], config['task_id']['attempt'],
                                  msg, pfwdefs.PFWDB_MSG_ERROR, 'begrun.out', 0)
        send_failed_email(config, msg)
        raise


######################################################################
def send_failed_email(config, msg2):
    """ Send failed email """

    if 'when_to_email' in config and config.getfull('when_to_email').lower() != 'never':
        print("Sending run failed email\n")
        msg1 = f"{config.getfull('submit_run')}:  processing attempt has failed in begrun."
        msg2 = "Typical failures to look for:\n"
        msg2 += "\tMissing desservices file section needed by file transfer\n"
        msg2 += "\tPermission problems in archive\n\n"

        outfile = "begrun.out"
        msg2 += f"########## {outfile} ##########\n"
        if os.path.exists(outfile):
            with open(outfile, 'r') as outfh:
                msg2 += '\n'.join(outfh.readlines())
        else:
            msg2 += 'Missing stdout\n'

        outfile = f"{config.getfull('submit_run')}_begrun.err"
        msg2 += f"########## {outfile} ##########\n"
        if os.path.exists(outfile):
            with open(outfile, 'r') as outfh:
                msg2 += '\n'.join(outfh.readlines())
        else:
            msg2 += 'Missing stderr\n'

        send_email(config, "begrun", pfwdefs.PF_EXIT_FAILURE, "", msg1, msg2)

if __name__ == "__main__":
    print(' '.join(sys.argv))  # print command line for debugging
    if len(sys.argv) != 2:
        print('Usage: begrun.py configfile')
        sys.exit(pfwdefs.PF_EXIT_FAILURE)
    begrun(sys.argv[1:])
