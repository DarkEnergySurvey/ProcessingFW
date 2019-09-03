#!/usr/bin/env python
# $Id: endrun.py 41004 2015-12-11 15:49:41Z mgower $
# $Rev:: 41004                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-12-11 09:49:41 #$:  # Date of last commit.

""" Steps executed submit-side at end of run """

import sys
import os

import processingfw.pfwdefs as pfwdefs
import processingfw.pfwdb as pfwdb
import despymisc.miscutils as miscutils
import processingfw.pfwconfig as pfwconfig
import filemgmt.archive_transfer_utils as archive_transfer_utils


def endrun(configfile):
    """ Program entry point """
    miscutils.fwdebug_print("BEG")

    config = pfwconfig.PfwConfig({'wclfile': configfile})
    os.chdir('../uberctrl')

    retval = pfwdefs.PF_EXIT_SUCCESS

    if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in config and \
       config[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() == 'run':
        if pfwdefs.ATTEMPT_ARCHIVE_PATH not in config:
            print "Error:  Cannot find %s in config" % pfwdefs.ATTEMPT_ARCHIVE_PATH
            print "\tIt is needed for the mass copy of the run back to the " \
                  "home archive at the end of the run"
            return pfwdefs.PF_EXIT_FAILURE


        archpath = config.getfull(config[pfwdefs.ATTEMPT_ARCHIVE_PATH])
        print "archpath =", archpath


        # call archive transfer for target archive to home archive
        # check if using target archive
        target_info = None
        if pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in config and \
           config.getfull(pfwdefs.USE_TARGET_ARCHIVE_OUTPUT).lower() != 'never':
            if pfwdefs.TARGET_ARCHIVE in config and \
                config.getfull(pfwdefs.TARGET_ARCHIVE) in config[pfwdefs.SW_ARCHIVESECT]:
                target_info = config[pfwdefs.SW_ARCHIVESECT][config.getfull(pfwdefs.TARGET_ARCHIVE)]
            else:
                print "Error:  cannot determine info for target archive"
                return pfwdefs.PF_EXIT_FAILURE
        else:
            print "Error:  Asked to transfer outputs at end of run, but not using target archive"
            return pfwdefs.PF_EXIT_FAILURE

        home_info = None
        print config[pfwdefs.HOME_ARCHIVE]
        if pfwdefs.HOME_ARCHIVE in config and \
            config[pfwdefs.HOME_ARCHIVE] in config[pfwdefs.SW_ARCHIVESECT]:
            home_info = config[pfwdefs.SW_ARCHIVESECT][config.getfull(pfwdefs.HOME_ARCHIVE)]

        # call transfer
        archive_transfer_utils.archive_copy_dir(target_info, home_info,
                                                config.getfull('archive_transfer'), 
                                                archpath, config)


    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]):
        miscutils.fwdebug_print("Calling update_attempt_end: retval = %s" % retval)
        dbh = pfwdb.PFWDB(config.getfull('submit_des_services'), 
                          config.getfull('submit_des_db_section'))
        dbh.end_task(config['task_id']['attempt'], retval, True)
        dbh.commit()
        dbh.close()

    miscutils.fwdebug_print("END - exiting with code %s" % retval)
    return retval



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: endrun.py configfile"
        sys.exit(pfwdefs.PF_EXIT_FAILURE)

    print ' '.join(sys.argv)    # print command so can run by hand if needed
    sys.stdout.flush()

    sys.exit(endrun(sys.argv[1]))
