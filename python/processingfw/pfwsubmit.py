#!/usr/bin/env python
# $Id: pfwsubmit.py 48384 2019-03-11 13:31:55Z friedel $
# $Rev:: 48384                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-03-11 08:31:55 #$:  # Date of last commit.

# pylint: disable=print-statement

""" need to write docstring """

import sys
import time
import re
import os
import stat

import intgutils.intgdefs as intgdefs
import despymisc.miscutils as miscutils
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwcondor as pfwcondor
import processingfw.pfwlog as pfwlog


######################################################################
def min_wcl_checks(config):
    """ execute minimal submit wcl checks """
    max_label_length = 30    # todo: figure out how to get length from DB

    msg = "ERROR\nError: Missing %s in submit wcl.  Make sure submitting correct file.  "
    msg += "Aborting submission."

    # check that reqnum and unitname exist
    (exists, _) = config.search(pfwdefs.REQNUM, {intgdefs.REPLACE_VARS: True})
    if not exists:
        miscutils.fwdie(msg % pfwdefs.REQNUM, pfwdefs.PF_EXIT_FAILURE)

    (exists, _) = config.search(pfwdefs.UNITNAME, {intgdefs.REPLACE_VARS: True})
    if not exists:
        miscutils.fwdie(msg % pfwdefs.UNITNAME, pfwdefs.PF_EXIT_FAILURE)

    # check that any given labels are short enough
    (exists, labelstr) = config.search(pfwdefs.SW_LABEL, {intgdefs.REPLACE_VARS: True})
    if exists:
        labels = miscutils.fwsplit(labelstr, ',')
        for lab in labels:
            if len(lab) > max_label_length:
                miscutils.fwdie("ERROR\nError: label %s is longer (%s) than allowed (%s).  " \
                                "Aborting submission." % \
                                (lab, len(lab), max_label_length), pfwdefs.PF_EXIT_FAILURE)


######################################################################
def check_proxy(config):
    """ Check if any block will submit to remote machine needing proxy, if so check for proxy """

    if miscutils.fwdebug_check(3, 'PFWSUBMIT_DEBUG'):
        miscutils.fwdebug_print("Beg")

    config.reset_blknum()
    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(), ',')
    for blockname in blocklist:
        if miscutils.fwdebug_check(3, 'PFWSUBMIT_DEBUG'):
            miscutils.fwdebug_print("Checking block %s..." % (blockname))
        config.set_block_info()

        (exists, check_proxy) = config.search(pfwdefs.SW_CHECK_PROXY, {intgdefs.REPLACE_VARS: True})
        if exists and miscutils.convertBool(check_proxy):
            timeleft = pfwcondor.get_grid_proxy_timeleft()
            assert timeleft > 0
            if timeleft < 21600:   # 5 * 60 * 60
                print "Warning:  Proxy expires in less than 5 hours"
                break
        config.inc_blknum()

    config.reset_blknum()
    if miscutils.fwdebug_check(3, 'PFWSUBMIT_DEBUG'):
        miscutils.fwdebug_print("End")

######################################################################
def create_common_vars(config, jobname):
    """ Create string containing vars string for job """

    blkname = config.getfull('blockname')
    attribs = config.get_condor_attributes(blkname, jobname)
    varstr = ""
    if len(attribs) > 0:
        varstr = "VARS %s" % jobname
        for (key, val) in attribs.items():
            varstr += ' %s="%s"' % (key[len(pfwdefs.ATTRIB_PREFIX):], val)
    varstr += ' jobname="%s"' % jobname
    varstr += ' pfwdir="%s"' % config.getfull('processingfw_dir')

    return varstr


######################################################################
def write_block_dag(config, blkdir, blockname, debugfh=None):
    """  writes block dag file """

    if not debugfh:
        debugfh = sys.stderr

    debugfh.write('write_block_dag pwd: %s\n' % os.getcwd())

    pfwdir = config.getfull('processingfw_dir')
    cwd = os.getcwd()

    miscutils.coremakedirs(blkdir)
    os.chdir(blkdir)
    print "curr dir = ", os.getcwd()

    configfile = "../uberctrl/config.des"

    jobmngr = write_stub_jobmngr_dag(config, blockname, blkdir, debugfh)
    dag = config.get_filename('blockdag', {intgdefs.REPLACE_VARS: True})

    dagfh = open(dag, 'w')

    dagfh.write('JOB begblock blocktask.condor\n')
    dagfh.write('VARS begblock exec="$(pfwdir)/libexec/begblock.py"\n')
    dagfh.write('VARS begblock args="%s"\n' % configfile)
    varstr = create_common_vars(config, 'begblock')
    dagfh.write('%s\n' % varstr)
    dagfh.write('SCRIPT pre begblock %s/libexec/logpre.py %s %s j $JOB\n' % \
                (pfwdir, configfile, blockname))
    dagfh.write('SCRIPT post begblock %s/libexec/logpost.py %s %s j $JOB $RETURN\n' % \
                (pfwdir, configfile, blockname))

    dagfh.write('\n')
    dagfh.write('JOB jobmngr %s.condor.sub\n' % jobmngr)
    dagfh.write('SCRIPT pre jobmngr %s/libexec/logpre.py %s %s j $JOB\n' % \
                (pfwdir, configfile, blockname))
    dagfh.write('SCRIPT post jobmngr %s/libexec/logpost.py %s %s j $JOB $RETURN\n' % \
                (pfwdir, configfile, blockname))

    dagfh.write('\n')
    dagfh.write('JOB endblock blocktask.condor\n')
    dagfh.write('VARS endblock exec="%s/libexec/endblock.py"\n' % pfwdir)
    dagfh.write('VARS endblock args="%s"\n' % configfile)
    varstr = create_common_vars(config, 'endblock')
    dagfh.write('%s\n' % varstr)
    dagfh.write('SCRIPT pre endblock %s/libexec/logpre.py %s %s j $JOB\n' % \
                (pfwdir, configfile, blockname))
    dagfh.write('SCRIPT post endblock %s/libexec/logpost.py %s %s j $JOB $RETURN\n' % \
                (pfwdir, configfile, blockname))

    dagfh.write('\nPARENT begblock CHILD jobmngr\n')
    dagfh.write('PARENT jobmngr CHILD endblock\n')
    dagfh.close()
    pfwcondor.add2dag(dag, config.get_dag_cmd_opts(),
                      config.get_condor_attributes(blockname, "blockmngr"),
                      blkdir, debugfh)
    os.chdir(cwd)
    return dag


######################################################################
def write_stub_jobmngr_dag(config, block, blkdir, debugfh=None):
    """  writes stub jobmngr dag file to be overwritten during block """

    if not debugfh:
        debugfh = sys.stderr

    debugfh.write('write_stub_jobmngr pwd: %s\n' % os.getcwd())

    pfwdir = config.getfull('processingfw_dir')
    dag = config.get_filename('jobdag')

    dagfh = open(dag, 'w')
    dagfh.write('JOB 0001 %s/share/condor/localjob.condor\n' % pfwdir)
    dagfh.write('SCRIPT pre 0001 %s/libexec/logpre.py ../uberctrl/config.des %s j $JOB' % \
                (pfwdir, block))
    dagfh.write('SCRIPT post 0001 %s/libexec/logpost.py ../uberctrl/config.des %s j $JOB $RETURN' %\
                (pfwdir, block))
    dagfh.close()

    pfwcondor.add2dag(dag, config.get_dag_cmd_opts(),
                      config.get_condor_attributes(block, "jobmngr"),
                      blkdir, debugfh)

    os.unlink(dag)
    return dag

######################################################################
def write_main_dag(config, maindag):
    """ Writes main manager dag input file """
    pfwdir = config.getfull('processingfw_dir')

    print "maindag = '%s', type=%s" % (maindag, type(maindag))
    dagfh = open(maindag, 'w')

    dagfh.write("""
JOB begrun %s/share/condor/runtask.condor
VARS begrun exec="$(pfwdir)/libexec/begrun.py"
VARS begrun arguments="../uberctrl/config.des"
""" % (pfwdir))
    varstr = create_common_vars(config, 'begrun')
    dagfh.write('%s\n' % varstr)

    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(), ',')
    for i in range(len(blocklist)):
        blockname = blocklist[i]
        blockdir = "../B%02d-%s" % (i+1, blockname)
        cjobname = "B%02d-%s" % (i+1, blockname)
        blockdag = write_block_dag(config, blockdir, blockname)
        dagfh.write("""
JOB %(cjob)s %(bdir)s/%(bdag)s.condor.sub
SCRIPT pre %(cjob)s %(pdir)s/libexec/blockpre.py ../uberctrl/config.des
SCRIPT post %(cjob)s %(bdir)s/blockpost.sh $RETURN


""" % {'cjob':cjobname, 'bdir':blockdir, 'bdag':blockdag, 'pdir':pfwdir})
        varstr = create_common_vars(config, cjobname)
        dagfh.write('%s\n' % varstr)
#SCRIPT post %(cjob)s %(pdir)s/libexec/blockpost.py ../uberctrl/config.des $RETURN

        with open('%s/blockpost.sh' % blockdir, 'w') as bpostfh:
            bpostfh.write("#!/usr/bin/env sh\n")
            bpostfh.write("sem --record-env\n")
            bpostfh.write("sem --fg --id blockpost -j 20 %s/libexec/blockpost.py ../uberctrl/config.des $1\n" % (pfwdir))
        os.chmod("%s/blockpost.sh" % (blockdir), stat.S_IRWXU | stat.S_IRWXG)

    dagfh.write("""
JOB endrun %s/share/condor/runtask.condor
VARS endrun exec="$(pfwdir)/libexec/endrun.py"
VARS endrun arguments="../uberctrl/config.des"
""" % (pfwdir))
    varstr = create_common_vars(config, 'endrun')
    dagfh.write('%s\n' % varstr)

    child = "B%02d-%s" % (1, blocklist[0])
    dagfh.write("PARENT begrun CHILD %s\n" % child)
    for i in range(1, len(blocklist)):
        parent = child
        child = "B%02d-%s" % (i+1, blocklist[i])
        dagfh.write("PARENT %s CHILD %s\n" % (parent, child))
    dagfh.write("PARENT %s CHILD endrun\n" % child)

    dagfh.close()
    pfwcondor.add2dag(maindag, config.get_dag_cmd_opts(),
                      config.get_condor_attributes('uberctrl', 'mainmngr'),
                      None, sys.stdout)


######################################################################
def run_sys_checks():
    """ Check valid system environemnt (e.g., condor setup) """

    ### Check for Condor in path as well as daemons running
    print '\tChecking for Condor....',
    max_tries = 5
    try_delay = 60 # seconds

    trycnt = 0
    done = False
    while not done and trycnt < max_tries:
        try:
            trycnt += 1
            pfwcondor.check_condor('7.4.0')
            done = True
        except pfwcondor.CondorException as excpt:
            print "ERROR"
            print str(excpt)
            if trycnt < max_tries:
                print "\nSleeping and then retrying"
                time.sleep(try_delay)
        except Exception as excpt:
            print "ERROR"
            raise excpt

    if not done and trycnt >= max_tries:
        miscutils.fwdie("Too many errors.  Aborting.", pfwdefs.PF_EXIT_FAILURE)

    print "DONE"


######################################################################
def submit_main_dag(config, dagfile, logfh):
    """ Submit main DAG file to Condor"""
    (exitcode, outtuple) = pfwcondor.condor_submit('%s.condor.sub' % (dagfile))
    if exitcode or re.search('ERROR', outtuple[0]):
        sys.stderr.write('\n%s\n' % (outtuple[0]))

        logfh.write('\ncondor_submit %s.condor.sub\n%s\n' %
                    (dagfile, outtuple[0]))
    else:
        print '\nImage processing successfully submitted to condor:'
        print '\tRun = %s' % (config.getfull('submit_run'))
        print "\tpfw_attempt_id = %s" % (config['pfw_attempt_id'])
        print "\tpfw_attempt task_id = %s" % (config['task_id']['attempt'])
    print '\n'

    # for completeness, log condorid of pipeline manager
    dagjob = pfwcondor.parse_condor_user_log('%s/%s.dagman.log' % \
                                             (config.getfull('uberctrl_dir'), dagfile))
    jobids = dagjob.keys()
    condorid = None
    if len(jobids) == 1:
        condorid = int(jobids[0])
    pfwlog.log_pfw_event(config, 'analysis', 'j', 'mngr', 'pretask')
    pfwlog.log_pfw_event(config, 'analysis', 'j', 'mngr',
                         {'cid': condorid})

    return condorid


######################################################################
def create_submitside_dirs(config):
    """ Create directories for storage of pfw files on submit side """
    # make local working dir
    workdir = config.getfull('work_dir')
    if miscutils.fwdebug_check(3, 'PFWSUBMIT_DEBUG'):
        miscutils.fwdebug_print("workdir = %s" % workdir)

    if os.path.exists(workdir):
        raise Exception('%s subdirectory already exists.\nAborting submission' % (workdir))

    print '\tMaking submit run directory...',
    miscutils.coremakedirs(workdir)
    print 'DONE'

    uberdir = config.getfull('uberctrl_dir')
    if miscutils.fwdebug_check(3, 'PFWSUBMIT_DEBUG'):
        miscutils.fwdebug_print("uberdir = %s" % uberdir)
    print '\tMaking submit uberctrl directory...',
    miscutils.coremakedirs(uberdir)
    print 'DONE'


if __name__ == "__main__":
    pass
