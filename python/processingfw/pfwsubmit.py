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
                miscutils.fwdie(f"ERROR\nError: label {lab} is longer ({len(lab)}) than allowed ({max_label_length}).  Aborting submission.", pfwdefs.PF_EXIT_FAILURE)


######################################################################
def check_proxy(config):
    """ Check if any block will submit to remote machine needing proxy, if so check for proxy """

    if miscutils.fwdebug_check(3, 'PFWSUBMIT_DEBUG'):
        miscutils.fwdebug_print("Beg")

    config.reset_blknum()
    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(), ',')
    for blockname in blocklist:
        if miscutils.fwdebug_check(3, 'PFWSUBMIT_DEBUG'):
            miscutils.fwdebug_print(f"Checking block {blockname}...")
        config.set_block_info()

        (exists, chk_prx) = config.search(pfwdefs.SW_CHECK_PROXY, {intgdefs.REPLACE_VARS: True})
        if exists and miscutils.convertBool(chk_prx):
            timeleft = pfwcondor.get_grid_proxy_timeleft()
            assert timeleft > 0
            if timeleft < 21600:   # 5 * 60 * 60
                print("Warning:  Proxy expires in less than 5 hours")
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
    if attribs:
        varstr = f"VARS {jobname}"
        for (key, val) in attribs.items():
            varstr += f" {key[len(pfwdefs.ATTRIB_PREFIX):]}=\"{val}\""
    varstr += f" jobname=\"{jobname}\""
    varstr += f" pfwdir=\"{config.getfull('processingfw_dir')}\""

    return varstr


######################################################################
def write_block_dag(config, blkdir, blockname, debugfh=None):
    """  writes block dag file """

    if not debugfh:
        debugfh = open('/home/friedel/despy3new/debugs', 'w')
        #debugfh = sys.stderr

    debugfh.write(f"write_block_dag pwd: {os.getcwd()}\n")

    pfwdir = config.getfull('processingfw_dir')
    cwd = os.getcwd()

    miscutils.coremakedirs(blkdir)
    os.chdir(blkdir)
    print("curr dir = ", os.getcwd())

    configfile = "../uberctrl/config.des"

    jobmngr = write_stub_jobmngr_dag(config, blockname, blkdir, debugfh)
    dag = config.get_filename('blockdag', {intgdefs.REPLACE_VARS: True})

    dagfh = open(dag, 'w')

    dagfh.write("JOB begblock blocktask.condor\n")
    dagfh.write("VARS begblock exec=\"$(pfwdir)/libexec/begblock.py\"\n")
    dagfh.write(f"VARS begblock args=\"{configfile}\"\n")
    varstr = create_common_vars(config, 'begblock')
    dagfh.write(f"{varstr}\n")
    dagfh.write(f"SCRIPT pre begblock {pfwdir}/libexec/logpre.py {configfile} {blockname} j $JOB\n")
    dagfh.write(f"SCRIPT post begblock {pfwdir}/libexec/logpost.py {configfile} {blockname} j $JOB $RETURN\n")

    dagfh.write('\n')
    dagfh.write(f"JOB jobmngr {jobmngr}.condor.sub\n")
    dagfh.write(f"SCRIPT pre jobmngr {pfwdir}/libexec/logpre.py {configfile} {blockname} j $JOB\n")
    dagfh.write(f"SCRIPT post jobmngr {pfwdir}/libexec/logpost.py {configfile} {blockname} j $JOB $RETURN\n")

    dagfh.write('\n')
    dagfh.write('JOB endblock blocktask.condor\n')
    dagfh.write(f"VARS endblock exec=\"{pfwdir}/libexec/endblock.py\"\n")
    dagfh.write(f"VARS endblock args=\"{configfile}\"\n")
    varstr = create_common_vars(config, 'endblock')
    dagfh.write(f"{varstr}\n")
    dagfh.write(f"SCRIPT pre endblock {pfwdir}/libexec/logpre.py {configfile} {blockname} j $JOB\n")
    dagfh.write(f"SCRIPT post endblock {pfwdir}/libexec/logpost.py {configfile} {blockname} j $JOB $RETURN\n")

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

    debugfh.write(f"write_stub_jobmngr pwd: {os.getcwd()}\n")

    pfwdir = config.getfull('processingfw_dir')
    dag = config.get_filename('jobdag')

    dagfh = open(dag, 'w')
    dagfh.write(f"JOB 0001 {pfwdir}/share/condor/localjob.condor\n")
    dagfh.write(f"SCRIPT pre 0001 {pfwdir}/libexec/logpre.py ../uberctrl/config.des {block} j $JOB")
    dagfh.write(f"SCRIPT post 0001 {pfwdir}/libexec/logpost.py ../uberctrl/config.des {block} j $JOB $RETURN")
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

    print(f"maindag = '{maindag}', type={type(maindag)}")
    dagfh = open(maindag, 'w')

    dagfh.write(f"""
JOB begrun {pfwdir}/share/condor/runtask.condor
VARS begrun exec="$(pfwdir)/libexec/begrun.py"
VARS begrun arguments="../uberctrl/config.des"
""")
    varstr = create_common_vars(config, 'begrun')
    dagfh.write(f"{varstr}\n")

    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(), ',')
    for i, blockname in enumerate(blocklist):
        blockdir = f"../B{i + 1:02d}-{blockname}"
        cjobname = f"B{i + 1:02d}-{blockname}"
        blockdag = write_block_dag(config, blockdir, blockname)
        dagfh.write(f"""
JOB {cjobname} {blockdir}/{blockdag}.condor.sub
SCRIPT pre {cjobname} {pfwdir}/libexec/blockpre.py ../uberctrl/config.des
SCRIPT post {cjobname} {blockdir}/blockpost.sh $RETURN


""")
        varstr = create_common_vars(config, cjobname)
        dagfh.write(f"{varstr}\n")
#SCRIPT post %(cjob)s %(pdir)s/libexec/blockpost.py ../uberctrl/config.des $RETURN

        with open(f"{blockdir}/blockpost.sh", 'w') as bpostfh:
            bpostfh.write("#!/usr/bin/env sh\n")
            bpostfh.write("sem --record-env\n")
            bpostfh.write(f"sem --fg --id blockpost -j 20 {pfwdir}/libexec/blockpost.py ../uberctrl/config.des $1\n")
        os.chmod(f"{blockdir}/blockpost.sh", stat.S_IRWXU | stat.S_IRWXG)

    dagfh.write(f"""
JOB endrun {pfwdir}/share/condor/runtask.condor
VARS endrun exec="$(pfwdir)/libexec/endrun.py"
VARS endrun arguments="../uberctrl/config.des"
""")
    varstr = create_common_vars(config, 'endrun')
    dagfh.write(f"{varstr}\n")

    child = f"B{1:02d}-{blocklist[0]}"
    dagfh.write(f"PARENT begrun CHILD {child}\n")
    for i in range(1, len(blocklist)):
        parent = child
        child = f"B{i + 1:02d}-{blocklist[i]}"
        dagfh.write(f"PARENT {parent} CHILD {child}\n")
    dagfh.write(f"PARENT {child} CHILD endrun\n")

    dagfh.close()
    pfwcondor.add2dag(maindag, config.get_dag_cmd_opts(),
                      config.get_condor_attributes('uberctrl', 'mainmngr'),
                      None, sys.stdout)


######################################################################
def run_sys_checks():
    """ Check valid system environemnt (e.g., condor setup) """

    ### Check for Condor in path as well as daemons running
    print('\tChecking for Condor....')
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
            print("ERROR")
            print(str(excpt))
            if trycnt < max_tries:
                print("\nSleeping and then retrying")
                time.sleep(try_delay)
        except Exception as excpt:
            print("ERROR")
            raise excpt

    if not done and trycnt >= max_tries:
        miscutils.fwdie("Too many errors.  Aborting.", pfwdefs.PF_EXIT_FAILURE)

    print("DONE")


######################################################################
def submit_main_dag(config, dagfile, logfh):
    """ Submit main DAG file to Condor"""
    (exitcode, outtuple) = pfwcondor.condor_submit(f"{dagfile}.condor.sub")
    if exitcode or re.search('ERROR', outtuple[0]):
        sys.stderr.write(f"\n{outtuple[0]}\n")

        logfh.write(f"\ncondor_submit {dagfile}.condor.sub\n{outtuple[0]}\n")
        logfh.flush()
    else:
        print('\nImage processing successfully submitted to condor:')
        print(f"\tRun = {config.getfull('submit_run')}")
        print(f"\tpfw_attempt_id = {config['pfw_attempt_id']}")
        print(f"\tpfw_attempt task_id = {config['task_id']['attempt']}")
    print('\n')

    # for completeness, log condorid of pipeline manager
    dagjob = pfwcondor.parse_condor_user_log(f"{config.getfull('uberctrl_dir')}/{dagfile}.dagman.log")
    jobids = list(dagjob.keys())
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
        miscutils.fwdebug_print(f"workdir = {workdir}")

    if os.path.exists(workdir):
        raise Exception(f"{workdir} subdirectory already exists.\nAborting submission")

    print('\tMaking submit run directory...')
    miscutils.coremakedirs(workdir)
    print('DONE')

    uberdir = config.getfull('uberctrl_dir')
    if miscutils.fwdebug_check(3, 'PFWSUBMIT_DEBUG'):
        miscutils.fwdebug_print(f"uberdir = {uberdir}")
    print('\tMaking submit uberctrl directory...')
    miscutils.coremakedirs(uberdir)
    print('DONE')
