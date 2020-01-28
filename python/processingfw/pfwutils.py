# $Id: pfwutils.py 48552 2019-05-20 19:38:27Z friedel $
# $Rev:: 48552                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:38:27 #$:  # Date of last commit.


""" Miscellaneous support functions for processing framework """

import re
import os
import sys
import tarfile
import errno
import subprocess
import shlex
import time

import despymisc.miscutils as miscutils
import processingfw.pfwdefs as pfwdefs
import qcframework.Messaging as Messaging


#######################################################################
def pad_jobnum(jobnum):
    """ Pad the job number """
    return f"{int(jobnum):04d}"


#######################################################################
def get_hdrup_sections(wcl, prefix):
    """ Returns header update sections appearing in given wcl """
    hdrups = {}
    for key, val in wcl.items():
        if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
            miscutils.fwdebug_print(f"\tsearching for hdrup prefix in {key}")

        if re.search(fr"^{prefix}\S+$", key):
            if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
                miscutils.fwdebug_print(f"\tFound hdrup prefex {key}")
            hdrups[key] = val
    return hdrups



#######################################################################
def search_wcl_for_variables(wcl):
    """ Find variables in given wcl """
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")
    usedvars = {}
    for key, val in wcl.items():
        if isinstance(val, dict):
            uvars = search_wcl_for_variables(val)
            if uvars is not None:
                usedvars.update(uvars)
        elif isinstance(val, str):
            viter = [m.group(1) for m in re.finditer(r'(?i)\$\{([^$}]+)\}', val)]
            for vstr in viter:
                if ':' in vstr:
                    vstr = vstr.split(':')[0]
                usedvars[vstr] = True
        else:
            if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
                miscutils.fwdebug_print("Note: wcl is not string.")
                miscutils.fwdebug_print(f"key = {key}, type(val) = {type(val)}, val = '{val}'")

    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("END")
    return usedvars

#######################################################################
def get_wcl_value(key, wcl):
    """ Return value of key from wcl, follows section notation """
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")
    val = wcl
    for k in key.split('.'):
        #print "get_wcl_value: k=", k
        val = val[k]
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("END")
    return val

#######################################################################
def set_wcl_value(key, val, wcl):
    """ Sets value of key in wcl, follows section notation """
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")
    wclkeys = key.split('.')
    valkey = wclkeys.pop()
    wcldict = wcl
    for k in wclkeys:
        wcldict = wcldict[k]

    wcldict[valkey] = val
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("END")

#######################################################################
def tar_dir(filename, indir):
    """ Tars a directory """
    if filename.endswith('.gz'):
        mode = 'w:gz'
    else:
        mode = 'w'
    with tarfile.open(filename, mode) as tar:
        tar.add(indir)

#######################################################################
def tar_list(tarfilename, filelist):
    """ Tars a directory """

    if tarfilename.endswith('.gz'):
        mode = 'w:gz'
    else:
        mode = 'w'

    with tarfile.open(tarfilename, mode) as tar:
        for filen in filelist:
            tar.add(filen)



#######################################################################
def untar_dir(filename, outputdir):
    """ Untars a directory """
    if filename.endswith('.gz'):
        mode = 'r:gz'
    else:
        mode = 'r'

    maxcnt = 4
    cnt = 1
    done = False
    while not done and cnt <= maxcnt:
        with tarfile.open(filename, mode) as tar:
            try:
                tar.extractall(outputdir)
                done = True
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    print(f"Problems untaring {filename}: {exc}")
                    if cnt < maxcnt:
                        print("Trying again.")
                else:
                    print(f"Error: {exc}")
                    raise
        cnt += 1

    if not done:
        print(f"Could not untar {filename}.  Aborting")


###########################################################################
# assumes exit code for version is 0
def get_version(execname, execdefs):
    """run command with version flag and parse output for version"""

    ver = None
    if (execname.lower() in execdefs and
            'version_flag' in execdefs[execname.lower()] and
            'version_pattern' in execdefs[execname.lower()]):
        verflag = execdefs[execname.lower()]['version_flag']
        verpat = execdefs[execname.lower()]['version_pattern']

        cmd = f"{execname} {verflag}"
        try:
            process = subprocess.Popen(cmd.split(),
                                       shell=False,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       text=True)
        except:
            (extype, exvalue, _) = sys.exc_info()
            print("********************")
            print(f"Unexpected error: {extype} - {exvalue}")
            print(f"cmd> {cmd}")
            print(f"Probably could not find {cmd.split()[0]} in path")
            print("Check for mispelled execname in submit wcl or")
            print("    make sure that the corresponding eups package is in the metapackage ")
            print("    and it sets up the path correctly")
            raise

        process.wait()
        out = process.communicate()[0]
        if process.returncode != 0:
            miscutils.fwdebug_print("INFO:  problem when running code to get version")
            miscutils.fwdebug_print(f"\t{execname} {verflag} {verpat}")
            miscutils.fwdebug_print(f"\tcmd> {cmd}")
            miscutils.fwdebug_print(f"\t{out}")
            ver = None
        else:
            # parse output with verpat
            try:
                pmatch = re.search(verpat, out)
                if pmatch:
                    ver = pmatch.group(1)
                else:
                    if miscutils.fwdebug_check(1, "PFWUTILS_DEBUG"):
                        miscutils.fwdebug_print(f"re.search didn't find version for exec {execname}")
                    if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
                        miscutils.fwdebug_print(f"\tcmd output={out}")
                        miscutils.fwdebug_print(f"\tcmd verpat={verpat}")
            except Exception as err:
                #print type(err)
                ver = None
                print(f"Error: Exception from re.match.  Didn't find version: {err}")
                raise
    else:
        if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
            miscutils.fwdebug_print(f"INFO: Could not find version info for exec {execname}")

    return ver


############################################################################
def run_cmd_qcf(cmd, logfilename, wid, execnames, use_qcf=False, dbh=None, pfwattid=0, patterns={}, threaded=False):
    """ Execute the command piping stdout/stderr to log and QCF """
    bufsize = 1024 * 10
    lasttime = time.time()
    if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print(f"working dir = {os.getcwd()}")
        miscutils.fwdebug_print(f"cmd = {cmd}")
        miscutils.fwdebug_print(f"logfilename = {logfilename}")
        miscutils.fwdebug_print(f"wid = {wid}")
        miscutils.fwdebug_print(f"execnames = {execnames}")
        miscutils.fwdebug_print(f"use_qcf = {use_qcf}")

    use_qcf = miscutils.convertBool(use_qcf)

    sys.stdout.flush()
    try:
        messaging = Messaging.Messaging(logfilename, execnames, pfwattid=pfwattid, taskid=wid,
                                        dbh=dbh, usedb=use_qcf, qcf_patterns=patterns, threaded=threaded)
        process_wrap = subprocess.Popen(shlex.split(cmd),
                                        shell=False,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
    except:
        (extype, exvalue, _) = sys.exc_info()
        print("********************")
        print(f"Unexpected error: {extype} - {exvalue}")
        print(f"cmd> {cmd}")
        print(f"Probably could not find {cmd.split()[0]} in path")
        print("Check for mispelled execname in submit wcl or")
        print("    make sure that the corresponding eups package is in the metapackage ")
        print("    and it sets up the path correctly")
        raise

    try:
        buf = os.read(process_wrap.stdout.fileno(), bufsize)
        while process_wrap.poll() is None or buf:
            if dbh is not None:
                now = time.time()
                if now - lasttime > 30.*60.:
                    if not dbh.ping():
                        dbh.reconnect()
                    lasttime = now
            messaging.write(buf)
            #print buf
            buf = os.read(process_wrap.stdout.fileno(), bufsize)
            # brief sleep
            if process_wrap.poll() is None:
                time.sleep(0.1)

    except IOError as exc:
        print(f"\tI/O error({exc.errno}): {exc.strerror}")

    except:
        (extype, exvalue, _) = sys.exc_info()
        print(f"\tError: Unexpected error: {extype} - {exvalue}")
        raise

    sys.stdout.flush()
    if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
        if process_wrap.returncode != 0:
            miscutils.fwdebug_print(f"\tInfo: cmd exited with non-zero exit code = {process_wrap.returncode}")
            miscutils.fwdebug_print(f"\tInfo: failed cmd = {cmd}")
        else:
            miscutils.fwdebug_print("\tInfo: cmd exited with exit code = 0")


    if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("END")
    return process_wrap.returncode


#######################################################################
def index_job_info(jobinfo):
    """ create dictionary of jobs indexed on blk task id """
    job_byblk = {}
    for j, jdict in jobinfo.items():
        blktid = jdict['pfw_block_task_id']
        if blktid not in job_byblk:
            job_byblk[blktid] = {}
        job_byblk[blktid][j] = jdict

    return job_byblk


#######################################################################
def index_wrapper_info(wrapinfo):
    """ create dictionaries of wrappers indexed on jobnum and modname """
    wrap_byjob = {}
    wrap_bymod = {}
    for wrap in wrapinfo.values():
        if wrap['pfw_job_task_id'] not in wrap_byjob:
            wrap_byjob[wrap['pfw_job_task_id']] = {}
        wrap_byjob[wrap['pfw_job_task_id']][wrap['wrapnum']] = wrap
        if wrap['modname'] not in wrap_bymod:
            wrap_bymod[wrap['modname']] = {}
        wrap_bymod[wrap['modname']][wrap['wrapnum']] = wrap

    return wrap_byjob, wrap_bymod


#######################################################################
def index_jobwrapper_info(jwrapinfo):
    """ create dictionaries of wrappers indexed on jobnum and wrapnum """

    jwrap_byjob = {}
    jwrap_bywrap = {}
    for jwrap in jwrapinfo.values():
        if jwrap['label'] is None:
            print("Missing label for jobwrapper task.")
            print("Make sure you are using print_job.py from same ProcessingFW version as processing attempt")
            sys.exit(1)
        if jwrap['parent_task_id'] not in jwrap_byjob:
            jwrap_byjob[jwrap['parent_task_id']] = {}
        jwrap_byjob[jwrap['parent_task_id']][int(jwrap['label'])] = jwrap
        jwrap_bywrap[int(jwrap['label'])] = jwrap

    return jwrap_byjob, jwrap_bywrap


#######################################################################
def should_save_file(mastersave, filesave, exitcode):
    """ Determine whether should save the file """
    msave = mastersave.lower()
    fsave = miscutils.convertBool(filesave)

    if msave == 'failure':
        if exitcode != 0:
            msave = 'always'
        else:
            msave = 'file'

    return (msave == 'always') or (msave == 'file' and fsave)


#######################################################################
def should_compress_file(mastercompress, filecompress, exitcode):
    """ Determine whether should compress the file """

    if miscutils.fwdebug_check(6, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print(f"BEG: master={mastercompress}, file={filecompress}, exitcode={exitcode}")

    mcompress = mastercompress
    if isinstance(mastercompress, str):
        mcompress = mastercompress.lower()

    fcompress = miscutils.convertBool(filecompress)

    if mcompress == 'success':
        if exitcode != 0:
            mcompress = 'never'
        else:
            mcompress = 'file'

    retval = (mcompress == 'file' and fcompress)

    if miscutils.fwdebug_check(6, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print(f"END - retval = {retval}")
    return retval


######################################################################
def pfw_dynam_load_class(pfw_dbh, wcl, parent_tid, attempt_task_id,
                         label, classname, extra_info):
    """ Dynamically load a class save timing info in task table """

    #task_id = -1
    #if pfw_dbh is not None:
    #    task_id = pfw_dbh.create_task(name='dynclass',
    #                                  info_table=None,
    #                                  parent_task_id=parent_tid,
    #                                  root_task_id=attempt_task_id,
    #                                  label=label,
    #                                  do_begin=True,
    #                                  do_commit=True)

    the_class_obj = None
    try:
        the_class = miscutils.dynamically_load_class(classname)
        valdict = {}
        try:
            valdict = miscutils.get_config_vals(extra_info, wcl, the_class.requested_config_vals())
        except AttributeError: # in case the_class doesn't have requested_config_vals
            pass
        the_class_obj = the_class(valdict, wcl)
    except:
        (extype, exvalue, _) = sys.exc_info()
        msg = f"Error: creating {label} object - {extype} - {exvalue}"
        print(f"\n{msg}")
        if pfw_dbh is not None:
            Messaging.pfw_message(pfw_dbh, wcl['pfw_attempt_id'], parent_tid, msg, pfwdefs.PFWDB_MSG_ERROR)
        raise

    #if pfw_dbh is not None:
    #    pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

    return the_class_obj


######################################################################
def diskusage(path):
#    """ Calls du to get disk space used by given path """
#    process = subprocess.Popen(['du', '-s', path], shell=False,
#                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#    process.wait()
#    out = process.communicate()[0]
#    (diskusage, _) = out.split()
#    return int(diskusage)
    """ Walks the path returning the sum of the filesizes """
    ### avoids symlinked files, but
    ### doesn't avoid adding hardlinks twice
    usum = 0
    for (dirpath, _, filenames) in os.walk(path):
        for name in filenames:
            if not os.path.islink(f"{dirpath}/{name}"):
                fsize = os.path.getsize(f"{dirpath}/{name}")
                if miscutils.fwdebug_check(6, "PUDISKU_DEBUG"):
                    miscutils.fwdebug_print(f"size of {dirpath}/{name} = {fsize}")
                usum += fsize
    if miscutils.fwdebug_check(3, "PUDISKU_DEBUG"):
        miscutils.fwdebug_print(f"usum = {usum}")
    return usum
