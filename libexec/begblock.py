#!/usr/bin/env python3
# $Id: begblock.py 47308 2018-07-31 19:42:07Z friedel $
# $Rev:: 47308                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-07-31 14:42:07 #$:  # Date of last commit.

""" Program run at beginning of block that performs job setup """

import sys
import os
import collections

import despymisc.miscutils as miscutils
import intgutils.intgdefs as intgdefs
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwutils as pfwutils
from processingfw.runqueries import runqueries
import processingfw.pfwblock as pfwblock
import processingfw.pfwdb as pfwdb


def begblock(argv):
    """ Program entry point """
    if argv is None:
        argv = sys.argv[1:]

    configfile = argv[0]
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    config.set_block_info()
    blknum = config[pfwdefs.PF_BLKNUM]

    blkdir = config.getfull('block_dir')
    os.chdir(blkdir)


    (exists, submit_des_services) = config.search('submit_des_services')
    if exists and submit_des_services is not None:
        os.environ['DES_SERVICES'] = submit_des_services
    (exists, submit_des_db_section) = config.search('submit_des_db_section')
    if exists and submit_des_db_section is not None:
        os.environ['DES_DB_SECTION'] = submit_des_db_section

    dbh = None
    doMirror = False
    blktid = -1
    if miscutils.fwdebug_check(3, 'PFWBLOCK_DEBUG'):
        miscutils.fwdebug_print(f"blknum = {config[pfwdefs.PF_BLKNUM]}")
    if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
        if config.dbh is None:
            dbh = pfwdb.PFWDB(submit_des_services, submit_des_db_section)
        else:
            dbh = config.dbh
        if config.get(pfwdefs.SQLITE_FILE) is not None:
            doMirror = True
            dbh.activateMirror(config)
        dbh.insert_block(config)
        blktid = config['task_id']['block'][str(blknum)]
        config['task_id']['begblock'] = dbh.create_task(name='begblock',
                                                        info_table=None,
                                                        parent_task_id=blktid,
                                                        root_task_id=int(config['task_id']['attempt']),
                                                        label=None,
                                                        do_begin=True,
                                                        do_commit=True)
        if doMirror:
            dbh.mirror.basic_insert_row('task', {'name': 'begblock',
                                                 'info_table': None,
                                                 'parent_task_id': blktid,
                                                 'root_task_id': int(config['task_id']['attempt']),
                                                 'label': None,
                                                 'id': config['task_id']['begblock']})
            dbh.mirror.begin_task(config['task_id']['begblock'])

    try:
        modulelist = miscutils.fwsplit(config.getfull(pfwdefs.SW_MODULELIST).lower())
        modules_prev_in_list = {}

        joblist = {}
        parlist = collections.OrderedDict()
        masterdata = collections.OrderedDict()
        filelist = {'infiles' : {},
                    'outfiles': {}}
        maxthread = 1
        for num, modname in enumerate(modulelist):
            print(f"XXXXXXXXXXXXXXXXXXXX {modname} XXXXXXXXXXXXXXXXXXXX")
            if modname not in config[pfwdefs.SW_MODULESECT]:
                miscutils.fwdie(f"Error: Could not find module description for module {modname}\n",
                                pfwdefs.PF_EXIT_FAILURE)
            moddict = config[pfwdefs.SW_MODULESECT][modname]

            runqueries(config, configfile, modname, modules_prev_in_list, dbh)
            pfwblock.read_master_lists(config, modname, masterdata, modules_prev_in_list)

            (infsect, outfsect) = pfwblock.get_datasect_types(config, modname)
            pfwblock.fix_master_lists(config, modname, masterdata, outfsect)

            if pfwdefs.PF_NOOP not in moddict or not miscutils.convertBool(moddict[pfwdefs.PF_NOOP]):
                pfwblock.create_fullnames(config, modname, masterdata)
                if miscutils.fwdebug_check(9, 'PFWBLOCK_DEBUG') and modname in masterdata:
                    with open(f"{modname}-masterdata.txt", 'w') as fh:
                        miscutils.pretty_print_dict(masterdata[modname], fh)

                pfwblock.add_file_metadata(config, modname, connect=dbh)
                sublists = pfwblock.create_sublists(config, modname, masterdata)
                if sublists is not None:
                    if miscutils.fwdebug_check(3, 'PFWBLOCK_DEBUG'):
                        miscutils.fwdebug_print(f"sublists.keys() = {list(sublists.keys())}")
                loopvals = pfwblock.get_wrapper_loopvals(config, modname)
                wrapinst = pfwblock.create_wrapper_inst(config, modname, loopvals)
                wcnt = 1
                for winst in wrapinst.values():
                    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
                        miscutils.fwdebug_print(f"winst {wcnt:d} - BEG")
                    pfwblock.assign_data_wrapper_inst(config, modname, winst, masterdata,
                                                      sublists, infsect, outfsect)
                    pfwblock.finish_wrapper_inst(config, modname, winst, outfsect)
                    tempfiles = pfwblock.create_module_wrapper_wcl(config, modname, winst)
                    for fl in tempfiles['infiles']:
                        if fl not in filelist['infiles']:
                            filelist['infiles'][fl] = num

                    for fl in tempfiles['outfiles']:
                        filelist['outfiles'][fl] = num
                    #filelist['infiles'] += tempfiles['infiles']
                    #filelist['outfiles'] += tempfiles['outfiles']
                    maxthread = max(pfwblock.divide_into_jobs(config, modname, winst, joblist, parlist), maxthread)
                    wcnt += 1
            modules_prev_in_list[modname] = True

            if miscutils.fwdebug_check(9, 'PFWBLOCK_DEBUG') and modname in masterdata:
                with open(f"{modname}-masterdata.txt", 'w') as fh:
                    miscutils.pretty_print_dict(masterdata[modname], fh)
        config['maxthread_used'] = maxthread
        scriptfile = pfwblock.write_runjob_script(config)

        intersect = list(set(filelist['infiles'].keys()) & set(filelist['outfiles'].keys()))
        finallist = []

        for fl in filelist['infiles']:
            if fl not in intersect:
                finallist.append(fl)
            else:
                if filelist['infiles'][fl] <= filelist['outfiles'][fl]:
                    raise Exception(f"Input file {fl} requested before it is generated.")

        if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
            missingfiles = dbh.check_files(config, finallist)
            if missingfiles:
                raise Exception("The following input files cannot be found in the archive:" + ",".join(missingfiles))

        miscutils.fwdebug_print("Creating job files - BEG")
        parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION

        for jobkey, jobdict in sorted(joblist.items()):
            jobdict['jobnum'] = pfwutils.pad_jobnum(config.inc_jobnum())
            jobdict['jobkeys'] = jobkey
            jobdict['numexpwrap'] = len(jobdict['tasks'])
            if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
                miscutils.fwdebug_print(f"jobnum = {jobdict['jobnum']}, jobkey = {jobkey}:")
            jobdict['tasksfile'] = write_workflow_taskfile(config, jobdict['jobnum'],
                                                           jobdict['tasks'])
            if (jobdict['inlist'] and
                    config.getfull(pfwdefs.USE_HOME_ARCHIVE_OUTPUT) != 'never' and
                    'submit_files_mvmt' in config and
                    (pfwdefs.PF_DRYRUN not in config or
                     not miscutils.convertBool(config.getfull(pfwdefs.PF_DRYRUN)))):
                # get home archive info
                home_archive = config.getfull('home_archive')
                archive_info = config[pfwdefs.SW_ARCHIVESECT][home_archive]
                archive_info['connection'] = dbh
                # load filemgmt class
                attempt_tid = config['task_id']['attempt']
                filemgmt = pfwutils.pfw_dynam_load_class(dbh, config,
                                                         attempt_tid, attempt_tid,
                                                         "filemgmt", archive_info['filemgmt'],
                                                         archive_info)
                # save file information
                filemgmt.register_file_data('list', jobdict['inlist'], config['pfw_attempt_id'], attempt_tid, False, None, None)
                if doMirror:
                    (_, fname, _) = miscutils.parse_fullname(jobdict['inlist'], parsemask)
                    finallist.append(fname)
                pfwblock.copy_input_lists_home_archive(config, filemgmt,
                                                       archive_info, jobdict['inlist'])
                filemgmt.commit()
            jobdict['inputwcltar'] = pfwblock.tar_inputfiles(config, jobdict['jobnum'],
                                                             jobdict['inwcl'] + jobdict['inlist'])
            if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
                dbh.insert_job(config, jobdict)
            pfwblock.write_jobwcl(config, jobkey, jobdict)
            if ('glidein_use_wall' in config and
                    miscutils.convertBool(config.getfull('glidein_use_wall')) and
                    'jobwalltime' in config):
                jobdict['wall'] = config['jobwalltime']

        if doMirror:
            dbh.updateMirrorFiles(finallist)

        miscutils.fwdebug_print("Creating job files - END")

        numjobs = len(joblist)
        if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
            dbh.update_block_numexpjobs(config, numjobs)

        dagfile = config.get_filename('jobdag')
        pfwblock.create_jobmngr_dag(config, dagfile, scriptfile, joblist)
    except:
        retval = pfwdefs.PF_EXIT_FAILURE
        with open(configfile, 'w') as cfgfh:
            config.write(cfgfh)   # save config, have updated jobnum, wrapnum, etc
        if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
            dbh.end_task(config['task_id']['begblock'], retval, True)
            dbh.end_task(blktid, retval, True)
            if doMirror:
                dbh.mirror.end_task(config['task_id']['begblock'], retval, True)
                dbh.mirror.end_task(blktid, retval, True)
        raise

    # save config, have updated jobnum, wrapnum, etc
    with open(configfile, 'w') as cfgfh:
        config.write(cfgfh)

    (exists, dryrun) = config.search(pfwdefs.PF_DRYRUN)
    if exists and miscutils.convertBool(dryrun):
        retval = pfwdefs.PF_EXIT_DRYRUN
    else:
        retval = pfwdefs.PF_EXIT_SUCCESS
    if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
        dbh.end_task(config['task_id']['begblock'], retval, True)
        if doMirror:
            dbh.mirror.end_task(config['task_id']['begblock'], retval, True)
            dbh.mirror.commit()
            dbh.mirror.close()
    miscutils.fwdebug_print(f"END - exiting with code {retval}")

    return retval


def write_workflow_taskfile(config, jobnum, tasks):
    """ Write the list of wrapper executions for a single job to a file """
    taskfile = config.get_filename('jobtasklist', {pfwdefs.PF_CURRVALS:{'jobnum':jobnum},
                                                   'required': True,
                                                   intgdefs.REPLACE_VARS: True})
    tjpad = pfwutils.pad_jobnum(jobnum)
    miscutils.coremakedirs(tjpad)
    with open(f"{tjpad}/{taskfile}", 'w') as tasksfh:
        for task in sorted(tasks, key=lambda singletask: int(singletask[0])):
            tasksfh.write(f"{task[0]}, {task[1]}, {task[2]}, {task[3]}, {task[4]}\n")
    return taskfile



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: begblock.py configfile")
        sys.exit(pfwdefs.PF_EXIT_FAILURE)

    print(' '.join(sys.argv))    # print command so can run by hand if needed
    sys.stdout.flush()
    sys.exit(begblock(sys.argv[1:]))
