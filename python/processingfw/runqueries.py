#!/usr/bin/env python3
# $Id: runqueries.py 47308 2018-07-31 19:42:07Z friedel $
# $Rev:: 47308                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-07-31 14:42:07 #$:  # Date of last commit.

# pylint: disable=print-statement

""" Run queries for a block to determine input files """

import sys
import os
import time
import traceback

import despymisc.miscutils as miscutils
import intgutils.intgdefs as intgdefs
import intgutils.replace_funcs as replfuncs
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb
from processingfw.pfwlog import log_pfw_event

###########################################################
def create_master_list(config, configfile, modname, moddict,
                       search_name, search_dict, search_type,
                       connection=None):
    """ Create master data list for a module's list or file def """
    miscutils.fwdebug_print("BEG")

    if 'qouttype' in search_dict:
        qouttype = search_dict['qouttype']
    else:
        qouttype = intgdefs.DEFAULT_QUERY_OUTPUT_FORMAT

    qoutfile = config.get_filename('qoutput',
                                   {pfwdefs.PF_CURRVALS: {'modulename': modname,
                                                          'searchname': search_name,
                                                          'suffix': qouttype}})
    qlog = config.get_filename('qoutput',
                               {pfwdefs.PF_CURRVALS:{'modulename': modname,
                                                     'searchname': search_name,
                                                     'suffix': 'out'}})

    prog = None
    if 'exec' in search_dict:
        prog = search_dict['exec']
        if 'args' not in search_dict:
            print(f"\t\tWarning:  {search_name} in module {modname} does not have args defined\n")
            args = ""
        else:
            args = search_dict['args']
    elif 'query_fields' in search_dict:
        if 'processingfw_dir' in config:
            dirgenquery = config['processingfw_dir']
        elif 'PROCESSINGFW_DIR' in os.environ:
            dirgenquery = os.environ['PROCESSINGFW_DIR']
        else:
            miscutils.fwdie("Error: Could not determine base path for genquerydb.py",
                            pfwdefs.PF_EXIT_FAILURE)

        prog = f"{dirgenquery}/libexec/genquerydb.py"
        args = f"--qoutfile {qoutfile} --qouttype {qouttype} --config {configfile} --module {modname} --search {search_name}"

    if not prog:
        print(f"\tWarning: {search_name} in module {modname} does not have exec or {pfwdefs.SW_QUERYFIELDS} defined")
        return

    search_dict['qoutfile'] = qoutfile
    search_dict['qlog'] = qlog

    prog = replfuncs.replace_vars_single(prog, config,
                                         {pfwdefs.PF_CURRVALS: {pfwdefs.SW_MODULESECT: modname},
                                          'searchobj': search_dict})

    # handle both outputxml and outputfile args
    args = replfuncs.replace_vars_single(args, config,
                                         {pfwdefs.PF_CURRVALS:{pfwdefs.SW_MODULESECT:modname,
                                                               'outputxml':qoutfile,
                                                               'outputfile':qoutfile,
                                                               'qoutfile':qoutfile},
                                          #intgdefs.REPLACE_VARS: True,
                                          'searchobj':search_dict})

    # get version for query code
    query_version = None
    if prog in config[pfwdefs.SW_EXEC_DEF]:
        query_version = pfwutils.get_version(prog, config[pfwdefs.SW_EXEC_DEF])

    if search_type == pfwdefs.SW_LISTSECT:
        datatype = 'L'
    elif search_type == pfwdefs.SW_FILESECT:
        datatype = 'F'
    else:
        datatype = search_type[0].upper()

    # call code
    query_tid = None
    if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
        if config.dbh is not None:
            pfw_dbh = config.dbh
        elif connection is not None:
            pfw_dbh = connection
        else:
            pfw_dbh = pfwdb.PFWDB()

        query_tid = pfw_dbh.insert_data_query(config, modname, datatype, search_name,
                                              prog, args, query_version)
    else:
        pfw_dbh = None

    cwd = os.getcwd()
    print(f"\t\tCalling code to create master list for obj {search_name} in module {modname}")
    print("\t\t", prog, args)
    print(f"\t\tSee output in {cwd}/{qlog}")
    print(f"\t\tSee master list will be in {cwd}/{qoutfile}")

    print("\t\tCreating master list - start ", time.time())

    cmd = f"{prog} {args}"
    exitcode = None
    try:
        exitcode = pfwutils.run_cmd_qcf(cmd, qlog, query_tid, os.path.basename(prog),
                                        config.getfull(pfwdefs.PF_USE_QCF), pfw_dbh,
                                        config['pfw_attempt_id'])
    except:
        print("******************************")
        print("Error: ")
        (extype, exvalue, trback) = sys.exc_info()
        print("******************************")
        traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
        exitcode = pfwdefs.PF_EXIT_FAILURE

    print("\t\tCreating master list - end ", time.time())
    sys.stdout.flush()
    if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
        pfw_dbh.end_task(query_tid, exitcode, True)
        if config.dbh is None and connection is None:
            pfw_dbh.close()

    if exitcode != 0:
        miscutils.fwdie(f"Error: problem creating master list (exitcode = {exitcode})",
                        exitcode)

    miscutils.fwdebug_print("END")

def runqueries(config, configfile, modname, modules_prev_in_list, connection=None):
    """ Run any queries for a particular module """
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    # process each "list" in each module
    if pfwdefs.SW_LISTSECT in moddict:
        uber_list_dict = moddict[pfwdefs.SW_LISTSECT]
        if 'list_order' in moddict:
            listorder = miscutils.fwsplit(moddict['list_order'].lower())
        else:
            listorder = list(uber_list_dict.keys())

        for listname in listorder:
            list_dict = uber_list_dict[listname]
            if 'depends' not in list_dict or \
                list_dict['depends'] not in modules_prev_in_list:
                print(f"\t{modname}-{listname}: creating master list\n")
                create_master_list(config, configfile, modname,
                                   moddict, listname, list_dict, pfwdefs.SW_LISTSECT, connection)

    # process each "file" in each module
    if pfwdefs.SW_FILESECT in moddict:
        for filename, file_dict in moddict[pfwdefs.SW_FILESECT].items():
            if 'depends' not in file_dict or \
                not file_dict['depends'] not in modules_prev_in_list:
                print(f"\t{modname}-{filename}: creating master list\n")
                create_master_list(config, configfile, modname,
                                   moddict, filename, file_dict, pfwdefs.SW_FILESECT, connection)

def main(argv=None):
    """ Program entry point """
    if argv is None:
        argv = sys.argv

    if len(argv) != 3:
        miscutils.fwdie("Usage: runqueries.pl configfile condorjobid\n", pfwdefs.PF_EXIT_FAILURE)

    configfile = argv[1]
    condorid = argv[2]

    config = pfwconfig.PfwConfig({'wclfile': configfile})
    # log condor jobid
    log_pfw_event(config, config['curr_block'], 'runqueries', 'j', ['cid', condorid])

    if pfwdefs.SW_MODULELIST not in config:
        miscutils.fwdie("Error:  No modules to run.", pfwdefs.PF_EXIT_FAILURE)

    ### Get master lists and files calling external codes when needed

    modulelist = miscutils.fwsplit(config[pfwdefs.SW_MODULELIST].lower())

    modules_prev_in_list = {}
    for modname in modulelist:
        if modname not in config[pfwdefs.SW_MODULESECT]:
            miscutils.fwdie(f"Error: Could not find module description for module {modname}\n",
                            pfwdefs.PF_EXIT_FAILURE)
        runqueries(config, configfile, modname, modules_prev_in_list)
        modules_prev_in_list[modname] = True

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
