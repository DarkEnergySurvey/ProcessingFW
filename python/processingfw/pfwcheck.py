# $Id: pfwcheck.py 47308 2018-07-31 19:42:07Z friedel $
# $Rev:: 47308                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-07-31 14:42:07 #$:  # Date of last commit.

# pylint: disable=print-statement

""" Contains functions used to check submit wcl for missing or invalid values """

import sys
import traceback

import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils
import intgutils.intgmisc as intgmisc
import intgutils.intgdefs as intgdefs
import despymisc.miscutils as miscutils
import filemgmt.filemgmt_defs as fmdefs

NUMCNTS = 4
ERRCNT_POS = 0
WARNCNT_POS = 1
CHANGECNT_POS = 2
CLEANCNT_POS = 3

def warning(indent, message):
    """ Method to print a warning message

    """
    print(f"{indent}Warning: {message}")

def error(indent, message):
    """ Method to print an error

    """
    print(f"{indent}Error: {message}")

###########################################################################
def check_globals(config, indent=''):
    """ Check global settings """

    print(f"{indent}Checking globals...")

    # initialize counters
    cnts = [0] * NUMCNTS

    # always required
    # TODO: unitname might need to be expanded to discover missing variables ???
    for key in ['pipeline', 'pipeprod', 'pipever', 'project',
                pfwdefs.REQNUM, pfwdefs.ATTNUM, pfwdefs.UNITNAME,
                'jira_id', 'target_site', pfwdefs.SW_SITESECT,
                'filename_pattern', 'directory_pattern',
                'job_file_mvmt', pfwdefs.ATTEMPT_ARCHIVE_PATH,
                pfwdefs.PF_USE_QCF, pfwdefs.PF_USE_DB_IN, pfwdefs.PF_USE_DB_OUT,
                pfwdefs.SW_BLOCKLIST, pfwdefs.SW_BLOCKSECT, pfwdefs.SW_MODULESECT,
                'create_junk_tarball', 'campaign']:
        try:
            if key not in config:
                error(indent + '    ', f"missing {key} global key or section")
                cnts[ERRCNT_POS] += 1
        except:
            error(indent + '    ', f"missing {key} global key or section")
            cnts[ERRCNT_POS] += 1


    for key in [pfwdefs.PF_USE_DB_IN, pfwdefs.PF_USE_DB_OUT]:
        if key in config:
            if miscutils.convertBool(config.getfull(key)):
                if 'submit_des_db_section' not in config:
                    error(indent + '    ', f"using DB ({key}), but missing submit_des_db_section")
                    cnts[ERRCNT_POS] += 1
                if 'submit_des_services' not in config:
                    error(indent + '    ', f"using DB ({key}), but missing submit_des_services")
                    cnts[ERRCNT_POS] += 1


    # if using QCF must also be writing run info into DB
    if (pfwdefs.PF_USE_QCF in config and
        miscutils.convertBool(config.getfull(pfwdefs.PF_USE_QCF)) and
        (pfwdefs.PF_USE_DB_OUT in config and
         not miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)))):
        error(indent + '    ', f"if {pfwdefs.PF_USE_QCF} is true, {pfwdefs.PF_USE_DB_OUT} must also be set to true")
        cnts[ERRCNT_POS] += 1

    if 'operator' not in config:
        error(indent + '    ', 'Must specify operator')
        cnts[ERRCNT_POS] += 1
    elif config.getfull('operator') in ['bcs']:
        error(indent + '    ', f"Operator cannot be shared login ({config.getfull('operator')}).")
        cnts[ERRCNT_POS] += 1

    print(f"{indent}    Checking {pfwdefs.SW_SAVE_RUN_VALS}...")
    if pfwdefs.SW_SAVE_RUN_VALS in config:
        keys2save = config.getfull(pfwdefs.SW_SAVE_RUN_VALS)
        keys = miscutils.fwsplit(keys2save, ',')
        for key in keys:
            exists = False
            try:
                (exists, _) = config.search(key, {intgdefs.REPLACE_VARS: True, 'expand': True})
            except SystemExit:
                pass

            if not exists:
                error(indent + '        ', f"Cannot determine {pfwdefs.SW_SAVE_RUN_VALS} value ({key}).")
                cnts[ERRCNT_POS] += 1


    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(), ',')
    for blockname in blocklist:
        if blockname not in config[pfwdefs.SW_BLOCKSECT]:
            error(indent + '    ', f" Invalid {pfwdefs.SW_BLOCKLIST}, bad block name ({blockname})")
            cnts[ERRCNT_POS] += 1

    return cnts



###########################################################################
def check_block(config, indent=''):
    """ check blocks level defs """

    cnts = [0] * NUMCNTS

    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(), ',')
    for blockname in blocklist:
        print(f"{indent}Checking block {blockname}...")
        config.set_block_info()

        for key in [pfwdefs.PF_USE_DB_IN, pfwdefs.PF_USE_DB_OUT]:
            if key in config and miscutils.convertBool(config.getfull(key)):
                (found, _) = config.search('target_des_db_section')
                if not found:
                    error(indent + '    ', f"using DB ({key}), but missing target_des_db_section")
                    cnts[ERRCNT_POS] += 1

                (found, _) = config.search('target_des_services')
                if not found:
                    error(indent + '    ', f"using DB ({key}), but missing target_des_services")
                    cnts[ERRCNT_POS] += 1

        # check modules
        block = config[pfwdefs.SW_BLOCKSECT][blockname]
        if pfwdefs.SW_MODULELIST in block:
            modulelist = miscutils.fwsplit(block[pfwdefs.SW_MODULELIST].lower(), ',')

            for modname in modulelist:
                if modname not in config[pfwdefs.SW_MODULESECT]:
                    error(indent + '    ', f"block {blockname} - invalid {pfwdefs.SW_MODULELIST}")
                    print(f"{indent}        (bad module name: {modname}, list: {modulelist})")
                    cnts[ERRCNT_POS] += 1
                else:
                    cnts2 = check_module(config, blockname, modname, indent + '    ')
                    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

        else:
            error(indent + '    ', f"block {blockname} - missing {pfwdefs.SW_MODULESECT} value")
            cnts[ERRCNT_POS] += 1

        config.inc_blknum()

    config.reset_blknum()

    return cnts



###########################################################################
def check_target_archive(config, indent=''):
    """ check info related to target archive """

    cnts = [0] * NUMCNTS

    print(f"{indent}Checking target archive...")
    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(), ',')
    for blockname in blocklist:
        config.set_block_info()

        (found_input, use_target_archive_input) = config.search(pfwdefs.USE_TARGET_ARCHIVE_INPUT,
                                                                {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})
        (found_output, use_target_archive_output) = config.search(pfwdefs.USE_TARGET_ARCHIVE_OUTPUT, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})
        (found_archive, _) = config.search(pfwdefs.TARGET_ARCHIVE, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})

        if not found_input:
            error(indent + '    ', f"block {blockname} - Could not determine {pfwdefs.USE_TARGET_ARCHIVE_INPUT}")
            cnts[ERRCNT_POS] += 1
        elif use_target_archive_input.lower() not in pfwdefs.VALID_TARGET_ARCHIVE_INPUT:
            error(indent + '    ', f"block {blockname} - Invalid {pfwdefs.USE_TARGET_ARCHIVE_INPUT} value")
            cnts[ERRCNT_POS] += 1

        if not found_output:
            error(indent + '    ', f"block {blockname} - Could not determine {pfwdefs.USE_TARGET_ARCHIVE_OUTPUT}")
            cnts[ERRCNT_POS] += 1
        elif use_target_archive_output.lower() not in pfwdefs.VALID_TARGET_ARCHIVE_OUTPUT:
            error(indent + '    ', f"block {blockname} - Invalid {pfwdefs.USE_TARGET_ARCHIVE_OUTPUT} value")
            cnts[ERRCNT_POS] += 1

        # if need to use a target_archive for this block
        if (found_input and use_target_archive_input.lower() != 'never') or \
           (found_output and use_target_archive_output.lower() != 'never'):
            if not found_archive:
                error(indent + '    ', f"block {blockname} - Missing {pfwdefs.TARGET_ARCHIVE} value")
                cnts[ERRCNT_POS] += 1
            elif pfwdefs.SW_ARCHIVESECT not in config:
                error(indent + '    ', f"block {blockname} - Needs archive section which doesn't exist")
                cnts[ERRCNT_POS] += 1
            elif pfwdefs.TARGET_ARCHIVE not in config[pfwdefs.SW_ARCHIVESECT]:
                error(indent + '    ', f"block {blockname} - Invalid {pfwdefs.TARGET_ARCHIVE} value")
                cnts[ERRCNT_POS] += 1
            else:
                # check that we have all archive req values exist
                pass

        config.inc_blknum()

    config.reset_blknum()


    return cnts


###########################################################################
def check_home_archive(config, indent=''):
    """ check info related to home archive """

    cnts = [0] * NUMCNTS

    print(f"{indent}Checking home archive...")
    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(), ',')
    for blockname in blocklist:
        config.set_block_info()

        (found_input, use_home_archive_input) = config.search(pfwdefs.USE_HOME_ARCHIVE_INPUT, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})
        (found_output, use_home_archive_output) = config.search(pfwdefs.USE_HOME_ARCHIVE_OUTPUT, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})
        (found_archive, home_archive) = config.search(pfwdefs.HOME_ARCHIVE, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})

        if not found_input:
            error(indent + '    ', f"block {blockname} - Could not determine {pfwdefs.USE_HOME_ARCHIVE_INPUT}")
            cnts[ERRCNT_POS] += 1
        elif use_home_archive_input.lower() not in pfwdefs.VALID_HOME_ARCHIVE_INPUT:
            error(indent + '    ', f"block {blockname} - Invalid {pfwdefs.USE_HOME_ARCHIVE_INPUT} value")
            cnts[ERRCNT_POS] += 1

        if not found_output:
            error(indent + '    ', f"block {blockname} - Could not determine {pfwdefs.USE_HOME_ARCHIVE_OUTPUT}")
            cnts[ERRCNT_POS] += 1
        elif use_home_archive_output.lower() not in pfwdefs.VALID_HOME_ARCHIVE_OUTPUT:
            error(indent + '    ', f"block {blockname} - Invalid {pfwdefs.USE_HOME_ARCHIVE_OUTPUT} value")
            cnts[ERRCNT_POS] += 1

        # if need to use a home_archive for this block
        if ((found_input and use_home_archive_input.lower() != 'never') or
            (found_output and use_home_archive_output.lower() != 'never')):
            if not found_archive:
                error(indent + '    ', f"block {blockname} - Missing {pfwdefs.HOME_ARCHIVE} value")
                cnts[ERRCNT_POS] += 1
            elif pfwdefs.SW_ARCHIVESECT not in config:
                error(indent + '    ', f"block {blockname} - Needs archive section which doesn't exist")
                cnts[ERRCNT_POS] += 1
            elif home_archive not in config[pfwdefs.SW_ARCHIVESECT]:
                error(indent + '    ', f"block {blockname} - Invalid {pfwdefs.HOME_ARCHIVE} value")
                cnts[ERRCNT_POS] += 1
            else:
                # check that we have all archive req values exist
                pass

        config.inc_blknum()

    config.reset_blknum()

    return cnts


###########################################################################
def check_module(config, blockname, modname, indent=''):
    """ Check module """

    cnts = [0] * NUMCNTS

    print(f"{indent}Checking module {modname}...")
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    dataobjs = {pfwdefs.SW_INPUTS: {}, pfwdefs.SW_OUTPUTS: {}}

    # check that have wrappername (required)
    if pfwdefs.SW_WRAPPERNAME not in moddict and \
            not miscutils.convertBool(moddict[pfwdefs.PF_NOOP]):
        error(indent + '    ', f"block {blockname}, module {modname} - missing {pfwdefs.SW_WRAPPERNAME} value")
        cnts[ERRCNT_POS] += 1

    # check that have at least 1 exec section (required)
    execsects = intgmisc.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)
    if not execsects and not miscutils.convertBool(moddict[pfwdefs.PF_NOOP]):
        error(indent + '    ', f"block {blockname}, module {modname} - 0 exec sections ({pfwdefs.SW_EXECPREFIX}*)")
        cnts[ERRCNT_POS] += 1
    else:
        # check exec sections
        for xsectname in execsects:
            xsectdict = moddict[xsectname]
            cnts2 = check_exec(config, blockname, modname, dataobjs, xsectname, xsectdict, indent + "    ")
            cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    # check file/list sections
    cnts2 = check_dataobjs(config, blockname, modname, moddict, dataobjs, indent + "    ")
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    return cnts



###########################################################################
def parse_wcl_objname(objname):
    """ Parse WCL object name into parts """

    sect = name = subname = None

    parts = miscutils.fwsplit(objname, '.')
    #print 'parts=', parts
    if len(parts) == 3:    # lists have 3 parts
        (sect, name, subname) = parts
    elif len(parts) == 2:  # files have 2 parts
        (sect, name) = parts
    elif len(parts) == 1:
        name = parts[0]
    else:
        error('', f"cannot parse objname {objname} (too many sections/periods)")

    return sect, name, subname


###########################################################################
def check_filepat_valid(config, filepat, blockname, modname, objname, objdict, indent=''):
    """ Check if given file pattern is valid """

    cnts = [0] * NUMCNTS

    if pfwdefs.SW_FILEPATSECT not in config:
        error(indent, f"Missing filename pattern definition section ({pfwdefs.SW_FILEPATSECT})")
        cnts[ERRCNT_POS] += 1
    elif filepat not in config[pfwdefs.SW_FILEPATSECT]:
        error(indent, f"block {blockname}, module {modname}, {objname} - Missing definition for {pfwdefs.SW_FILEPAT} '{filepat}'")
        cnts[ERRCNT_POS] += 1

    # todo: if pattern, check that all needed values exist

    return cnts


###########################################################################
def check_file_valid_input(config, blockname, modname, fname, fdict, indent=''):
    """ Check if given input file is valid """

    cnts = [0] * NUMCNTS

    # check that any given filename pattern has a definition
    if pfwdefs.SW_FILEPAT in fdict:
        cnts2 = check_filepat_valid(config, fdict[pfwdefs.SW_FILEPAT], blockname, modname, fname, fdict, indent + '    ')
        cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    # check that it has filepat, filename, depends, or query wcl (required)
    # if filename is a pattern, can I check that all needed values exist?
    # todo check depends happens in same block previous to this module
    if (('listonly' not in fdict or not miscutils.convertBool(fdict['listonly'])) and
        pfwdefs.SW_FILEPAT not in fdict and pfwdefs.FILENAME not in fdict and
        'fullname' not in fdict and 'query_fields' not in fdict and
        pfwdefs.DATA_DEPENDS not in fdict):
        error(indent, f"block {blockname}, module {modname}, {pfwdefs.SW_INPUTS}, {fname} - Missing terms needed to determine input filename")
        cnts[ERRCNT_POS] += 1

    # check that it has pfwdefs.DIRPAT :    err
    # can I check that all values for pfwdefs.DIRPAT exist?
    if pfwdefs.DIRPAT not in fdict:
        error(indent, f"block {blockname}, module {modname}, {pfwdefs.SW_INPUTS}, {fname} - Missing {pfwdefs.DIRPAT}")
        cnts[ERRCNT_POS] += 1

    return cnts


###########################################################################
def check_list_valid_input(config, blockname, modname, objname, objdict, indent=''):
    """ Check if input list is valid """

    cnts = [0] * NUMCNTS

    (_, _, _) = parse_wcl_objname(objname)

    # how to name list
    if pfwdefs.SW_FILEPAT not in objdict and pfwdefs.FILENAME not in objdict:
        error(indent, f"block {blockname}, module {modname}, {pfwdefs.SW_INPUTS}, {objname} - Missing terms needed to determine list filename")
        cnts[ERRCNT_POS] += 1

    # directory location for list
    if pfwdefs.DIRPAT not in objdict:
        error(indent, f"block {blockname}, module {modname}, {pfwdefs.SW_INPUTS}, {objname} - Missing {pfwdefs.DIRPAT}")
        cnts[ERRCNT_POS] += 1

    # what goes into the list
    if pfwdefs.DIV_LIST_BY_COL not in objdict and 'columns' not in objdict:
        error(indent, f"block {blockname}, module {modname}, {pfwdefs.SW_INPUTS}, {objname} - Missing terms needed to determine column(s) in list(s) ({pfwdefs.DIV_LIST_BY_COL} or {'columns'})")
        cnts[ERRCNT_POS] += 1

    return cnts


###########################################################################
def check_exec_inputs(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    """ Check exec input definition is valid """

    cnts = [0] * NUMCNTS
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    if pfwdefs.SW_INPUTS in xsectdict:
        print(f"{indent}Checking {xsectname} {pfwdefs.SW_INPUTS}...")
        indent += '    '
        #print "%sxsectdict[pfwdefs.SW_INPUTS] = %s" % (indent, xsectdict[pfwdefs.SW_INPUTS])
        # for each entry in inputs
        for objname in miscutils.fwsplit(xsectdict[pfwdefs.SW_INPUTS], ','):
            objname = objname.lower()

            (sect, name, subname) = parse_wcl_objname(objname)

            if sect is None:
                error(indent + '    ', f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_INPUTS} - Invalid entry ({objname}).  Missing section label")
                cnts[ERRCNT_POS] += 1
            else:
                # check that appears in [file/list]sect : error
                bad = False
                if sect not in moddict or name not in moddict[sect]:
                    found = False
                    if 'loopobj' in moddict and moddict['loopobj'].startswith(sect) and sect in moddict:
                        temp = moddict['loopobj'].split('.')[1:]
                        d = moddict[sect]
                        for t in temp:
                            if t in d:
                                d = d[t]
                        if name in d:
                            found = True
                        else:
                            if 'div_list_by_col' in d:
                                if name in d['div_list_by_col']:
                                    found = True
                                    moddict[sect][name] = d['div_list_by_col'][name]

                    if not found:
                        bad = True
                        error(indent + '    ', f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_INPUTS} - Invalid entry ({objname}).  Cannot find definition.")
                        cnts[ERRCNT_POS] += 1

                if not bad:
                    if subname is None:  # file
                        dataobjs[pfwdefs.SW_INPUTS][objname] = True
                    elif sect != pfwdefs.SW_LISTSECT:   # only lists can have subname
                        error(indent + '    ', f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_INPUTS}, {objname} - Too many sections/periods for a {sect}.")
                        cnts[ERRCNT_POS] += 1
                    elif subname not in moddict[pfwdefs.SW_FILESECT]:
                        error(indent + '    ', f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_INPUTS}, {objname} - Cannot find definition for {subname}")
                        cnts[ERRCNT_POS] += 1
                    else:
                        dataobjs[pfwdefs.SW_INPUTS][f"{pfwdefs.SW_LISTSECT}.{name}"] = True
                        dataobjs[pfwdefs.SW_INPUTS][f"{pfwdefs.SW_FILESECT}.{subname}"] = True
                        dataobjs[pfwdefs.SW_INPUTS][objname] = True
                        fdict = moddict[pfwdefs.SW_FILESECT][subname]
                        if ('listonly' not in fdict or not miscutils.convertBool(fdict['listonly'])):
                            warning(indent, f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_INPUTS}, {objname} - File in list does not have listonly=True")
                            cnts[WARNCNT_POS] += 1

    return cnts


###########################################################################
def check_file_valid_output(config, blockname, modname, fname, fdict, indent=''):
    """ Check if output file definition is valid """

    cnts = [0] * NUMCNTS

    msginfo = f"block {blockname}, module {modname}, {pfwdefs.SW_OUTPUTS} {fname}"

    # check that it has pfwdefs.DIRPAT :    err
    # can I check that all values for pfwdefs.DIRPAT exist?
    if pfwdefs.DIRPAT not in fdict:
        error(indent, f"{msginfo} - Missing {pfwdefs.DIRPAT}")
        cnts[ERRCNT_POS] += 1
    else:
        # todo: check that all values for pfwdefs.DIRPAT exist
        pass

    # check that it has filepat, filename (required)
    if pfwdefs.SW_FILEPAT not in fdict and \
       pfwdefs.FILENAME not in fdict and \
       'fullname' not in fdict:
        error(indent, f"{msginfo} - Missing terms needed to determine output filename")
        cnts[ERRCNT_POS] += 1
    else:

        # check that any given filename pattern has a definition
        if pfwdefs.SW_FILEPAT in fdict:
            cnts2 = check_filepat_valid(config, fdict[pfwdefs.SW_FILEPAT],
                                        blockname, modname, fname, fdict, indent + '    ')
            cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    # check that it has filetype :    err
    if pfwdefs.FILETYPE not in fdict:
        error(indent, f"{msginfo} - Missing {pfwdefs.FILETYPE}")
        cnts[ERRCNT_POS] += 1
    elif fdict[pfwdefs.FILETYPE] not in config[fmdefs.FILETYPE_METADATA]:
        error(indent, f"{msginfo} - Invalid {pfwdefs.FILETYPE} ({fdict[pfwdefs.FILETYPE]})")
        cnts[ERRCNT_POS] += 1

    return cnts


###########################################################################
def check_exec_outputs(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    """ Check if exec output definition is valid """

    # initialize
    cnts = [0] * NUMCNTS
    moddict = config[pfwdefs.SW_MODULESECT][modname]


    if pfwdefs.SW_OUTPUTS in xsectdict:
        # for each entry in inputs
        print(f"{indent}Checking {xsectname}, {pfwdefs.SW_OUTPUTS}...")
        indent += '    '
        #print "%sxsectdict[pfwdefs.SW_OUTPUTS] = %s" % (indent, xsectdict[pfwdefs.SW_OUTPUTS])
        for objname in miscutils.fwsplit(xsectdict[pfwdefs.SW_OUTPUTS], ','):
            objname = objname.lower()
            #print '%sobjname=%s' % (indent, objname)

            (sect, name, _) = parse_wcl_objname(objname)
            #print '%s(sect, name, subname) = (%s, %s, %s)' % (indent, sect, name, subname)
            if sect is None:
                error(indent + '    ', f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_OUTPUTS} - Invalid entry {objname}.  Missing section label")
                cnts[ERRCNT_POS] += 1
            else:
                # check that appears in [file/list]sect : err
                if sect not in moddict or name not in moddict[sect]:
                    error(indent + '    ', f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_OUTPUTS} - Invalid entry {objname}.  Cannot find definition.")
                    cnts[ERRCNT_POS] += 1
                else:
                    dataobjs[pfwdefs.SW_OUTPUTS][objname] = True

    return cnts




###########################################################################
def check_exec_parentchild(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    """ Check that parent and children appear in inputs and outputs """
    # assumes check_exec_input and check_exec_output have already been executed so there are entries in dataobjs

    cnts = [0] * NUMCNTS
    if pfwdefs.SW_PARENTCHILD in xsectdict:
        print(f"{indent}Checking {xsectname} {pfwdefs.SW_PARENTCHILD}...")
        indent += '    '
        #print "%sxsectdict[pfwdefs.SW_PARENTCHILD] = %s" % (indent, xsectdict[pfwdefs.SW_PARENTCHILD])
        #print "%sdataobjs[pfwdefs.SW_INPUTS] = %s" % (indent, dataobjs[pfwdefs.SW_INPUTS])
        #print "%sdataobjs[pfwdefs.SW_OUTPUTS] = %s" % (indent, dataobjs[pfwdefs.SW_OUTPUTS])
        #print "%sfsplit = %s" % (indent, miscutils.fwsplit(xsectdict[pfwdefs.SW_PARENTCHILD], ',') )

        msginfo = f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_PARENTCHILD}"
        for pair in miscutils.fwsplit(xsectdict[pfwdefs.SW_PARENTCHILD], ','):
            pair = pair.lower()
            if ':' in pair:
                (parent, child) = miscutils.fwsplit(pair, ':')
                if '.' in parent:
                    if parent not in dataobjs[pfwdefs.SW_INPUTS]:
                        error(indent, f"{msginfo} - parent {parent} not listed in {pfwdefs.SW_INPUTS}")
                        cnts[ERRCNT_POS] += 1
                else:
                    error(indent, f"{msginfo} - parent {parent} missing section label")

                    cnts[ERRCNT_POS] += 1

                if '.' in child:
                    if child not in dataobjs[pfwdefs.SW_OUTPUTS]:
                        error(indent, f"{msginfo} - child {child} not listed in {pfwdefs.SW_OUTPUTS}")
                        cnts[ERRCNT_POS] += 1
                else:
                    error(indent, f"{msginfo} - child {child} missing section label")
                    cnts[ERRCNT_POS] += 1
            else:
                error(indent, f"{msginfo} - Invalid parent/child pair ({pair}).  Missing colon.")
                cnts[ERRCNT_POS] += 1
    elif pfwdefs.SW_INPUTS in xsectdict and pfwdefs.SW_OUTPUTS in xsectdict:
        msginfo = f"block {blockname}, module {modname}, {xsectname}"
        warning(indent, f"{msginfo} - has {pfwdefs.SW_INPUTS} and {pfwdefs.SW_OUTPUTS}, but not {pfwdefs.SW_PARENTCHILD}")
        cnts[WARNCNT_POS] += 1

    return cnts



###########################################################################
def check_dataobjs(config, blockname, modname, moddict, dataobjs, indent=''):
    """ calls functions to check files have all needed info as well as note extra file defs """

    cnts = [0] * NUMCNTS

    # check every file
    if pfwdefs.SW_FILESECT in moddict:
        print(f"{indent}Checking {pfwdefs.SW_FILESECT} section...")
        for fname, fdict in moddict[pfwdefs.SW_FILESECT].items():
            key = f"{pfwdefs.SW_FILESECT}.{fname}"
            if key not in dataobjs[pfwdefs.SW_INPUTS] and \
               key not in dataobjs[pfwdefs.SW_OUTPUTS] and \
               ('listonly' not in fdict or not miscutils.convertBool(fdict['listonly'])):
                warning(indent + '    ', f"{pfwdefs.SW_FILESECT}.{fname} does not appear in provenance lines")
                cnts[WARNCNT_POS] += 1

            if key in dataobjs[pfwdefs.SW_INPUTS]:
                cnts2 = check_file_valid_input(config, blockname, modname,
                                               fname, fdict, indent + '    ')
                cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts


            if key in dataobjs[pfwdefs.SW_OUTPUTS]:
                cnts2 = check_file_valid_output(config, blockname, modname,
                                                fname, fdict, indent + '    ')
                cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts


    # check every list
    if pfwdefs.SW_LISTSECT in moddict:
        print(f"{indent}Checking {pfwdefs.SW_LISTSECT} section...")
        for lname, ldict in moddict[pfwdefs.SW_LISTSECT].items():
            key = f"{pfwdefs.SW_LISTSECT}.{lname}"
            if key not in dataobjs[pfwdefs.SW_INPUTS] and \
               key not in dataobjs[pfwdefs.SW_OUTPUTS]:
                found = False
                if 'columns' in ldict:
                    for col in ldict['columns'].split(','):
                        nkey = key + "." + col
                        nkey = nkey.replace('.fullname', '')
                        if nkey in dataobjs[pfwdefs.SW_INPUTS] or \
                           nkey in dataobjs[pfwdefs.SW_OUTPUTS]:
                            found = True
                        # check to see if list def has file name
                        if not found:
                            nkey = col
                            nkey = 'file.' + nkey.replace('.fullname', '')
                            if nkey in dataobjs[pfwdefs.SW_INPUTS] or \
                               nkey in dataobjs[pfwdefs.SW_OUTPUTS]:
                                found = True

                if not found:
                    warning(indent + '    ', f"{pfwdefs.SW_LISTSECT}.{lname} does not appear in provenance lines")
                    cnts[WARNCNT_POS] += 1

            if key in dataobjs[pfwdefs.SW_INPUTS]:
                cnts2 = check_list_valid_input(config, blockname, modname,
                                               lname, ldict, indent + '    ')
                cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    return cnts



###########################################################################
def check_exec_cmd(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    """ Check exec cmd definition """

    cnts = [0] * NUMCNTS

    # check that each exec section has execname (required)
    if pfwdefs.SW_EXECNAME not in xsectdict:
        error(indent, f"block {blockname}, module {modname}, {xsectname} - missing {pfwdefs.SW_EXECNAME}")
        cnts[ERRCNT_POS] += 1
    elif '/' in xsectdict[pfwdefs.SW_EXECNAME]:
        warning(indent, f"block {blockname}, module {modname}, {xsectname} - hardcoded path in {pfwdefs.SW_EXECNAME} ({xsectdict[pfwdefs.SW_EXECNAME]})")
        cnts[WARNCNT_POS] += 1

    # almost all production cases would need to have command line arguments
    if pfwdefs.SW_CMDARGS not in xsectdict:
        warning(indent, f"block {blockname}, module {modname}, {xsectname} - missing {pfwdefs.SW_CMDARGS}")
        cnts[WARNCNT_POS] += 1
    else:
        moddict = config[pfwdefs.SW_MODULESECT][modname]
        argvars = pfwutils.search_wcl_for_variables(xsectdict[pfwdefs.SW_CMDARGS])
        for var in argvars:
            if var.endswith('.fullname'):
                var2 = var[0:-(len('.fullname'))]
                (sect, name, subname) = parse_wcl_objname(var2)
                if sect not in moddict or name not in moddict[sect]:
                    error(indent, f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_CMDARGS} - Undefined variable ({var})")
                    cnts[ERRCNT_POS] += 1

                if subname and subname not in moddict[pfwdefs.SW_FILESECT]:
                    error(indent, f"block {blockname}, module {modname}, {xsectname}, {pfwdefs.SW_CMDARGS} - Undefined variable ({var})")
                    cnts[ERRCNT_POS] += 1
            else:
                curvals = {'curr_block': blockname, 'curr_module': modname}
                (_, _) = config.search(var, {pfwdefs.PF_CURRVALS: curvals,
                                             'searchobj': xsectdict,
                                             'required':False,
                                             intgdefs.REPLACE_VARS: True})

        # check that all values in args exist?/
        # check for value names that look like file/list names but are missing file/list in front
        # check that all file/list entries in args appears in inputs/outputs : err
    return cnts



###########################################################################
def check_exec(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    """ Check if exec section is valid """

    cnts = [0] * NUMCNTS

    print(f"{indent}Checking {xsectname}...")
    try:
        cnts2 = check_exec_inputs(config, blockname, modname, dataobjs,
                                  xsectname, xsectdict, indent + '    ')
        cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    except:
        cnts[0] += 1
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=4)

    try:
        cnts2 = check_exec_outputs(config, blockname, modname, dataobjs,
                                   xsectname, xsectdict, indent + '    ')
        cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    except:
        cnts[0] += 1
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=4)

    try:
        cnts2 = check_exec_parentchild(config, blockname, modname, dataobjs,
                                       xsectname, xsectdict, indent + '    ')
        cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    except:
        cnts[0] += 1
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=4)

    try:
        cnts2 = check_exec_cmd(config, blockname, modname, dataobjs,
                               xsectname, xsectdict, indent + '    ')
        cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    except:
        cnts[0] += 1
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=4)

    return cnts


###########################################################################
def check(config, indent=''):
    """ Check submit wcl """

    # initialize counters

    cnts = [0, 0, 0, 0]

    cnts2 = check_globals(config, indent)
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    if cnts[ERRCNT_POS] > 0:
        print(f"{indent}Aborting test")
        return cnts

    cnts2 = check_block(config, indent)
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    if cnts[ERRCNT_POS] > 0:
        print(f"{indent}Aborting test")
        return cnts

    cnts2 = check_target_archive(config, indent)
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    #if cnts[ERRCNT_POS] > 0:
    #    print "%sAborting test" % (indent)
    #    return cnts

    cnts2 = check_home_archive(config, indent)
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    #if cnts[ERRCNT_POS] > 0:
    #    print "%sAborting test" % (indent)
    #    return cnts

    return cnts
