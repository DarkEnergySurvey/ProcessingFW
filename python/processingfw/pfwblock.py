# $Id: pfwblock.py 48552 2019-05-20 19:38:27Z friedel $
# $Rev:: 48552                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:38:27 #$:  # Date of last commit.

# pylint: disable=print-statement

""" functions used by the block tasks """

import sys
import stat
import os
import itertools
import copy
import re
import time
import json
import collections

import despymisc.miscutils as miscutils
import despydmdb.dbsemaphore as dbsem

import filemgmt.archive_transfer_utils as archive_transfer_utils
import filemgmt.metadefs as metadefs
import filemgmt.fmutils as fmutils

from intgutils.wcl import WCL
import intgutils.intgdefs as intgdefs
import intgutils.intgmisc as intgmisc
import intgutils.replace_funcs as replfuncs
import intgutils.queryutils as queryutils

import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils
import processingfw.pfwcondor as pfwcondor

#######################################################################
def get_datasect_types(config, modname):
    """ tell which data sections (files, lists) are inputs vs outputs """

    miscutils.fwdebug_print(f"BEG {modname}")

    #infsect = which_are_inputs(config, modname)
    #outfsect = which_are_outputs(config, modname)

    inputs = {pfwdefs.SW_FILESECT: [],
              pfwdefs.SW_LISTSECT: []}
    outfiles = collections.OrderedDict()
    intermedfiles = collections.OrderedDict()

    # For wrappers with more than 1 exec section, the inputs of one exec can
    #     be the inputs of a 2nd exec the framework should not attempt to stage
    #     these intermediate files
    execs = intgmisc.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname],
                                       pfwdefs.SW_EXECPREFIX)
    for _, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in miscutils.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                parts = miscutils.fwsplit(outfile, '.')
                outfiles['.'.join(parts[1:])] = True
                intermedfiles[outfile] = True

        if pfwdefs.SW_INPUTS in einfo:
            inarr = miscutils.fwsplit(einfo[pfwdefs.SW_INPUTS].lower())
            inarr2 = []
            for inname in inarr:
                numdots = inname.count('.')
                if numdots == 1:
                    inarr2.append(inname)
                else:
                    parts = miscutils.fwsplit(inname, '.')
                    inarr2.append('.'.join(parts[0:2]))
                    inarr2.append(f"file.{parts[2]}")

            for inname in inarr2:
                if inname not in intermedfiles:
                    parts = miscutils.fwsplit(inname, '.')
                    inputs[parts[0]].append('.'.join(parts[1:]))
    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"inputs={inputs}")
        miscutils.fwdebug_print(f"outputs={list(outfiles.keys())}")
    miscutils.fwdebug_print("END")
    return (inputs, list(outfiles.keys()))



#######################################################################
def copy_master(masterdata, nickname=None, startline=1):
    """ For master data list that has multiple files per line, copy set of files """

    lines = {}
    linecnt = startline
    for masterline in masterdata['list'][intgdefs.LISTENTRY].values():
        try:
            if nickname is not None:
                if nickname not in masterline['file']:
                    raise KeyError(f"Line doesn't have file with nickname {nickname}")
                lines[linecnt] = {'file': {'file0001': masterline['file'][nickname]}}
            elif len(masterline['file']) == 1:
                lines[linecnt] = {'file': {'file0001': list(masterline['file'].values())[0]}}
            else:
                raise ValueError("Problem copying master line - nickname count mismatch")

            linecnt += 1
        except:
            print(f"line {linecnt}: masterline['file'] = {masterline['file']}")
            print(f"\n\nline {linecnt}: nickname = {masterline['file']}")
            raise
    #return {'list': {intgdefs.LISTENTRY: lines}}
    return lines

#######################################################################
def add_runtime_path(config, currvals, fname, finfo, filename):
    """ Add runtime path to filename """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"creating path for {fname}")
        miscutils.fwdebug_print(f"finfo = {finfo}")
        miscutils.fwdebug_print(f"currvals = {currvals}")




    path = config.get_filepath('runtime', None, {pfwdefs.PF_CURRVALS: currvals,
                                                 'searchobj': finfo,
                                                 intgdefs.REPLACE_VARS: True,
                                                 'expand': True})

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"\tpath = {path}")

    #filename = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
    #                                      'searchobj': finfo,
    #                                      intgdefs.REPLACE_VARS: True,
    #                                      'expand': True})

    cmpext = ''
    if 'compression' in finfo and finfo['compression'] is not None and finfo['compression'] != 'None':
        #print "compression: %s, {finfo['compression'], type(finfo['compression']))
        cmpext = finfo['compression']

    fullname = []
    if isinstance(filename, list):
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"{fname} has multiple names, number of names = {len(filename)}")
        for name in filename:
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"path + filename = {path}/{name}")
            fullname.append(f"{path}/{name}{cmpext}")
    else:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"Adding path to filename for {filename}")
        fullname = [f"{path}/{filename}{cmpext}"]

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"END fullname = {fullname}")
    return fullname


#######################################################################
def create_simple_list(config, lname, ldict, currvals):
    """ Create simple filename list file based upon patterns """
    miscutils.fwdebug_print(f"BEG - {lname}")
    listname = config.getfull('listname',
                              {pfwdefs.PF_CURRVALS: currvals,
                               'searchobj': ldict})

    filename = config.get_filename(None,
                                   {pfwdefs.PF_CURRVALS: currvals,
                                    'searchobj': ldict,
                                    'required': True,
                                    'expand': True,
                                    intgdefs.REPLACE_VARS: False})

    pfwutils.search_wcl_for_variables(config)


    if isinstance(filename, list):
        listcontents = '\n'.join(filename)
    else:
        listcontents = filename

    listdir = os.path.dirname(listname)
    if listdir and not os.path.exists(listdir):
        miscutils.coremakedirs(listdir)

    with open(listname, 'w') as listfh:
        listfh.write(listcontents+"\n")

    miscutils.fwdebug_print("END\n\n")


###########################################################
def create_sublist_file(config, moddict, fname, finfo, currvals):
    """ Create sublists of filenames for file definition """
    #filename = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
    #                                      'searchobj': finfo,
    #                                      intgdefs.REPLACE_VARS: False,
    #                                      'expand': False})

    searchopts = {pfwdefs.PF_CURRVALS: currvals,
                  'searchobj': finfo,
                  intgdefs.REPLACE_VARS: False,
                  'expand': False}

    # first check for filename pattern override
    (found, filenamepat) = config.search('filename', searchopts)
    if not found:
        # get filename pattern from global settings:
        (found, filepat) = config.search(pfwdefs.SW_FILEPAT, searchopts)

    if not found:
        miscutils.fwdie(f"Error: Could not find file pattern {pfwdefs.SW_FILEPAT}",
                        pfwdefs.PF_EXIT_FAILURE)

    if pfwdefs.SW_FILEPATSECT not in config:
        miscutils.fwdie(f"Error: Could not find filename pattern section ({pfwdefs.SW_FILEPATSECT})",
                        pfwdefs.PF_EXIT_FAILURE)
    elif filepat in config[pfwdefs.SW_FILEPATSECT]:
        filenamepat = config[pfwdefs.SW_FILEPATSECT][filepat]
    else:
        miscutils.fwdie(f"Error: Could not find filename pattern for {filepat}",
                        pfwdefs.PF_EXIT_FAILURE, 2)

    # get 2 list (filename, filedict) by expanding variables in the filename pattern
    newfileinfo = replfuncs.replace_vars(filenamepat, config,
                                         {pfwdefs.PF_CURRVALS: currvals,
                                          'searchobj': finfo,
                                          intgdefs.REPLACE_VARS: True,
                                          'expand': True,
                                          'keepvars': True})

    # convert to same format as if read from file created by query
    filelist_wcl = None
    if newfileinfo:
        if isinstance(newfileinfo[0], str):
            newfileinfo = ([newfileinfo[0]], [newfileinfo[1]])

        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"newfileinfo = {str(newfileinfo)}")

        filedict_list = []
        for fcnt in range(0, len(newfileinfo[0])):
            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"name = {str(newfileinfo[0][fcnt])}")
                miscutils.fwdebug_print(f"info = {str(newfileinfo[1][fcnt])}")
            file1 = newfileinfo[1][fcnt]
            file1['filename'] = newfileinfo[0][fcnt]

            # merge particular file information with file definition
            sinfo = copy.deepcopy(finfo)
            sinfo.update(file1)

            file1['fullname'] = add_runtime_path(config, currvals, fname, sinfo, file1['filename'])[0]
            filedict_list.append(file1)
        filelist_wcl = queryutils.convert_single_files_to_lines(filedict_list)

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.pretty_print_dict(filelist_wcl)
    return filelist_wcl


###########################################################
def create_simple_sublist(config, moddict, lname, ldict, currvals):
    """ create a simple sublist of files for a list without query """

    miscutils.fwdebug_print("BEG")

    # grab file section names from columns value in list def
    filesects = collections.OrderedDict()
    if 'columns' in ldict:
        columns = convert_col_string_to_list(ldict['columns'], with_format=True)
        for col in columns:
            filesects[col.lower().split('.')[0]] = True

    if len(filesects) > 1:
        miscutils.fwdie('The framework currently does not support multiple file-column lists without query', pfwdefs.PF_EXIT_FAILURE)

    fname = list(filesects.keys())[0]
    finfo = moddict[pfwdefs.SW_FILESECT][fname]
    filelist_wcl = create_sublist_file(config, moddict, fname, finfo, currvals)

    miscutils.fwdebug_print("END")

    return filelist_wcl


#######################################################################
def get_match_keys(sdict):
    """ Get keys on which to match files """
    mkeys = []

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"keys in sdict: {list(sdict.keys())}")

    if 'loopkey' in sdict:
        mkeys = miscutils.fwsplit(sdict['loopkey'].lower())
        #mkeys.sort()
    elif 'match' in sdict:
        mkeys = miscutils.fwsplit(sdict['match'].lower())
        #mkeys.sort()
    elif 'divide_by' in sdict:
        mkeys = miscutils.fwsplit(sdict['divide_by'].lower())
        #mkeys.sort()

    return mkeys


#######################################################################
def find_sublist(objdef, objinst, sublists):
    """ Find sublist """

    if len(sublists) > 1:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"sublist keys: {list(sublists.keys())}")

        matchkeys = get_match_keys(objdef)

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"matchkeys: {matchkeys}")

        index = ""
        for mkey in matchkeys:
            if mkey not in objinst:
                miscutils.fwdie(f"Error: Cannot find match key {mkey} in inst {objinst}",
                                pfwdefs.PF_EXIT_FAILURE)
            index += objinst[mkey] + '_'

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("sublist index = " + index)

        if index not in sublists:
            miscutils.fwdie("Error: Cannot find sublist matching " + index, pfwdefs.PF_EXIT_FAILURE)
        sublist = sublists[index]
    else:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"Taking first sublist.  sublist keys: {list(sublists.keys())}")
        sublist = list(sublists.values())[0]

    return sublist

#######################################################################
def which_are_inputs(config, modname):
    """ Return dict of files/lists that are inputs for given module """
    miscutils.fwdebug_print(f"BEG {modname}")

    inputs = {pfwdefs.SW_FILESECT: [], pfwdefs.SW_LISTSECT: []}
    outfiles = collections.OrderedDict()

    # For wrappers with more than 1 exec section, the inputs of one exec can
    #     be the inputs of a 2nd exec the framework should not attempt to stage
    #     these intermediate files
    execs = intgmisc.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname],
                                       pfwdefs.SW_EXECPREFIX)
    for _, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in miscutils.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                outfiles[outfile] = True

        if pfwdefs.SW_INPUTS in einfo:
            inarr = miscutils.fwsplit(einfo[pfwdefs.SW_INPUTS].lower())
            for inname in inarr:
                if inname not in outfiles:
                    parts = miscutils.fwsplit(inname, '.')
                    inputs[parts[0]].append('.'.join(parts[1:]))

    #miscutils.fwdebug_print(inputs)
    miscutils.fwdebug_print("END")
    return inputs


#######################################################################
def which_are_outputs(config, modname):
    """ Return dict of files that are outputs for given module """
    miscutils.fwdebug_print(f"BEG {modname}")

    outfiles = collections.OrderedDict()

    execs = intgmisc.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname],
                                       pfwdefs.SW_EXECPREFIX)
    for _, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in miscutils.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                parts = miscutils.fwsplit(outfile, '.')
                outfiles['.'.join(parts[1:])] = True

    #miscutils.fwdebug_print(outfiles.keys())
    miscutils.fwdebug_print("END")
    return list(outfiles.keys())





#######################################################################
def assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict,
                                currvals, winst, fsectname, finfo,
                                masterdata, sublists, is_iter_obj=False):
    """ Assign files to wrapper instance """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG: Working on file {fsectname}")
        miscutils.fwdebug_print(f"theinputs: {theinputs}")
        miscutils.fwdebug_print(f"outputs: {theoutputs}")
        miscutils.fwdebug_print(f"is_iter_obj: {is_iter_obj}")

    if pfwdefs.IW_FILESECT not in winst:
        winst[pfwdefs.IW_FILESECT] = collections.OrderedDict()

    if 'listonly' in finfo and miscutils.convertBool(finfo['listonly']):
        for osectname in theoutputs:
            if osectname.endswith('.'+fsectname):
                winst[pfwdefs.IW_FILESECT][fsectname] = collections.OrderedDict()
                miscutils.fwdebug_print(f"Added {fsectname} a listonly key to the file section")

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"Skipping {fsectname} due to listonly key")
        return

    modname = moddict['modulename']

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"modname = {modname}")
        miscutils.fwdebug_print(f"winst: {winst}")
        miscutils.fwdebug_print(f"currvals: {currvals}")

    fkey = f"file-{fsectname}"
    winst[pfwdefs.IW_FILESECT][fsectname] = collections.OrderedDict()
    if sublists is not None and fkey in sublists:  # files came from query
        sublist = find_sublist(finfo, winst, sublists[fkey])
        ignore_multiple_error = False
        if 'ignore_multiple_error' in finfo and miscutils.convertBool(finfo['ignore_multiple_error']):
            ignore_multiple_error = True

        if len(sublist['list'][intgdefs.LISTENTRY]) > 1 and not ignore_multiple_error:
            print(f"Error: more than 1 line to choose from for file {fkey}")
            print("\twinst = ", winst)
            print("\tnum sublists = ", len(sublists[fkey]))
            skeys = list(sublists[fkey].keys())
            for i in range(0, min(10, len(skeys))):
                print(skeys[i])
            print("\n")
            print("\t# files = ", len(sublist['list'][intgdefs.LISTENTRY]))
            print(miscutils.pretty_print_dict(sublist['list'][intgdefs.LISTENTRY]))

            print("\tCheck divide_by/match")
            miscutils.fwdie(f"Error: more than 1 line to choose from for file ({fkey})",
                            pfwdefs.PF_EXIT_FAILURE)

        fullnames = []
        for line in sublist['list'][intgdefs.LISTENTRY].values():
            if 'file' not in line:
                miscutils.fwdie("Error: 0 file in line" + str(line), pfwdefs.PF_EXIT_FAILURE)

            if len(line['file']) > 1:
                #print miscutils.pretty_print_dict(line['file'])
                raise Exception("more than 1 file to choose from for file" + line['file'])
            finfo = list(line['file'].values())[0]
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"finfo = {finfo}")

            fullnames.append(finfo['fullname'])
        winst[pfwdefs.IW_FILESECT][fsectname]['fullname'] = ','.join(fullnames)

        ## save input and output filenames (with job scratch path)
        ## In order to preserve capitalization, put on right side of =,
        ##    using dummy count for left side
        #if fsectname in theinputs[pfwdefs.SW_FILESECT]:
        #    miscutils.fwdebug_print("Added to wrapinputs %s" % fullnames)
        #    for fname in fullnames:
        #        winst['wrapinputs'][len(winst['wrapinputs'])+1] = fname
        #elif fsectname in theoutputs:
        #    miscutils.fwdebug_print("Added to wrapoutputs %s" % fullnames)
        #    for fname in fullnames:
        #        winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = fname
    elif 'fullname' in moddict[pfwdefs.SW_FILESECT][fsectname]:
        winst[pfwdefs.IW_FILESECT][fsectname]['fullname'] = moddict[pfwdefs.SW_FILESECT][fsectname]['fullname']
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"Copied fullname for {fsectname} = {winst[pfwdefs.IW_FILESECT][fsectname]}")
        #if fsectname in theinputs[pfwdefs.SW_FILESECT]:
        #    miscutils.fwdebug_print("Added to wrapinputs %s" % moddict[pfwdefs.SW_FILESECT][fsectname]['fullname'])
        #    winst['wrapinputs'][len(winst['wrapinputs'])+1] = moddict[pfwdefs.SW_FILESECT][fsectname]['fullname']
        #elif fsectname in theoutputs:
        #    miscutils.fwdebug_print("Added to wrapoutputs %s" % moddict[pfwdefs.SW_FILESECT][fsectname]['fullname'])
        #    winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = moddict[pfwdefs.SW_FILESECT][fsectname]['fullname']
    else:
        sobj = copy.deepcopy(winst)
        sobj.update(finfo)   # order matters file values must override winst values

        # note: save keys/vals used when creating filenames in order to use to create future filenames

        if 'filename' in moddict[pfwdefs.SW_FILESECT][fsectname]:
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"filename in {fsectname}")

            filename = config.get('filename', {pfwdefs.PF_CURRVALS: currvals,
                                               'searchobj': sobj,
                                               'expand': False,
                                               'required': True,
                                               intgdefs.REPLACE_VARS:False})

            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"filename = {filename}")

        else:
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"creating filename for {fsectname}")
                miscutils.fwdebug_print(f"\tfinfo = {finfo}")
                miscutils.fwdebug_print(f"\tsobj = {sobj}")
            filename = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
                                                  'searchobj': sobj,
                                                  'expand': False,
                                                  intgdefs.REPLACE_VARS:False})

        fileinfo = replfuncs.replace_vars(filename, config, {pfwdefs.PF_CURRVALS: currvals,
                                                             'searchobj': sobj,
                                                             'expand': True,
                                                             intgdefs.REPLACE_VARS:True,
                                                             'keepvars': True})
        if fileinfo is None:
            miscutils.fwdie(f"empty fileinfo {modname}.{fkey}", pfwdefs.PF_EXIT_FAILURE)

        # save file info as if we read from query
        fnames = fileinfo[0]
        filelist = []
        if isinstance(fnames, list):
            for cnt, val in enumerate(fnames):
                finfo = fileinfo[1][cnt]
                finfo['filename'] = val
                filelist.append(finfo)
        else:
            finfo = fileinfo[1]
            finfo['filename'] = fnames
            filelist.append(finfo)


        if modname not in masterdata:
            masterdata[modname] = collections.OrderedDict()

        if fkey in masterdata[modname]:
            initcnt = len(masterdata[modname][fkey]['list']['line']) + 1
            newdata = queryutils.convert_single_files_to_lines(filelist, initcnt)
            masterdata[modname][fkey]['list']['line'].update(newdata['list']['line'])
        else:
            masterdata[modname][fkey] = queryutils.convert_single_files_to_lines(filelist)

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"saved file info for {modname}.{fkey}")

        winst[pfwdefs.IW_FILESECT][fsectname]['filename'] = fnames

        # Add runtime path to filename
        fullname = add_runtime_path(config, currvals, fsectname, sobj, winst[pfwdefs.IW_FILESECT][fsectname]['filename'])
        #if fsectname in theinputs[pfwdefs.SW_FILESECT]:
        #    for name in fullname:
        #        miscutils.fwdebug_print("Added to wrapinputs %s" % name)
        #        winst['wrapinputs'][len(winst['wrapinputs'])+1] = name
        #elif fsectname in theoutputs:
        #    for name in fullname:
        #        miscutils.fwdebug_print("Added to wrapoutputs %s" % name)
        #        winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = name

        winst[pfwdefs.IW_FILESECT][fsectname]['fullname'] = ','.join(fullname)
        #print winst[pfwdefs.IW_FILESECT][fsectname]['fullname']
        del winst[pfwdefs.IW_FILESECT][fsectname]['filename']



    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"is_iter_obj = {is_iter_obj} {finfo}")
    if finfo is not None and is_iter_obj:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("is_iter_obj = true")
        winst['iter_obj_info'] = {}
        for key, val in finfo.items():
            if key not in ['fullname', 'filename', 'filepat', 'dirpat', 'filetype', 'compression']:
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"is_iter_obj: saving {key}")
                winst['iter_obj_info'][key] = val

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"END: Done working on file {fsectname}")
        miscutils.fwdebug_print(f"END: winst={winst}")



#######################################################################
def assign_list_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals,
                                winst, lname, ldict, sublists):
    """ Assign list to wrapper instance """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG: Working on list {lname} from {moddict['modulename']}")
        miscutils.fwdebug_print(f"sublists.keys() = {list(sublists.keys())}")
        miscutils.fwdebug_print(f"currvals = {currvals}")
    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"ldict = {ldict}")

    if pfwdefs.IW_LISTSECT not in winst:
        winst[pfwdefs.IW_LISTSECT] = collections.OrderedDict()


    ### create an object that has values from ldict and winst
    sobj = copy.deepcopy(ldict)
    sobj.update(winst)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"sobj = {sobj}")

    #miscutils.fwdebug_print("creating listdir and listname")

    #listdir = config.get_filepath('runtime', 'list', {pfwdefs.PF_CURRVALS: currvals,
    #                     'required': True, intgdefs.REPLACE_VARS: True,
    #                     'searchobj': sobj})
    #listname = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
    #                               'searchobj': sobj, 'required': True, intgdefs.REPLACE_VARS: True})
    #miscutils.fwdebug_print("listname = {listname))
    #listname = "%s/{listdir, listname)

    #winst[pfwdefs.IW_LISTSECT][lname]['fullname'] = listname
    #miscutils.fwdebug_print("full listname = {winst[pfwdefs.IW_LISTSECT][lname]['fullname']))

    sublist = None
    lkey = f"list-{lname}"
    if lkey not in sublists:
        sublist = create_simple_sublist(config, moddict, lname, ldict, currvals)
    else:
        sublist = find_sublist(ldict, winst, sublists[lkey])

    if sublist is not None:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"lname = {lname}, sublist has {len(sublist['list'][intgdefs.LISTENTRY])} lines")

        for llabel, lldict in sublist['list'][intgdefs.LISTENTRY].items():
            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"llabel = {llabel}, ldict = {ldict}")
            for flabel, _ in lldict['file'].items():
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"flabel = {flabel}, theinputs = {theinputs}")

                #if flabel in theinputs['file']:
                #    miscutils.fwdebug_print("Added to wrapinputs %s" % fdict['fullname'])
                #    winst['wrapinputs'][len(winst['wrapinputs'])+1] = fdict['fullname']


        ### create an object that has values from ldict and winst
        msobj = copy.deepcopy(ldict)
        msobj.update(winst)

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"msobj = {msobj}")


        if pfwdefs.DIV_LIST_BY_COL in msobj:
            divbycol = msobj[pfwdefs.DIV_LIST_BY_COL]
            del msobj[pfwdefs.DIV_LIST_BY_COL]
            for divcolname, divcoldict in divbycol.items():
                sobj = copy.deepcopy(msobj)
                sobj.update(divcoldict)
                winst[pfwdefs.IW_LISTSECT][divcolname] = {'fullname': output_list(config, sublist, sobj, lname, currvals),
                                                          'columns': ','.join(convert_col_string_to_list(divcoldict['columns'], False))}
                lineformat = intgdefs.DEFAULT_LIST_FORMAT
                if intgdefs.LIST_FORMAT in divcoldict:
                    lineformat = divcoldict[intgdefs.LIST_FORMAT]
                winst[pfwdefs.IW_LISTSECT][divcolname][intgdefs.LIST_FORMAT] = lineformat

        else:
            cols = get_list_all_columns(msobj, with_format=False)
            winst[pfwdefs.IW_LISTSECT][lname] = {'fullname': output_list(config, sublist, msobj, lname, currvals),
                                                 'columns': ','.join(cols[0])}

            lineformat = intgdefs.DEFAULT_LIST_FORMAT
            if intgdefs.LIST_FORMAT in ldict:
                lineformat = ldict[intgdefs.LIST_FORMAT]
            winst[pfwdefs.IW_LISTSECT][lname][intgdefs.LIST_FORMAT] = lineformat
    else:
        print(f"Warning: Couldn't find files to put in list {lname} in {moddict['modulename']}")

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END")




#######################################################################
def assign_data_wrapper_inst(config, modname, winst, masterdata, sublists,
                             theinputs, theoutputs):
    """ Assign data like files and lists to wrapper instances """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG {modname}")
        miscutils.fwdebug_print(f"sublists.keys() = {list(sublists.keys())}")

    moddict = config[pfwdefs.SW_MODULESECT][modname]
    currvals = {'curr_module': modname}
    (found, loopkeys) = config.search('wrapperloop',
                                      {pfwdefs.PF_CURRVALS: currvals,
                                       'required': False,
                                       intgdefs.REPLACE_VARS: True})
    if found:
        loopkeys = miscutils.fwsplit(loopkeys.lower())
    else:
        loopkeys = []

    #winst['wrapinputs'] = OrderedDict()
    #winst['wrapoutputs'] = OrderedDict()

    # create currvals
    currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: winst[pfwdefs.PF_WRAPNUM]}
    for key in loopkeys:
        currvals[key] = winst[key]
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("currvals " + str(currvals))

    # do wrapper loop object first, if exists, to provide keys for filenames
    iter_obj_key = get_wrap_iter_obj_key(config, moddict)


    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"{winst[pfwdefs.PF_WRAPNUM]}: Assigning files to wrapper inst")

    #if iter_obj_key is not None or pfwdefs.SW_FILESECT in moddict:
    if iter_obj_key is not None:
        (iter_obj_sect, iter_obj_name) = miscutils.fwsplit(iter_obj_key, '.')
        iter_obj_dict = pfwutils.get_wcl_value(iter_obj_key, moddict)
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"iter_obj {iter_obj_name} {iter_obj_sect}")
        if iter_obj_sect.lower() == pfwdefs.SW_FILESECT.lower():
            assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst,
                                        iter_obj_name, iter_obj_dict, masterdata, sublists, True)
        elif iter_obj_sect.lower() == pfwdefs.SW_LISTSECT.lower():
            assign_list_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst,
                                        iter_obj_name, iter_obj_dict, sublists)
        else:
            miscutils.fwdie(f"Error: unknown iter_obj_sect ({iter_obj_sect})",
                            pfwdefs.PF_EXIT_FAILURE)


    if pfwdefs.SW_FILESECT in moddict:
        for fname, fdict in moddict[pfwdefs.SW_FILESECT].items():
            if iter_obj_key is not None and \
               iter_obj_sect.lower() == pfwdefs.SW_FILESECT.lower() and \
               iter_obj_name.lower() == fname.lower():
                continue    # already did iter_obj
            assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst,
                                        fname, fdict, masterdata, sublists, False)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("currvals " + str(currvals))

    if pfwdefs.SW_LISTSECT in moddict:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"{winst[pfwdefs.PF_WRAPNUM]}: Assigning lists to wrapper inst")
        for lname, ldict in moddict[pfwdefs.SW_LISTSECT].items():
            if iter_obj_key is not None and \
               iter_obj_sect.lower() == pfwdefs.SW_LISTSECT.lower() and \
               iter_obj_name.lower() == lname.lower():
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"skipping list {lname} as already did for it as iter_obj")
                continue    # already did iter_obj
            assign_list_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst,
                                        lname, ldict, sublists)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")



#######################################################################
def output_list(config, sublist, sobj, lname, currvals):
    """ Output list """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG: {lname}")
        miscutils.fwdebug_print(f"sobj dict: {sobj}")
        miscutils.fwdebug_print("creating listdir and listname")

    # list dir and filename must use current attempt values
    currvals2 = copy.deepcopy(currvals)
    currvals2[pfwdefs.REQNUM] = config.getfull(pfwdefs.REQNUM)
    currvals2[pfwdefs.UNITNAME] = config.getfull(pfwdefs.UNITNAME)
    currvals2[pfwdefs.ATTNUM] = config.getfull(pfwdefs.ATTNUM)

    listdir = config.get_filepath('runtime', 'list', {pfwdefs.PF_CURRVALS: currvals2,
                                                      'required': True, intgdefs.REPLACE_VARS: True,
                                                      'searchobj': sobj})

    listname = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals2,
                                          'searchobj': sobj, 'required': True, intgdefs.REPLACE_VARS: True})
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"listname = {listname}")
    listname = f"{listdir}/{listname}"

    #winst[pfwdefs.IW_LISTSECT][lname]['fullname'] = listname
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"full listname = {listname}")

    listdir = os.path.dirname(listname)
    miscutils.coremakedirs(listdir)

    lineformat = intgdefs.DEFAULT_LIST_FORMAT
    if intgdefs.LIST_FORMAT in sobj:
        lineformat = sobj[intgdefs.LIST_FORMAT]

    lines = list(sublist['list'][intgdefs.LISTENTRY].values())
    if 'sortkey' in sobj and sobj['sortkey'] is not None:
        # (key, numeric, reverse)
        sort_reverse = False
        sort_numeric = False

        if sobj['sortkey'].strip().startswith('('):
            rmatch = re.match(r'\(([^)]+)', sobj['sortkey'])
            if rmatch:
                sortinfo = miscutils.fwsplit(rmatch.group(1))
                sort_key = sortinfo[0]
                if len(sortinfo) > 1:
                    sort_numeric = miscutils.convertBool(sortinfo[1])
                if len(sortinfo) > 2:
                    sort_reverse = miscutils.convertBool(sortinfo[2])
            else:
                miscutils.fwdie(f"Error: problems parsing sortkey...\n{sobj['sortkey']}",
                                pfwdefs.PF_EXIT_FAILURE)
        else:
            sort_key = sobj['sortkey']

        sort_key = sort_key.lower()

        if sort_numeric:
            lines = sorted(lines, reverse=sort_reverse,
                           key=lambda k: float(get_value_from_line(k, sort_key, None, 1)))
        else:
            lines = sorted(lines, reverse=sort_reverse,
                           key=lambda k: get_value_from_line(k, sort_key, None, 1))

    allow_missing = False
    if 'allow_missing' in sobj:
        allow_missing = miscutils.convertBool(sobj['allow_missing'])

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"sobj = {sobj}")
    columns = get_list_all_columns(sobj)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"Writing list to file {listname}")
    with open(listname, "w") as listfh:
        for linedict in lines:
            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"columns = {columns}")
            output_line(listfh, linedict, lineformat, allow_missing, columns[0])

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return listname




#####################################################################
def output_line(listfh, line, lineformat, allow_missing, keyarr):
    """ output line into input list for science code"""
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG line={line}  keyarr={keyarr}")

    lineformat = lineformat.lower()

    if lineformat in ['config', 'wcl']:
        listfh.write("<file>\n")

    numkeys = len(keyarr)
    for i in range(0, numkeys):
        key = keyarr[i]
        value = None
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"key: {key}")

        valuefmt = None
        if key.startswith('$FMT{'):
            rmatch = re.match(r'\$FMT\{\s*([^,]+)\s*,\s*(\S+)\s*\}', key)
            if rmatch:
                valuefmt = rmatch.group(1).strip()
                key = rmatch.group(2).strip()
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"valuefmt = {valuefmt}, key = {key}")
            else:
                miscutils.fwdie(f"Error: invalid FMT column: {key}", pfwdefs.PF_EXIT_FAILURE)


        if '.' in  key:
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("Found period in key")
            [nickname, key2] = key.replace(' ', '').split('.')
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"\tnickname = {nickname}, key2 = {key2}")
            value = get_value_from_line(line, key2, nickname, None)
            if value is None:
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"Didn't find value in line with nickname {nickname}")
                    miscutils.fwdebug_print(f"Trying to find {key2} without nickname")
                value = get_value_from_line(line, key2, None, 1)
                if value is None:
                    if allow_missing:
                        value = ""
                    else:
                        miscutils.fwdie(f"Error: could not find value {key} for line...\n{line}", pfwdefs.PF_EXIT_FAILURE)
                else: # assume nickname was really table name
                    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"\tassuming nickname ({nickname}) was really table name")
                    key = key2
        else:
            value = get_value_from_line(line, key, None, 1)

        # handle last field (separate to avoid trailing comma)
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"printing key={key} value={value}")
        if i == numkeys - 1:
            print_value(listfh, key, value, lineformat, True, valuefmt)
        else:
            print_value(listfh, key, value, lineformat, False, valuefmt)

    if lineformat in ["config", 'wcl']:
        listfh.write("</file>\n")
    else:
        listfh.write("\n")


#####################################################################
def print_value(outfh, key, value, lineformat, last, valuefmt):
    """ output value to input list in correct format """

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG {key}={value} ({type(value)})")

    if valuefmt is not None:
        if re.search(r'%\d*d', valuefmt):
            value = valuefmt % int(value)
        elif re.search(r'%\d*(.\d+)f', valuefmt):
            value = valuefmt % float(value)
        else:
            value = valuefmt % value

    lineformat = lineformat.lower()
    if lineformat in ['config', 'wcl']:
        outfh.write(f"     {key}={str(value)}\n")
    else:
        outfh.write(str(value))
        if not last:
            if lineformat == 'textcsv':
                outfh.write(', ')
            elif lineformat == 'texttab':
                outfh.write('\t')
            else:
                outfh.write(' ')



#######################################################################
def finish_wrapper_inst(config, modname, winst, outfsect):
    """ Finish creating wrapper instances with tasks like making input and output filenames """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG {modname}")
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    #input_filenames = []
    #output_filenames = []
    #for fname in winst['wrapinputs'].values():
    #    input_filenames.append(miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME))
    #
    #for fname in winst['wrapoutputs'].values():
    #    output_filenames.append(miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME))

    if 'iter_obj_info' in winst:
        for key, val in winst['iter_obj_info'].items():
            if key not in winst:
                winst[key] = val
        del winst['iter_obj_info']

    # create searching options
    currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: winst[pfwdefs.PF_WRAPNUM]}
    searchopts = {pfwdefs.PF_CURRVALS: currvals,
                  'searchobj': winst,
                  intgdefs.REPLACE_VARS: True,
                  'required': True}


    if pfwdefs.SW_FILESECT in moddict:
        for fname, fdict in moddict[pfwdefs.SW_FILESECT].items():
            #print "fname = %s" % fname
            is_output_file = False
            for ofsect in outfsect:
                #print "ofsect = %s" % ofsect
                if ofsect == fname or ofsect.endswith('.'+fname):
                    is_output_file = True
            #print "is_output_file = %s" % is_output_file

            if 'listonly' in fdict and miscutils.convertBool(fdict['listonly']):
                if not is_output_file:
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"Skipping {fname} due to listonly key")
                    continue

            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"{winst[pfwdefs.PF_WRAPNUM]}: working on file: {fname}")
                if 'fullname' in winst[pfwdefs.IW_FILESECT][fname]:
                    miscutils.fwdebug_print(f"fullname = {winst[pfwdefs.IW_FILESECT][fname]['fullname']}")

            #for k in ['filetype', metadefs.WCL_META_REQ, metadefs.WCL_META_OPT,
            #          pfwdefs.SAVE_FILE_ARCHIVE, pfwdefs.COMPRESS_FILES,pfwdefs.DIRPAT]:
            #    if k in fdict:
            for k in fdict:
                if k not in ['keyvals']:
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"{fname} copying {k}")
                    winst[pfwdefs.IW_FILESECT][fname][k] = copy.deepcopy(fdict[k])
                elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"{fname}: no {k}")

            if pfwdefs.SW_OUTPUT_OPTIONAL in fdict:
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"{fname} copying {pfwdefs.SW_OUTPUT_OPTIONAL}")

                winst[pfwdefs.IW_FILESECT][fname][pfwdefs.IW_OUTPUT_OPTIONAL] = miscutils.convertBool(fdict[pfwdefs.SW_OUTPUT_OPTIONAL])

            hdrups = pfwutils.get_hdrup_sections(fdict, metadefs.WCL_UPDATE_HEAD_PREFIX)
            for hname, hdict in hdrups.items():
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"{fname} copying {hname}")
                winst[pfwdefs.IW_FILESECT][fname][hname] = copy.deepcopy(hdict)

            # save OPS path for archive
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"Is fname ({fname}) in outputfiles? {is_output_file}")
            filesave = miscutils.checkTrue(pfwdefs.SAVE_FILE_ARCHIVE, fdict, True)
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"Is save_file_archive true? {filesave}")
            if is_output_file:
                winst[pfwdefs.IW_FILESECT][fname][pfwdefs.SAVE_FILE_ARCHIVE] = filesave  # canonicalize
                if pfwdefs.DIRPAT not in fdict:
                    print(f"Warning: Could not find {pfwdefs.DIRPAT} in {fname}'s section")
                else:
                    searchobj = copy.deepcopy(fdict)
                    searchobj.update(winst)
                    searchopts['searchobj'] = searchobj
                    winst[pfwdefs.IW_FILESECT][fname]['archivepath'] = config.get_filepath('ops',
                                                                                           fdict[pfwdefs.DIRPAT], searchopts)

        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"fdict = {fdict}")

    searchopts[intgdefs.REPLACE_VARS] = True

    # wrappername
    winst['wrappername'] = config.getfull('wrappername', searchopts)

    # input wcl fullname
    inputwcl_name = config.get_filename('inputwcl', searchopts)
    inputwcl_path = config.get_filepath('runtime', 'inputwcl', searchopts)
    #print inputwcl_name, inputwcl_path
    winst['inputwcl'] = inputwcl_path + '/' + inputwcl_name


    # log fullname
    log_name = config.get_filename('log', searchopts)
    log_path = config.get_filepath('runtime', 'log', searchopts)
    winst['log'] = log_path + '/' + log_name
    winst['log_archive_path'] = config.get_filepath('ops', 'log', searchopts)
    #output_filenames.append(winst['log'])


    # output wcl fullname
    outputwcl_name = config.get_filename('outputwcl', searchopts)
    outputwcl_path = config.get_filepath('runtime', 'outputwcl', searchopts)
    winst['outputwcl'] = outputwcl_path + '/' + outputwcl_name

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    #return input_filenames, output_filenames


#######################################################################
def add_file_metadata(config, modname, connect=None):
    """ add file metadata sections to a single file section from a module"""

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print("Working on module " + modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    execs = intgmisc.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)

    if pfwdefs.SW_FILESECT in moddict:
        filemgmt = None
        try:
            filemgmt_class = miscutils.dynamically_load_class(config.getfull('filemgmt'))
            paramdict = config.get_param_info(filemgmt_class.requested_config_vals(),
                                              {pfwdefs.PF_CURRVALS: {'curr_module': modname}})
            if connect is not None:
                paramdict['connection'] = connect
            filemgmt = filemgmt_class(paramdict)
        except:
            print(f"Error:  Problems dynamically loading class ({config.getfull('filemgmt')}) in order to get metadata specs")
            raise

        for k in execs:
            if pfwdefs.SW_OUTPUTS in moddict[k]:
                for outfile in miscutils.fwsplit(moddict[k][pfwdefs.SW_OUTPUTS]):
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("Working on output file " + outfile)
                    match = re.match(fr'{pfwdefs.SW_FILESECT}.(\w+)', outfile)
                    if match:
                        fname = match.group(1)
                        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("Working on file " + fname)
                        if fname not in moddict[pfwdefs.SW_FILESECT]:
                            msg = f"Error: file {fname} listed in {pfwdefs.SW_OUTPUTS}, but not defined in {pfwdefs.SW_FILESECT} section"
                            miscutils.fwdie(msg, pfwdefs.PF_EXIT_FAILURE)

                        fdict = moddict[pfwdefs.SW_FILESECT][fname]
                        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print(f"output file dictionary for {outfile} = {fdict}")
                        #filetype = fdict['filetype'].lower()
                    elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"output file {k} doesn't have definition ({pfwdefs.SW_FILESECT})")

            elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"No was_generated_by for {k}")

    elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"No file section ({pfwdefs.SW_FILESECT})")

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


#######################################################################
def init_use_archive_info(config, jobwcl, which_use_input, which_use_output, which_archive):
    """ Initialize use archive info """
    if which_use_input in config:
        jobwcl[which_use_input] = config.getfull(which_use_input).lower()
    else:
        jobwcl[which_use_input] = 'never'

    if which_use_output in config:
        jobwcl[which_use_output] = config.getfull(which_use_output).lower()
    else:
        jobwcl[which_use_output] = 'never'

    if jobwcl[which_use_input] != 'never' or jobwcl[which_use_output] != 'never':
        jobwcl[which_archive] = config.getfull(which_archive)
        archive = jobwcl[which_archive]
    else:
        jobwcl[which_archive] = None
        archive = 'no_archive'

    return archive


#######################################################################
def write_jobwcl(config, jobkey, jobdict):
    """ write a little config file containing variables needed at the job level """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG jobnum={jobdict['jobnum'], } jobkey={jobkey}")

    jobdict['jobwclfile'] = config.get_filename('jobwcl', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM: jobdict['jobnum']},
                                                           'required': True, intgdefs.REPLACE_VARS: True})
    jobdict['outputwcltar'] = config.get_filename('outputwcltar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']},
                                                                   'required': True, intgdefs.REPLACE_VARS: True})

    jobdict['envfile'] = config.get_filename('envfile')

    modulelist = miscutils.fwsplit(config.getfull(pfwdefs.SW_MODULELIST).lower())
    fwgroups = collections.OrderedDict()
    gnum = 1
    for modname in modulelist:
        if modname in jobdict['parlist']:
            fwgroups[f'g{gnum:04d}'] = {'wrapnums': ','.join(jobdict['parlist'][modname]['wrapnums']),
                                        'fw_nthread': jobdict['parlist'][modname]['fw_nthread'],
                                        'fw_thread_reuse': jobdict['parlist'][modname]['fw_thread_reuse']
                                        }
            gnum += 1
    send_services = False
    if pfwdefs.SEND_SERVICES_FILE in config:
        send_services = miscutils.convertBool(config[pfwdefs.SEND_SERVICES_FILE])

    jobwcl = WCL({'pfw_attempt_id': config['pfw_attempt_id'],
                  pfwdefs.REQNUM: config.getfull(pfwdefs.REQNUM),
                  pfwdefs.UNITNAME:config.getfull(pfwdefs.UNITNAME),
                  pfwdefs.ATTNUM: config.getfull(pfwdefs.ATTNUM),
                  pfwdefs.PF_BLKNUM: config.getfull(pfwdefs.PF_BLKNUM),
                  pfwdefs.PF_JOBNUM: jobdict['jobnum'],
                  'numexpwrap': len(jobdict['tasks']),
                  'save_md5sum': config.getfull('save_md5sum'),
                  'usedb': config.getfull(pfwdefs.PF_USE_DB_OUT),
                  'useqcf': config.getfull(pfwdefs.PF_USE_QCF),
                  'pipeprod': config.getfull('pipeprod'),
                  'pipever': config.getfull('pipever'),
                  'jobkeys': jobkey[1:].replace('_', ','),
                  pfwdefs.SW_ARCHIVESECT: config[pfwdefs.SW_ARCHIVESECT],
                  'output_wcl_tar': jobdict['outputwcltar'],
                  'envfile': jobdict['envfile'],
                  'junktar': config.get_filename('junktar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}}),
                  'junktar_archive_path': config.get_filepath('ops', 'junktar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}}),
                  'fw_groups': fwgroups,
                  'verify_files': config.getfull(pfwdefs.PF_VERIFY_FILES),
                  'maxthread_used': config.getfull('maxthread_used'),
                  'qcf': config.getfull('qcf'),
                  })

    if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
        jobwcl['task_id'] = {'attempt': config['task_id']['attempt'],
                             'block': config['task_id']['block'][config.getfull(pfwdefs.PF_BLKNUM)],
                             'job': config['task_id']['job'][jobdict['jobnum']]}
    else:
        jobwcl['task_id'] = {'attempt': -1,
                             'block': -2,
                             'job': -3}


    (_, create_junk_tarball) = config.search(pfwdefs.CREATE_JUNK_TARBALL, {intgdefs.REPLACE_VARS: True})
    jobwcl[pfwdefs.CREATE_JUNK_TARBALL] = miscutils.convertBool(create_junk_tarball)

    if 'transfer_stats' in config:
        jobwcl['transfer_stats'] = config.getfull('transfer_stats')

    # compression
    if pfwdefs.MASTER_COMPRESSION in config:
        jobwcl[pfwdefs.MASTER_COMPRESSION] = config.getfull(pfwdefs.MASTER_COMPRESSION).lower()
    else:
        jobwcl[pfwdefs.MASTER_COMPRESSION] = pfwdefs.MASTER_COMPRESSION_DEFAULT.lower()

    if pfwdefs.COMPRESSION_CLEANUP in config:
        jobwcl[pfwdefs.COMPRESSION_CLEANUP] = config.getfull(pfwdefs.COMPRESSION_CLEANUP)
    else:
        jobwcl[pfwdefs.COMPRESSION_CLEANUP] = pfwdefs.COMPRESSION_CLEANUP_DEFAULT

    if jobwcl[pfwdefs.MASTER_COMPRESSION] != 'never':
        for key in [pfwdefs.COMPRESSION_EXEC,
                    pfwdefs.COMPRESSION_ARGS,
                    pfwdefs.COMPRESSION_SUFFIX,
                    pfwdefs.COMPRESSION_CLEANUP]:
            if key in config:
                jobwcl[key] = config.get(key)

    # copy transfer_semname keys to jobwcl
    for tsemname in ['input_transfer_semname_target',
                     'input_transfer_semname_home',
                     'input_transfer_semname',
                     'output_transfer_semname_target',
                     'output_transfer_semname_home',
                     'output_transfer_semname',
                     'transfer_semname']:
        if tsemname in config:
            jobwcl[tsemname] = config.getfull(tsemname)

    if pfwdefs.MASTER_SAVE_FILE in config:
        jobwcl[pfwdefs.MASTER_SAVE_FILE] = config.getfull(pfwdefs.MASTER_SAVE_FILE)
    else:
        jobwcl[pfwdefs.MASTER_SAVE_FILE] = pfwdefs.MASTER_SAVE_FILE_DEFAULT


    target_archive = init_use_archive_info(config, jobwcl, pfwdefs.USE_TARGET_ARCHIVE_INPUT,
                                           pfwdefs.USE_TARGET_ARCHIVE_OUTPUT, pfwdefs.TARGET_ARCHIVE)
    home_archive = init_use_archive_info(config, jobwcl, pfwdefs.USE_HOME_ARCHIVE_INPUT,
                                         pfwdefs.USE_HOME_ARCHIVE_OUTPUT, pfwdefs.HOME_ARCHIVE)


    # include variables needed by target archive's file mgmt class
    if jobwcl[pfwdefs.TARGET_ARCHIVE] is not None:
        try:
            filemgmt_class = miscutils.dynamically_load_class(config[pfwdefs.SW_ARCHIVESECT][target_archive]['filemgmt'])
            valdict = config.get_param_info(filemgmt_class.requested_config_vals())
            jobwcl.update(valdict)
        except Exception as err:
            print(f"ERROR\nError: creating loading job_file_mvmt class\n{err}")
            raise

    # include variables needed by home archive's file mgmt class
    if jobwcl[pfwdefs.HOME_ARCHIVE] is not None:
        try:
            filemgmt_class = miscutils.dynamically_load_class(config[pfwdefs.SW_ARCHIVESECT][home_archive]['filemgmt'])
            valdict = config.get_param_info(filemgmt_class.requested_config_vals(),
                                            {pfwdefs.PF_CURRVALS: config[pfwdefs.SW_ARCHIVESECT][home_archive]})
            jobwcl.update(valdict)
        except Exception as err:
            print(f"ERROR\nError: creating loading job_file_mvmt class\n{err}")
            raise

    try:
        jobwcl['job_file_mvmt'] = config['job_file_mvmt'][config.getfull('curr_site')][home_archive][target_archive]
    except:
        print(f"\n\n\nError: Problem trying to find: config['job_file_mvmt'][{config.getfull('curr_site')}][{home_archive}][{target_archive}]")
        print("USE_HOME_ARCHIVE_INPUT =", jobwcl[pfwdefs.USE_HOME_ARCHIVE_INPUT])
        print("USE_HOME_ARCHIVE_OUTPUT =", jobwcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT])
        print("site =", config.getfull('curr_site'))
        print("home_archive =", home_archive)
        print("target_archive =", target_archive)
        print("job_file_mvmt =")
        miscutils.pretty_print_dict(config['job_file_mvmt'])
        print("\n")
        raise

    # include variables needed by job_file_mvmt class
    try:
        jobfilemvmt_class = miscutils.dynamically_load_class(jobwcl['job_file_mvmt']['mvmtclass'])
        valdict = config.get_param_info(jobfilemvmt_class.requested_config_vals(),
                                        {pfwdefs.PF_CURRVALS: jobwcl['job_file_mvmt']})
        jobwcl.update(valdict)
    except Exception as err:
        print(f"ERROR\nError: creating loading job_file_mvmt class\n{err}")
        raise

    if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
        if send_services:
            jobwcl['des_services'] = "REPLACE_SERVICES"
        elif 'target_des_services' in config and config.getfull('target_des_services') is not None:
            jobwcl['des_services'] = config.getfull('target_des_services')
        jobwcl['des_db_section'] = config['target_des_db_section']

    jobwcl['filetype_metadata'] = config['filetype_metadata']
    jobwcl['file_header'] = config['file_header']
    jobwcl['filename_pattern'] = config['filename_pattern']
    jobwcl['directory_pattern'] = config['directory_pattern']
    jobwcl[pfwdefs.IW_EXEC_DEF] = config[pfwdefs.SW_EXEC_DEF]
    #jobwcl['wrapinputs'] = jobdict['wrapinputs']

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"jobwcl.keys() = {list(jobwcl.keys())}")

    tjpad = pfwutils.pad_jobnum(jobdict['jobnum'])
    miscutils.coremakedirs(tjpad)
    with open(f"{tjpad}/{jobdict['jobwclfile']}", 'w') as wclfh:
        jobwcl.write(wclfh, True, 4)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


#######################################################################
def add_needed_values(config, modname, wrapinst, wrapwcl):
    """ Make sure all variables in the wrapper instance have values in the wcl """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG {modname}")

    # start with those needed by framework
    neededvals = {pfwdefs.REQNUM: config.getfull(pfwdefs.REQNUM,
                                                 {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                  'searchobj': wrapinst}),
                  pfwdefs.UNITNAME:config.getfull(pfwdefs.UNITNAME,
                                                  {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                   'searchobj': wrapinst}),
                  pfwdefs.ATTNUM: config.getfull(pfwdefs.ATTNUM,
                                                 {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                  'searchobj': wrapinst}),
                  pfwdefs.PF_BLKNUM: config.getfull(pfwdefs.PF_BLKNUM,
                                                    {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                     'searchobj': wrapinst}),
                  pfwdefs.PF_JOBNUM: config.getfull(pfwdefs.PF_JOBNUM,
                                                    {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                     'searchobj': wrapinst}),
                  pfwdefs.PF_WRAPNUM: config.getfull(pfwdefs.PF_WRAPNUM,
                                                     {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                      'searchobj': wrapinst}),
                 }

    # start with specified
    if 'req_vals' in config[pfwdefs.SW_MODULESECT][modname]:
        for rval in miscutils.fwsplit(config[pfwdefs.SW_MODULESECT][modname]['req_vals']):
            neededvals[rval] = True

    # go through all values in wcl
    #miscutils.pretty_print_dict(wrapwcl)
    neededvals.update(pfwutils.search_wcl_for_variables(wrapwcl))


    # add neededvals to wcl (values can also contain vars)
    done = False
    count = 0
    maxtries = 1000
    while not done and count < maxtries:
        done = True
        count += 1
        for nval in list(neededvals.keys()):
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"nval = {nval}")
            if isinstance(neededvals[nval], bool):
                if ':' in nval:
                    nval = nval.split(':')[0]

                if nval in ['qoutfile']:
                    val = nval
                else:
                    try:
                        (found, val) = config.search(nval,
                                                     {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                      'searchobj': wrapinst,
                                                      'required': False,
                                                      intgdefs.REPLACE_VARS: False})
                    except:
                        print("Why  config.search threw an error")

                    if not found:
                        try:
                            val = pfwutils.get_wcl_value(nval, wrapwcl)
                        except KeyError as err:
                            print("----- Searching for value in wcl:", nval)
                            print(wrapwcl.write())
                            raise err



                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"val = {val}")

                neededvals[nval] = val
                viter = [m.group(1) for m in re.finditer(r'(?i)\$\{([^}]+)\}', str(val))]
                for vstr in viter:
                    if ':' in vstr:
                        vstr = vstr.split(':')[0]
                    if vstr not in neededvals:
                        neededvals[vstr] = True
                        done = False

    if count >= maxtries:
        raise Exception("Error: exceeded maxtries")


    # add needed values to wrapper wcl
    for key, val in neededvals.items():
        pfwutils.set_wcl_value(key, val, wrapwcl)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


#######################################################################
def create_wrapper_inst(config, modname, loopvals):
    """ Create set of empty wrapper instances """

    miscutils.fwdebug_print(f"BEG {modname}")
    wrapperinst = collections.OrderedDict()
    (found, loopkeys) = config.search('wrapperloop',
                                      {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                       'required': False,
                                       intgdefs.REPLACE_VARS: True})
    wrapperinst = collections.OrderedDict()
    if found:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"loopkeys = {loopkeys}")
        loopkeys = miscutils.fwsplit(loopkeys.lower())
        #loopkeys.sort()  # sort so can make same key easily

        for instvals in sorted(loopvals):
            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"creating instance for {str(instvals)}")

            config.inc_wrapnum()
            winst = {pfwdefs.PF_WRAPNUM: config[pfwdefs.PF_WRAPNUM]}

            if len(instvals) != len(loopkeys):
                miscutils.fwdebug_print("Error: invalid number of values for instance")
                miscutils.fwdebug_print(f"\t{len(loopkeys):d} loopkeys ({loopkeys})")
                miscutils.fwdebug_print(f"\t{len(instvals):d} instvals ({instvals})")
                raise IndexError("Invalid number of values for instance")

            try:
                instkey = ""
                for k, lkey in enumerate(loopkeys):
                    winst[lkey] = instvals[k]
                    instkey += '_' + instvals[k]
            except:
                miscutils.fwdebug_print("Error: problem trying to create wrapper instance")
                miscutils.fwdebug_print(f"\tWas creating instance for {str(instvals)}")
                miscutils.fwdebug_print(f"\tloopkeys = {loopkeys}")
                raise

            winst['wrapkeys'] = instkey
            wrapperinst[instkey] = winst
    else:
        config.inc_wrapnum()
        wrapperinst['noloop'] = {pfwdefs.PF_WRAPNUM: config[pfwdefs.PF_WRAPNUM],
                                 'wrapkeys': 'noloop'}

    miscutils.fwdebug_print(f"Number wrapper inst: {len(wrapperinst)}")
    if not wrapperinst:
        miscutils.fwdebug_print("Error: 0 wrapper inst")
        raise Exception("Error: 0 wrapper instances")

    miscutils.fwdebug_print("END\n\n")
    return wrapperinst



#####################################################################
def create_new_filename(config, fsectname, fsectdict, sobj, currvals):

    miscutils.fwdebug_print("BEG")

    new_sobj = copy.deepcopy(fsectdict)
    new_sobj.update(sobj)

    # see if wcl specifies filename directly
    if 'filename' in fsectdict:
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"filename in {fsectname}")

        filename = config.get('filename', {pfwdefs.PF_CURRVALS: currvals,
                                           'searchobj': sobj,
                                           'expand': False,
                                           'required': True,
                                           intgdefs.REPLACE_VARS:False})

        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"filename = {filename}")
    else:
        # create filename from pattern
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"creating filename for {fsectname}")
            miscutils.fwdebug_print(f"\tfsectdict = {fsectdict}")
            miscutils.fwdebug_print(f"\tsobj = {sobj}")
            miscutils.fwdebug_print(f"\tnews_obj = {new_sobj}")

        filename = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
                                              'searchobj': new_sobj,
                                              'expand': False,
                                              intgdefs.REPLACE_VARS:False})

    fileinfo = replfuncs.replace_vars(filename, config,
                                      {pfwdefs.PF_CURRVALS: currvals,
                                       'searchobj': new_sobj,
                                       'expand': True,
                                       intgdefs.REPLACE_VARS:True,
                                       'keepvars': True})
    if fileinfo is None:
        miscutils.fwdie(f"empty fileinfo {fsectname}", pfwdefs.PF_EXIT_FAILURE)

    # save file info as if we read from query
    fnames = fileinfo[0]
    filelist = []
    if isinstance(fnames, list):
        for cnt, val in enumerate(fnames):
            finfo = fileinfo[1][cnt]
            finfo['filename'] = val
            filelist.append(finfo)
    else:
        finfo = fileinfo[1]
        finfo['filename'] = fnames
        filelist.append(finfo)

    return filelist


#####################################################################
def create_new_depends_filenames(config, master, modname, flabel):
    """ Create new filenames for output files that depended upon input data """

    miscutils.fwdebug_print(f"BEG {modname} {flabel}")

    moddict = config[pfwdefs.SW_MODULESECT][modname]
    currvals = {'curr_module': modname}
    fsectdict = moddict[pfwdefs.SW_FILESECT][flabel]

    for _, ldict in master['list'][intgdefs.LISTENTRY].items():
        for fnickname in ldict['file'].keys():
            newfinfo = copy.deepcopy(ldict['file'][fnickname])
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"fnickname={fnickname}, newfinfo={newfinfo}")

            if 'filename' in newfinfo:
                del newfinfo['filename']
                if 'compression' in newfinfo:
                    del newfinfo['compression']
                if 'fullname' in newfinfo:
                    del newfinfo['fullname']

                sobj = copy.deepcopy(newfinfo)
                sobj.update(fsectdict)

                filelist = create_new_filename(config, flabel, fsectdict, sobj, currvals)
                #print type(filelist), filelist
                if len(filelist) == 1:
                    ###newfinfo = filelist[0]
                    newfinfo.update(filelist[0])
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"fnickname={fnickname}, newfinfo={newfinfo}")
                ldict['file'][fnickname] = newfinfo

    miscutils.fwdebug_print("END\n\n")



#####################################################################
def fix_master_lists(config, modname, masterdata, theoutputs):
    """ Replace filename for master data copied as depend for output file """

    miscutils.fwdebug_print(f"BEG {modname}")

    # create python list of files and lists for this module
    searchobj = config.combine_lists_files(modname)

    for (sname, sdict) in searchobj:
        if 'depends-newname' in sdict:   # depends
            miscutils.fwdebug_print(f"need to fix filenames {sname}")
            master = masterdata[modname][sname]
            checksect = sname
            if checksect.startswith(pfwdefs.SW_LISTSECT):
                columns = get_list_all_columns(sdict, False)
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"columns={columns}")

                for collist in columns:
                    for col in collist:
                        match = re.search(r"(\S+).fullname", col)
                        if match:
                            flabel = match.group(1)
                            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                                miscutils.fwdebug_print(f"flabel={flabel}")
                            create_new_depends_filenames(config, master, modname, flabel)
                        else:
                            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                                miscutils.fwdebug_print(f"skipping column {col} since not file name")
            else:  # file
                #miscutils.fwdebug_print("sname=%s" % sname)
                match = re.search(fr"{pfwdefs.SW_FILESECT}-(\S+)", sname)
                if match:
                    flabel = match.group(1)
                    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"flabel={flabel}")
                    create_new_depends_filenames(config, master, modname, flabel)
                else:
                    raise KeyError(f"Bad file section name {sname}")
    miscutils.fwdebug_print("END\n\n")


#####################################################################
def read_master_lists(config, modname, masterdata, modules_prev_in_list):
    """ Read master lists and files from files created earlier """
    miscutils.fwdebug_print(f"BEG {modname}")

    # create python list of files and lists for this module
    searchobj = config.combine_lists_files(modname)

    #print "read master list order:  ", searchobj

    for (sname, sdict) in searchobj:
        #print sname
        # get filename for file containing dataset
        if 'qoutfile' in sdict:
            qoutfile = sdict['qoutfile']
            print(f"\t\t{sname}: reading master dataset from {qoutfile}")

            qouttype = intgdefs.DEFAULT_QUERY_OUTPUT_FORMAT
            if 'qouttype' in sdict:
                qouttype = sdict['qouttype']

            # read dataset file
            starttime = time.time()
            print("\t\t\tReading file - start ", starttime)
            if qouttype == 'json':
                master = None
                with open(qoutfile, 'r') as jsonfh:
                    master = json.load(jsonfh)
            elif qouttype == 'xml':
                raise Exception("xml datasets not supported yet")
            elif qouttype == 'wcl':
                master = WCL()
                with open(qoutfile, 'r') as wclfh:
                    master.read(wclfh, filename=qoutfile)
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"master.keys() = {list(master.keys())}")
            else:
                raise Exception(f"Unsupported dataset format in qoutfile for object {sname} in module {modname} ({qoutfile}) ")
            endtime = time.time()
            print("\t\t\tReading file - end ", endtime)
            print(f"\t\t\tReading file took {endtime - starttime} seconds")

            numlines = len(master['list'][intgdefs.LISTENTRY])
            print(f"\t\t\tNumber of lines in dataset {sname}: {numlines}\n")

            if numlines == 0:
                raise Exception(f"ERROR: 0 lines in dataset {sname} in module {modname}")

            #sdict['master'] = master
            if modname not in masterdata:
                masterdata[modname] = collections.OrderedDict()
            masterdata[modname][sname] = master
        elif pfwdefs.DATA_DEPENDS in sdict or 'depends-newname' in sdict:   # depends
            # = modname.filesect.filelabel
            # = modname.listsect.listlabel.filelabel

            tempdict = {}
            if modname not in masterdata:
                masterdata[modname] = collections.OrderedDict()

            print(f"\t\t{modname}-{sname}: depends in sdict")
            deplist = []
            if pfwdefs.DATA_DEPENDS in sdict:
                deplist = sdict[pfwdefs.DATA_DEPENDS].lower().split(",")
            else:
                deplist = sdict['depends-newname'].lower().split(",")
            count = 1
            for dep in deplist:
                depends = None
                depends = miscutils.fwsplit(dep, '.')

                dkey = f"{depends[1]}-{depends[2]}"
                if depends[0] in masterdata and dkey in masterdata[depends[0]]:
                    if len(depends) == 3:
                        tempdict.update(copy_master(masterdata[depends[0]][dkey], None, count))
                    else:
                        tempdict.update(copy_master(masterdata[depends[0]][dkey], depends[3], count))
                    count = len(tempdict) + 1
                else:
                    print("Error.  Debugging info:")
                    print('modname = ', modname)
                    print('sname = ', sname)
                    print('depends =', depends)
                    print('dkey =', dkey)
                    print('masterdata keys=', list(masterdata.keys()))
                    if depends[0] in masterdata:
                        print(f"masterdata[{depends[0]}].keys()={list(masterdata[depends[0]].keys())}")
                    miscutils.fwdie("ERROR: Could not find data for depends", pfwdefs.PF_EXIT_FAILURE)
            masterdata[modname][sname] = {'list': {intgdefs.LISTENTRY: tempdict}}
            #print "\n\nLENGTH ",len(tempdict)

    miscutils.fwdebug_print("END\n\n")


#######################################################################
def remove_column_format(columns):
    """ Return columns minus any formatting specification """

    columns2 = []
    for col in columns:
        if col.startswith('$FMT{'):
            rmatch = re.match(r'\$FMT\{\s*([^,]+)\s*,\s*(\S+)\s*\}', col)
            if rmatch:
                columns2.append(rmatch.group(2).strip())
            else:
                miscutils.fwdie(f"Error: invalid FMT column: {col}", pfwdefs.PF_EXIT_FAILURE)
        else:
            columns2.append(col)
    return columns2


#######################################################################
def convert_col_string_to_list(colstr, with_format=True):
    """ Convert a column string to list of columns """
    columns = re.findall(r'\$\S+\{.*\}|[^,\s]+', colstr)

    if not with_format:
        columns = remove_column_format(columns)
    return columns


#######################################################################
def get_list_all_columns(ldict, with_format=True):
    """ For a list definition, return list of columns in all list files """
    columns = []
    if pfwdefs.DIV_LIST_BY_COL in ldict:
        for divcoldict in ldict[pfwdefs.DIV_LIST_BY_COL].values():
            columns.append(convert_col_string_to_list(divcoldict['columns'], with_format))
    elif 'columns' in ldict:
        columns.append(convert_col_string_to_list(ldict['columns'], with_format))
    else:
        miscutils.fwdebug_print("columns not in ldict, so defaulting to fullname")
        columns.append(['fullname'])

    #print "get_list_all_columns: columns=", columns
    return columns



#######################################################################
def create_fullnames(config, modname, masterdata):
    """ add paths to filenames """    # what about compression extension

    miscutils.fwdebug_print(f"BEG {modname}")
    dataset = config.combine_lists_files(modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    for (sname, sdict) in dataset:
        if modname in masterdata and sname in masterdata[modname]:
            master = masterdata[modname][sname]
            numlines = len(master['list'][intgdefs.LISTENTRY])
            print(f"\t{modname}-{sname}: number of lines in master = {numlines}")
            if numlines == 0:
                miscutils.fwdie("Error: 0 lines in master list", pfwdefs.PF_EXIT_FAILURE)


            if pfwdefs.DIV_LIST_BY_COL in sdict or 'columns' in sdict:  # list
                miscutils.fwdebug_print(f"list sect: sname={sname}")
                dictcurr = collections.OrderedDict()
                columns = get_list_all_columns(sdict, False)
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"columns={columns}")

                for collist in columns:
                    for col in collist:
                        match = re.search(r"(\S+).fullname", col)
                        if match:
                            flabel = match.group(1)
                            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                                miscutils.fwdebug_print(f"flabel={flabel}")
                            if flabel in moddict[pfwdefs.SW_FILESECT]:
                                dictcurr[flabel] = copy.deepcopy(moddict[pfwdefs.SW_FILESECT][flabel])
                                dictcurr[flabel]['curr_module'] = modname
                            else:
                                print("list files = ", list(moddict[pfwdefs.SW_FILESECT].keys()))
                                miscutils.fwdie(f"Error: Looking at list columns - could not find {flabel} def in dataset",
                                                pfwdefs.PF_EXIT_FAILURE)
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"dictcurr={dictcurr}")

                for llabel, ldict in master['list'][intgdefs.LISTENTRY].items():
                    for flabel, fdict in ldict['file'].items():
                        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print(f"flabel={flabel}, fdict={fdict}")
                        if 'fullname' not in fdict:
                            if flabel in dictcurr:
                                fdict['fullname'] = add_runtime_path(config, dictcurr[flabel],
                                                                     flabel, fdict,
                                                                     fdict['filename'])[0]
                            elif len(dictcurr) == 1:
                                fdict['fullname'] = add_runtime_path(config, list(dictcurr.values())[0],
                                                                     flabel, fdict,
                                                                     fdict['filename'])[0]
                            else:
                                print("dictcurr: ", list(dictcurr.keys()))
                                miscutils.fwdie(f"Error: Looking at lines - could not find {flabel} def in dictcurr", pfwdefs.PF_EXIT_FAILURE)
                        elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print(f"fullname already in fdict: flabel={flabel}")


            else:  # file
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"file sect: sname={sname}")
                currvals = copy.deepcopy(sdict)
                currvals['curr_module'] = modname

                for llabel, ldict in master['list'][intgdefs.LISTENTRY].items():
                    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"file sect: llabel={llabel}")
                    for flabel, fdict in ldict['file'].items():
                        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print(f"file sect: flabel={flabel}")
                        if miscutils.fwdebug_check(10, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print(f"fdict: fdict={fdict}")
                        fdict['fullname'] = add_runtime_path(config, currvals, flabel,
                                                             fdict, fdict['filename'])[0]
        else:
            print(f"\t{modname}-{sname}: no masterlist...skipping")

    miscutils.fwdebug_print("END\n\n")



#######################################################################
def create_sublists(config, modname, masterdata):
    """ break master lists into sublists based upon match or divide_by """
    miscutils.fwdebug_print(f"BEG {modname}")
    dataset = config.combine_lists_files(modname)

    sublists = collections.OrderedDict()
    for (sname, sdict) in dataset:
        if modname in masterdata and sname in masterdata[modname]:
            master = masterdata[modname][sname]
            numlines = len(master['list'][intgdefs.LISTENTRY])
            print(f"\t{modname}-{sname}: number of lines in master = {numlines}")
            if numlines == 0:
                miscutils.fwdie("Error: 0 lines in master list", pfwdefs.PF_EXIT_FAILURE)

            sublists[sname] = collections.OrderedDict()
            keys = get_match_keys(sdict)

            if keys:
                sdict['keyvals'] = collections.OrderedDict()
                print(f"\t{modname}-{sname}: dividing by {keys}")
                for linenick, linedict in master['list'][intgdefs.LISTENTRY].items():
                    index = ""
                    listkeys = []
                    for key in keys:
                        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print(f"key = {key}")
                            miscutils.fwdebug_print(f"linedict = {linedict}")
                        val = get_value_from_line(linedict, key, None, 1)
                        index += val + '_'
                        listkeys.append(val)
                    sdict['keyvals'][index] = listkeys
                    if index not in sublists[sname]:
                        sublists[sname][index] = {'list': {intgdefs.LISTENTRY: collections.OrderedDict()}}
                    sublists[sname][index]['list'][intgdefs.LISTENTRY][linenick] = copy.deepcopy(linedict)
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"index = {index}")
                        miscutils.fwdebug_print(f"listkeys = {listkeys}")

            else:
                sublists[sname]['onlyone'] = copy.deepcopy(master)

        else:
            print(f"\t{modname}-{sname}: no masterlist...skipping")

    miscutils.fwdebug_print("END\n\n")
    return sublists


#######################################################################
def get_wrap_iter_obj_key(config, moddict):
    """ get wrapper iter object key """
    iter_obj_key = None
    if 'loopobj' in moddict:
        iter_obj_key = moddict['loopobj'].lower()
    else:
        miscutils.fwdebug_print(f"Could not find loopobj in modict {moddict}")
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"Could not find loopobj. moddict keys = {list(moddict.keys())}")
    return iter_obj_key


#######################################################################
def get_wrapper_loopvals(config, modname):
    """ get the values for the wrapper loop keys """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG {modname}")

    loopvals = []

    moddict = config[pfwdefs.SW_MODULESECT][modname]
    (found, loopkeys) = config.search('wrapperloop',
                                      {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                       'required': False, intgdefs.REPLACE_VARS: True})
    if found:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"\tloopkeys = {loopkeys}")
        loopkeys = miscutils.fwsplit(loopkeys.lower())
        #loopkeys.sort()  # sort so can make same key easily


        ## determine which list/file would determine loop values
        iter_obj_key = get_wrap_iter_obj_key(config, moddict)
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"iter_obj_key={iter_obj_key}")

        ## get wrapper loop values
        if iter_obj_key is not None:
            loopdict = pfwutils.get_wcl_value(iter_obj_key, moddict)
            ## check if loopobj has info from query
            if 'keyvals' in loopdict:
                loopvals = list(loopdict['keyvals'].values())
            else:
                miscutils.fwdebug_print(f"Warning: Couldn't find keyvals for loopobj {moddict['loopobj']}")
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"iter_obj_key={iter_obj_key}")
                    miscutils.fwdebug_print(f"moddict={moddict}")


        if not loopvals:
            print("\tDefaulting to wcl values")
            loopvals = []
            for key in loopkeys:
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"key={key}")
                (found, val) = config.search(key,
                                             {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                              'required': False,
                                              intgdefs.REPLACE_VARS: True})
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"found={found}")
                if found:
                    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"val={val}")
                    val = miscutils.fwsplit(val)
                    loopvals.append(val)
            loopvals = itertools.product(*loopvals)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return loopvals


#############################################################
def get_value_from_line(line, key, nickname=None, numvals=None):
    """ Return value from a line in master list """
    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG: key = {key}, nickname = {nickname}, numvals = {numvals}")
    # returns None if 0 matches
    #         scalar value if 1 match
    #         array if > 1 match

    # since values could be repeated across files in line,
    # create hash of values to get unique values
    valhash = collections.OrderedDict()

    key = key.lower()

    if '.' in key:
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("Found nickname")
        (nickname, key) = key.split('.')

    # is value defined at line level?
    if key in line:
        valhash[line[key]] = True

    # check files
    if 'file' in line:
        if nickname is not None:
            if nickname in line['file'] and key in line['file'][nickname]:
                try:
                    valhash[line['file'][nickname][key]] = True
                except:
                    miscutils.fwdebug_print("ERROR")
                    miscutils.fwdebug_print(f"valhash={valhash}")
                    miscutils.fwdebug_print(f"line['file'][{nickname}]={line['file'][nickname]}")
                    miscutils.fwdebug_print(f"line['file'][{nickname}][{key}]={line['file'][nickname][key]}")
                    miscutils.fwdebug_print(f"type(x)={type(line['file'][nickname][key])}")
                    raise
        else:
            for _, fdict in line['file'].items():
                if key in fdict:
                    valhash[fdict[key]] = True

    valarr = list(valhash.keys())

    if numvals is not None and len(valarr) != numvals:
        miscutils.fwdebug_print("Error: in get_value_from_line:")
        print(f"\tnumber found ({len(valarr)}) doesn't match requested ({numvals})\n")
        if nickname is not None:
            print("\tnickname =", nickname)

        print("\tvalue to find:", key)
        print("\tline:")
        miscutils.pretty_print_dict(line)
        print("\tvalarr:", valarr)
        miscutils.fwdie(f"Error: number found ({len(valarr)}) doesn't match requested ({numvals})",
                        pfwdefs.PF_EXIT_FAILURE)

    if not valarr:
        retval = None
    elif numvals == 1 or len(valarr) == 1:
        retval = str(valarr[0])
    else:
        retval = str(valarr)

    if hasattr(retval, "strip"):
        retval = retval.strip()

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return retval

#######################################################################
def get_wcl_metadata_keys(filetype, wrapper_wcl, currvals, config):
    """ Add to wrapper wcl any file metadata wcl values """

    wclkeys = set()
    for _, hdict in config['filetype_metadata'][filetype]['hdus'].items():
        for _, sdict in hdict.items():
            if 'w' in sdict:
                wclkeys.update(set(sdict['w'].keys()))

    return wclkeys

#######################################################################
def get_filetypes_output_files(moddict, outputfiles, wrapperwcl):
    """ Get the filetypes for all the output files """

    filetypes = []
    filesect = moddict[pfwdefs.SW_FILESECT]
    for ofile in outputfiles:
        ofsectkeys = ofile.split('.')
        ofsect = ofsectkeys[-1].lower()
        try:
            filetypes.append(filesect[ofsect]['filetype'])
        except:
            print('ofile =', ofile)
            print('ofsect =', ofsect)
            print("filesect.keys() = ", list(filesect.keys()))
            raise


    return filetypes

#######################################################################
# Assumes currvals includes specific values (e.g., band, ccd)
def create_single_wrapper_wcl(config, modname, wrapinst):
    """ create single wrapper wcl """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG {modname} {wrapinst[pfwdefs.PF_WRAPNUM]}")
    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"\twrapinst={wrapinst}")
    files = {"infiles": [],
             "outfiles": []}

    currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: wrapinst[pfwdefs.PF_WRAPNUM]}
    wrapperwcl = WCL({'modname': modname,
                      'wrapkeys': wrapinst['wrapkeys']})
    outfiles = []
    outlists = []
    infiles = []
    inlists = []
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    execs = intgmisc.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)
    for execkey, execval in execs.items():
        if pfwdefs.IW_INPUTS in execval:
            temp = replfuncs.replace_vars_single(execval[pfwdefs.IW_INPUTS], config,
                                                 {pfwdefs.PF_CURRVALS: currvals,
                                                  'searchobj': execval[pfwdefs.IW_INPUTS],
                                                  'required': True,
                                                  intgdefs.REPLACE_VARS: True})
            temp = temp.replace(' ', '')
            temp = temp.split(',')
            for item in temp:
                vals = item.split('.')
                if vals[0] == pfwdefs.SW_FILESECT:
                    infiles.append(vals[1])
                elif vals[0] == pfwdefs.SW_LISTSECT:
                    inlists.append(vals[1])

        if pfwdefs.IW_OUTPUTS in execval:
            temp = replfuncs.replace_vars_single(execval[pfwdefs.IW_OUTPUTS], config,
                                                 {pfwdefs.PF_CURRVALS: currvals,
                                                  'searchobj': execval[pfwdefs.IW_OUTPUTS],
                                                  'required': True,
                                                  intgdefs.REPLACE_VARS: True})
            temp = temp.replace(' ', '')
            temp = temp.split(',')
            for item in temp:
                vals = item.split('.')
                if vals[0] == pfwdefs.SW_FILESECT:
                    outfiles.append(vals[1])
                elif vals[0] == pfwdefs.SW_LISTSECT:
                    outlists.append(vals[1])

    # file is optional
    if pfwdefs.IW_FILESECT in wrapinst:
        wrapperwcl[pfwdefs.IW_FILESECT] = copy.deepcopy(wrapinst[pfwdefs.IW_FILESECT])
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"\tfile={wrapperwcl[pfwdefs.IW_FILESECT]}")
        for (sectname, sectdict) in wrapperwcl[pfwdefs.IW_FILESECT].items():
            sectdict['sectname'] = sectname
            isanoutput = False
            isaninput = False
            if sectname in outfiles:
                isanoutput = True
            if sectname in infiles:
                isaninput = True
            if 'fullname' in sectdict:
                if isanoutput:
                    files['outfiles'] += sectdict['fullname'].split(',')
                elif isaninput:
                    files['infiles'] += sectdict['fullname'].split(',')
            elif 'listonly' in sectdict and sectdict['listonly'] == 'True':
                pass
            else:
                print("MISSING", sectdict.items())

    # list is optional
    if pfwdefs.IW_LISTSECT in wrapinst:
        wrapperwcl[pfwdefs.IW_LISTSECT] = copy.deepcopy(wrapinst[pfwdefs.IW_LISTSECT])
        for k, v in wrapperwcl[pfwdefs.IW_LISTSECT].items():
            isoutlist = False
            isinlist = False
            if k in outlists:
                isoutlist = True
            elif k in inlists:
                isinlist = True

            if os.path.isfile(v['fullname']):
                cols = v['columns'].split(',')
                cc = -1
                for num, col in enumerate(cols):
                    if 'fullname' in col:
                        cc = num
                        break
                if cc != -1:
                    with open(v['fullname'], 'r') as fl:
                        rl = fl.readlines()
                        for line in rl:
                            temp = line.split()[cc]
                            temp = temp.replace(',', '')
                            if isoutlist:
                                files['outfiles'].append(temp.split('[')[0])
                            elif isinlist:
                                files['infiles'].append(temp.split('[')[0])

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"\tlist={wrapperwcl[pfwdefs.IW_LISTSECT]}")

    for typ in ['outfiles', 'infiles']:
        for num, ff in enumerate(files[typ]):
            # drop any direstory structure
            files[typ][num] = ff.split('/')[-1]

    # do we want exec_list variable?
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"\tpfwdefs.SW_EXECPREFIX={pfwdefs.SW_EXECPREFIX}")
    numexec = 0
    modname = currvals['curr_module']
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    execs = intgmisc.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)
    for execkey in execs:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"Working on exec section ({execkey})")
        numexec += 1
        iwkey = execkey.replace(pfwdefs.SW_EXECPREFIX, pfwdefs.IW_EXECPREFIX)
        wrapperwcl[iwkey] = collections.OrderedDict()
        execsect = moddict[execkey]
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"\t\t({execsect})")

        # get filetypes for adding wcl metadata to wrapper input wcl
        if pfwdefs.SW_OUTPUTS in execsect:
            filetypes = get_filetypes_output_files(moddict, miscutils.fwsplit(execsect[pfwdefs.OW_OUTPUTS]),
                                                   wrapperwcl)
            wclkeys = set()   # set to eliminate duplicates
            for ftype in filetypes:
                wclkeys.update(get_wcl_metadata_keys(ftype, wrapperwcl, currvals, config))

            for wkey in list(wclkeys):
                if wkey not in wrapperwcl:
                    wrapperwcl[wkey] = config.getfull(wkey,
                                                      {pfwdefs.PF_CURRVALS: currvals,
                                                       'searchobj': wrapinst})

        for key, val in execsect.items():
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"\t\t{key} ({val})")
            if key == pfwdefs.SW_INPUTS:
                iwexkey = pfwdefs.IW_INPUTS
            elif key == pfwdefs.SW_OUTPUTS:
                iwexkey = pfwdefs.IW_OUTPUTS
            elif key == pfwdefs.SW_ANCESTRY:
                iwexkey = pfwdefs.IW_ANCESTRY
            else:
                iwexkey = key

            if key != 'cmdline':
                wrapperwcl[iwkey][iwexkey] = replfuncs.replace_vars_single(val, config,
                                                                           {pfwdefs.PF_CURRVALS: currvals,
                                                                            'searchobj': val,
                                                                            'required': True,
                                                                            intgdefs.REPLACE_VARS: True})
            else:
                wrapperwcl[iwkey]['cmdline'] = copy.deepcopy(val)
        if 'execnum' not in wrapperwcl[execkey]:
            result = re.match(fr"{pfwdefs.IW_EXECPREFIX}(\d+)", execkey)
            if not result:
                miscutils.fwdie(f"Error:  Could not determine execnum from exec label {execkey}",
                                pfwdefs.PF_EXIT_FAILURE)
            wrapperwcl[execkey]['execnum'] = result.group(1)

        execname = wrapperwcl[iwkey]['execname']
        if intgdefs.IW_EXEC_DEF in config:
            execdefs = config[intgdefs.IW_EXEC_DEF]
            if (execname.lower() in execdefs and
                    'version_flag' in execdefs[execname.lower()] and
                    'version_pattern' in execdefs[execname.lower()]):
                wrapperwcl[iwkey]['version_flag'] = execdefs[execname.lower()]['version_flag']
                wrapperwcl[iwkey]['version_pattern'] = execdefs[execname.lower()]['version_pattern']
            else:
                miscutils.fwdebug_print(f"Info:  Missing version keys for {execname}")

        else:
            print(f"why {intgdefs.IW_EXEC_DEF}")

    if pfwdefs.SW_WRAPSECT in config[pfwdefs.SW_MODULESECT][modname]:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"Copying wrapper section ({pfwdefs.SW_WRAPSECT})")
        wrapperwcl[pfwdefs.IW_WRAPSECT] = copy.deepcopy(config[pfwdefs.SW_MODULESECT][modname][pfwdefs.SW_WRAPSECT])

    if pfwdefs.IW_WRAPSECT not in wrapperwcl:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print(f"{modname} ({wrapinst[pfwdefs.PF_WRAPNUM]}): Initializing wrapper section ({pfwdefs.IW_WRAPSECT})")
        wrapperwcl[pfwdefs.IW_WRAPSECT] = collections.OrderedDict()
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipeline'] = config.getfull('pipeline')
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipeprod'] = config.getfull('pipeprod')
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipever'] = config.getfull('pipever')

    wrapperwcl[pfwdefs.IW_WRAPSECT]['wrappername'] = wrapinst['wrappername']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['outputwcl'] = wrapinst['outputwcl']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['tmpfile_prefix'] = config.getfull('tmpfile_prefix', {pfwdefs.PF_CURRVALS: currvals})
    wrapperwcl['log'] = wrapinst['log']
    wrapperwcl['log_archive_path'] = wrapinst['log_archive_path']

    if numexec == 0:
        miscutils.pretty_print_dict(config[pfwdefs.SW_MODULESECT][modname])
        raise Exception(f"Error:  Could not find an exec section for module {modname}")


    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")

    return wrapperwcl, files


# translate sw terms to iw terms in values if needed
def translate_sw_iw(config, wrapperwcl, modname, winst):
    """ Translate submit wcl keys to input wcl keys """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG {modname}")
    if miscutils.fwdebug_check(9, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"winst = {list(winst.keys())}")
    if miscutils.fwdebug_check(9, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"wrapperwcl = {list(wrapperwcl.keys())}")

    if ((pfwdefs.SW_FILESECT == pfwdefs.IW_FILESECT) and
            (pfwdefs.SW_LISTSECT == pfwdefs.IW_LISTSECT)):
        print("Skipping translation SW to IW")
    else:
        translation = [(pfwdefs.SW_FILESECT, pfwdefs.IW_FILESECT),
                       (pfwdefs.SW_LISTSECT, pfwdefs.IW_LISTSECT)]
        wcltodo = [wrapperwcl]
        while wcltodo:
            if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print(f"len(wcltodo) = {len(wcltodo)}")
            wcl = wcltodo.pop()
            for key, val in wcl.items():
                if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print(f"key = {key}")
                if isinstance(val, dict):
                    if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"append key = {key} ({list(val.keys())})")
                    wcltodo.append(val)
                elif isinstance(val, str):
                    if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"val = {val}, {type(val)}")
                    for (swkey, iwkey) in translation:
                        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print(f"\tbefore swkey = {swkey}, iwkey = {iwkey}, val = {val}")
                        val = re.sub(fr"^{swkey}\.", f"{iwkey}.", val)
                        val = val.replace(fr"{{{swkey}.", f"{{{iwkey}.")
                        val = val.replace(fr" {swkey}.", f" {iwkey}.")
                        val = val.replace(fr",{swkey}.", f",{iwkey}.")
                        val = val.replace(fr":{swkey}.", f":{iwkey}.")

                        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print(f"\tafter val = {val}")
                    if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print(f"final value = {val}")
                    wcl[key] = val

    #print "new wcl = ", wrapperwcl.write(sys.stdout, True, 4)
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")



#######################################################################
def create_module_wrapper_wcl(config, modname, winst):
    """ Create wcl for wrapper instances for a module """
    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"BEG {modname}")

    if modname not in config[pfwdefs.SW_MODULESECT]:
        raise Exception(f"Error: Could not find module description for module {modname}\n")

    wrapperwcl, files = create_single_wrapper_wcl(config, modname, winst)
    translate_sw_iw(config, wrapperwcl, modname, winst)
    add_needed_values(config, modname, winst, wrapperwcl)
    write_wrapper_wcl(config, winst['inputwcl'], wrapperwcl)

    (exists, val) = config.search(pfwdefs.SW_WRAPPER_DEBUG,
                                  {pfwdefs.PF_CURRVALS: {'curr_module': modname}})
    if exists:
        winst['wrapdebug'] = val
    else:
        winst['wrapdebug'] = 0

    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")

    return files

#######################################################################
def divide_into_jobs(config, modname, winst, joblist, parlist):
    """ Divide wrapper instances into jobs """
    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG")

    if pfwdefs.SW_DIVIDE_JOBS_BY not in config and len(joblist) > 1:
        miscutils.fwdie(f"Error: no {pfwdefs.SW_DIVIDE_JOBS_BY} in config, but already > 1 job",
                        pfwdefs.PF_EXIT_FAILURE)

    key = '_nokey'
    if pfwdefs.SW_DIVIDE_JOBS_BY in config:
        key = ""
        for divb in miscutils.fwsplit(config.getfull(pfwdefs.SW_DIVIDE_JOBS_BY, {pfwdefs.PF_CURRVALS: {'curr_module':modname},
                                                                                 'searchobj': winst}), ','):
            key += '_' + config.getfull(divb, {pfwdefs.PF_CURRVALS: {'curr_module':modname},
                                               'searchobj': winst})


    if key not in joblist:
        #joblist[key] = {'tasks':[], 'inwcl':[], 'inlist':[], 'wrapinputs':OrderedDict(), 'parlist':{}}
        joblist[key] = {'tasks':[], 'inwcl':[], 'inlist':[], 'parlist':{}}

    maxthread = pfwdefs.MAX_FWTHREADS_DEFAULT

    if modname not in joblist[key]['parlist']:
        joblist[key]['parlist'][modname] = {'wrapnums': [],
                                            'fw_nthread': pfwdefs.MAX_FWTHREADS_DEFAULT,
                                            'fw_thread_reuse': pfwdefs.FWTHREADS_REUSE_DEFAULT}

        # check whether supposed to use FW multithreading  (check master on/off switch)
        usefwthreads = pfwdefs.MASTER_USE_FWTHREADS_DEFAULT
        if pfwdefs.MASTER_USE_FWTHREADS in config:
            usefwthreads = miscutils.convertBool(config.getfull('MASTER_USE_FWTHREADS'))

        # determine the number of fw threads for this module
        if usefwthreads:
            global_max_thread = int(config.getfull('fw_nmaxthread', default=maxthread))
            try:
                mthread = int(config.getfull(pfwdefs.MAX_FWTHREADS, {pfwdefs.PF_CURRVALS: {'curr_module': modname}}, default=1))
                if mthread is None:
                    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
                        miscutils.fwdebug_print(f"{pfwdefs.MAX_FWTHREADS} not found for module {modname}, defaulting to {pfwdefs.MAX_FWTHREADS_DEFAULT}")
                else:
                    maxthread = max(mthread, global_max_thread)
            except KeyError:
                if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
                    miscutils.fwdebug_print(f"{pfwdefs.MAX_FWTHREADS} not found for module {modname}, defaulting to {pfwdefs.MAX_FWTHREADS_DEFAULT}")
        joblist[key]['parlist'][modname]['fw_nthread'] = maxthread
        joblist[key]['parlist'][modname]['fw_thread_reuse'] = int(config.getfull('fwthread_reuse', default=pfwdefs.FWTHREADS_REUSE_DEFAULT))
    joblist[key]['parlist'][modname]['wrapnums'].append(winst[pfwdefs.PF_WRAPNUM])

    joblist[key]['tasks'].append([winst[pfwdefs.PF_WRAPNUM], winst['wrappername'], winst['inputwcl'], winst['wrapdebug'], winst['log']])
    joblist[key]['inwcl'].append(winst['inputwcl'])
    #if winst['wrapinputs'] is not None and len(winst['wrapinputs']) > 0:
    #    joblist[key]['wrapinputs'][winst[pfwdefs.PF_WRAPNUM]] = winst['wrapinputs']
    if pfwdefs.IW_LISTSECT in winst:
        for linfo in winst[pfwdefs.IW_LISTSECT].values():
            joblist[key]['inlist'].append(linfo['fullname'])

    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"number of job lists = {len(joblist)}")
        miscutils.fwdebug_print(f"\tkeys = {', '.join(list(joblist.keys()))}")
        miscutils.fwdebug_print("END\n")
    return maxthread


def write_runjob_script(config):
    """ Write runjob script """

    miscutils.fwdebug_print("BEG")

    jobdir = config.get_filepath('runtime', 'jobdir', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM: "$padjnum"}})
    print("The target jobdir =", jobdir)

    usedb = miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT])
    send_services = False
    services_file = None
    if pfwdefs.SEND_SERVICES_FILE in config:
        send_services = miscutils.convertBool(config[pfwdefs.SEND_SERVICES_FILE])
        services_file = os.path.basename(config['submit_des_services'])
    scriptfile = config.get_filename('runjob')

    #      Since wcl's variable syntax matches shell variable syntax and
    #      underscores are used to separate name parts, have to use place
    #      holder for jobnum and replace later with shell variable
    #      Otherwise, get_filename fails to substitute for padjnum
    envfile = config.get_filename('envfile', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:"9999"}})
    envfile = envfile.replace("j9999", "j${padjnum}")

    scriptstr = """#!/usr/bin/env sh
echo "PFW: job_shell_script cmd: $0 $@";
"""
    if send_services :
        scriptstr += """
if [ $# -ne 7 ]; then
    echo "Usage: $0 <jobnum> <input tar> <job wcl> <tasklist> <env file> <output tar> <services file>";
"""
    else:
        scriptstr += """
if [ $# -ne 6 ]; then
    echo "Usage: $0 <jobnum> <input tar> <job wcl> <tasklist> <env file> <output tar>";
"""
    scriptstr += """
    echo "PFW: job_shell_script exit_status: 1"
    exit 1;
fi
jobnum=$1

lenjobnum=`expr length "$jobnum"`
if [ $lenjobnum == 4 ]; then
    padjnum=$jobnum
else
    jobnum=$(echo $jobnum | sed 's/^0*//')
    padjnum=`printf %04d $jobnum`
fi
echo "jobnum = '$jobnum'"
echo "padjnum = '$padjnum'"

intar=$2
jobwcl=$3
tasklist=$4
envfile=$5
outputtar=$6
initdir=`pwd`

"""

    max_eups_tries = 3
    if 'max_eups_tries' in config:
        max_eups_tries = config.getfull('max_eups_tries')


    # setup job environment
    scriptstr += f"""
export SHELL=/bin/bash    # needed for setup to work in Condor environment
export PFW_JOB_START_EPOCH=`date "+%s"`
echo "PFW: job_shell_script starttime: $PFW_JOB_START_EPOCH"
echo -n "PFW: job_shell_script exechost: "
hostname
echo ""

BATCHID=""
if test -n "$SUBMIT_CONDORID"; then
    echo "PFW: condorid $SUBMIT_CONDORID"
fi

### Output batch jobid for record keeping
### specific to batch scheduler
if test -n "$PBS_JOBID"; then
   BATCHID=`echo $PBS_JOBID | cut -d'.' -f1`
   NP=`awk 'END {{print NR}}' $PBS_NODEFILE`
fi
if test -n "$LSB_JOBID"; then
   BATCHID=$LSB_JOBID
fi
if test -n "$LOADL_STEP_ID"; then
   BATCHID=`echo $LOADL_STEP_ID | awk -F "." '{{print $(NF-1) "." $(NF) }}'`
fi
if test -n "$BATCHID"; then
    echo "PFW: batchid $BATCHID"
fi

echo ""
echo ""
echo "Initial condor job directory = " $initdir
echo "Files copied over by condor:"
ls -la
echo ""
echo "Creating empty job output files to guarantee condor job nice exit"
touch $envfile
tar -cvf $outputtar --files-from /dev/null


d1=`date "+%s"`
echo "PFW: eups_setup starttime: $d1"
cnt=0
maxtries={max_eups_tries}
mydelay=300
mystat=1
while [ $mystat -ne 0 -a $cnt -lt $maxtries ]; do
    let cnt=cnt+1
    if [ ! -r {config.getfull('setupeups')} ]; then
        echo "Error: eups setup script is not readable ({config.getfull('setupeups')})"
        edir=`dirname {config.getfull('setupeups')}`
        echo $edir
        ls -l $edir || sleep 60
        mystat=1
    else
        echo "Sourcing script to set up EUPS ({config.getfull('setupeups')})"
        source {config.getfull('setupeups')}

        echo "Using eups to setup up {config.getfull('pipeprod')} {config.getfull('pipever')}"
        setup --nolock {config.getfull('pipeprod')} {config.getfull('pipever')}
        mystat=$?
        if [ $mystat -ne 0 ]; then
            echo "Warning: eups setup had non-zero exit code ($mystat)"
        fi
    fi
    if [ $mystat -ne 0 -a $cnt -lt $maxtries ]; then
        echo "Sleeping then retrying..."
        sleep $mydelay
    fi
done
d2=`date "+%s"`
echo "PFW: eups_setup endtime: $d2"
if [ $mystat != 0 ]; then
    echo "Error: eups setup had non-zero exit code ($mystat)"
    shd2=`date "+%s"`
    echo "PFW: job_shell_script endtime: $shd2"
    echo "PFW: job_shell_script exit_status: {pfwdefs.PF_EXIT_EUPS_FAILURE}"
    exit $mystat    # note exit code not passed back through grid universe jobs
fi
"""

    if not usedb:
        scriptstr += 'echo "DESDMTIME: eups_setup $((d2-d1)) secs"'

    # add any job environment from submit wcl
    scriptstr += 'echo ""\n'
    if pfwdefs.SW_JOB_ENVIRONMENT in config:
        for name, value in config[pfwdefs.SW_JOB_ENVIRONMENT].items():
            scriptstr += f'export {name.upper()}="{value}"\n'
    if send_services:
        scriptstr += f"chmod 600 $initdir/{services_file}\n"
        scriptstr += f"export DES_SERVICES=$initdir/{services_file}\n"
        scriptstr += f"sed -i \"s|REPLACE_SERVICES|$DES_SERVICES|g\" $jobwcl\n"
        scriptstr += 'echo ""\n'

    # print start of job information

    scriptstr += """
echo "Saving environment after setting up meta package to $envfile"
env | sort > $envfile
pwd
ls -l $envfile
"""

    if pfwdefs.SW_JOB_BASE_DIR in config and config.getfull(pfwdefs.SW_JOB_BASE_DIR) is not None:
        full_job_dir = config.getfull(pfwdefs.SW_JOB_BASE_DIR) + '/' + jobdir
        scriptstr += f"""
echo ""
jobdir={full_job_dir}
echo "Making target job's directory ($jobdir)"
if [ -e $jobdir ]; then
    echo "Job scratch directory already exists ($jobdir).   Aborting";
    exit 1;
fi

mkdir -p $jobdir

if [ ! -e $jobdir ]; then
    echo "Could not make job scratch directory ($jobdir).   Aborting";
    exit 1;
fi

cd $jobdir
        """
    else:
        print(f"{pfwdefs.SW_JOB_BASE_DIR} wasn't specified.   Running job in condor job directory")

    # untar file containing input wcl files
    scriptstr += """
echo ""
echo "Untaring input tar: $intar"
d1=`date "+%s"`
echo "PFW: untaring_input_tar starttime: $d1"
tar -xzf $initdir/$intar
d2=`date "+%s"`
echo "PFW: untaring_input_tar endtime: $d2"
"""
    if not usedb:
        scriptstr += 'echo "DESDMTIME: untar_input_tar $((d2-d1)) secs"'

    # copy files so can test by hand after job
    # save initial directory to job wcl file
    scriptstr += """
echo "Copying job wcl and task list to job working directory"
d1=`date "+%s"`
echo "PFW: copy_job_setup starttime: $d1"
cp $initdir/$jobwcl $jobwcl
cp $initdir/$tasklist $tasklist
d2=`date "+%s"`
echo "PFW: copy_job_setup endtime: $d2"
echo "condor_job_init_dir = " $initdir >> $jobwcl
"""
    if not usedb:
        scriptstr += 'echo "DESDMTIME: copy_jobwcl_tasklist $((d2-d1)) secs"'

    # special handling of services file from fermi
    if pfwdefs.SW_SITESECT in config:
        if 'dcache' in config[pfwdefs.SW_SITESECT][config['runsite']] and miscutils.convertBool(config[pfwdefs.SW_SITESECT][config['runsite']]['dcache']):
            scriptstr += f"""
source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh
ifdh cp /pnfs/des/persistent/{config['fermi_id']}/{config[pfwdefs.SW_SITESECT][config['runsite']]['target_des_services']} {config[pfwdefs.SW_SITESECT][config['runsite']]['target_des_services']}
export DES_SERVICES="`pwd`/{config[pfwdefs.SW_SITESECT][config['runsite']]['target_des_services']}"
# change the permissions of the services file as it comes over as 644 rather than 600
chmod 600 $DES_SERVICES
# replace the placeholder in the jobwcl with the proper value
sed -i "s|REPLACE_SERVICES|$DES_SERVICES|g" $jobwcl
"""
            # set a placeholder in the config
            config['target_des_services'] = "REPLACE_SERVICES"

    # call the job workflow program
    scriptstr += """
echo ""
echo "Calling pfwrunjob.py"
echo "cmd> ${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist"
d1=`date "+%s"`
echo "PFW: pfwrunjob starttime: $d1"
${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist
rjstat=$?
d2=`date "+%s"`
echo "PFW: pfwrunjob endtime: $d2"
echo ""
echo ""
"""
    if send_services:
        scriptstr += f"rm -f $DES_SERVICES\n"
    scriptstr += """
if [ -e outputwcl ]; then
    tar -cf $initdir/$outputtar outputwcl;
else
    echo "INFO:  No outputwcl directory at end of job";
fi
"""

    purge_job_dir = 'success'
    if 'purge_job_dir' in config:
        purge_job_dir = config.getfull('purge_job_dir').lower()

    if purge_job_dir == 'success':
        scriptstr += """
if [ $rjstat -eq 0 ]; then
    cd $initdir;
    echo "Purging job scratch directory ($jobdir)";
    rm -rf $jobdir;
else
    echo "Non-zero exit code, skipping purge of job scratch directory ($jobdir)";
fi
"""
    elif purge_job_dir == 'always':
        scriptstr += """
cd $initdir;
echo "Purging job scratch directory ($jobdir)";
rm -rf $jobdir;
"""


    scriptstr += """
shd2=`date "+%s"`
echo "PFW: job_shell_script endtime: $shd2"
echo "PFW: job_shell_script exit_status: $rjstat"
"""

    if not usedb:
        scriptstr += """
echo "DESDMTIME: pfwrunjob.py $((d2-d1)) secs"
echo "DESDMTIME: job_shell_script $((shd2-PFW_JOB_START_EPOCH)) secs"
"""

    scriptstr += "exit $rjstat"

    # write shell script to file
    with open(scriptfile, 'w') as scriptfh:
        scriptfh.write(scriptstr)

    os.chmod(scriptfile, stat.S_IRWXU | stat.S_IRWXG)

    miscutils.fwdebug_print("END\n\n")

    return scriptfile



#######################################################################
def create_jobmngr_dag(config, dagfile, scriptfile, joblist):
    """ Write job manager DAG file """

    miscutils.fwdebug_print("BEG")
    config['numjobs'] = len(joblist)
    condorfile = create_runjob_condorfile(config, scriptfile)

    pfwdir = config.getfull('processingfw_dir')
    blockname = config.getfull('blockname')
    blkdir = config.getfull('block_dir')

    use_condor_transfer_output = True
    if 'use_condor_transfer_output' in config:
        use_condor_transfer_output = miscutils.convertBool(config.getfull('use_condor_transfer_output'))

    send_services = False
    if pfwdefs.SEND_SERVICES_FILE in config:
        send_services = miscutils.convertBool(config[pfwdefs.SEND_SERVICES_FILE])
    with open(f"{blkdir}/{dagfile}", 'w') as dagfh:
        for _, jobdict in joblist.items():
            jobnum = jobdict['jobnum']
            tjpad = pfwutils.pad_jobnum(jobnum)

            dagfh.write(f"JOB {tjpad} {condorfile}\n")
            dagfh.write(f"VARS {tjpad} jobnum=\"{tjpad}\"\n")
            dagfh.write(f"VARS {tjpad} exec=\"../{scriptfile}\"\n")
            dagfh.write(f"VARS {tjpad} args=\"{jobnum} {jobdict['inputwcltar']} {jobdict['jobwclfile']} {jobdict['tasksfile']} {jobdict['envfile']} {jobdict['outputwcltar']}")
            if send_services:
                dagfh.write(f" {os.path.basename(config['submit_des_services'])}")
            dagfh.write("\"\n")
            dagfh.write(f"VARS {tjpad} transinput=\"{jobdict['inputwcltar']},{jobdict['jobwclfile']},{jobdict['tasksfile']},jobpost_{tjpad}.sh")
            if send_services:
                dagfh.write(f",{config['submit_des_services']}")
            dagfh.write("\"\n")
            if 'wall' in jobdict:
                dagfh.write(f"VARS {tjpad} wall=\"{jobdict['wall']}\"\n")

            if use_condor_transfer_output:
                dagfh.write(f"VARS {tjpad} transoutput=\"{jobdict['outputwcltar']},{jobdict['envfile']}\"\n")
            dagfh.write(f"SCRIPT pre {tjpad} {pfwdir}/libexec/jobpre.py ../uberctrl/config.des $JOB\n")
            dagfh.write(f"SCRIPT post {tjpad} {tjpad}/jobpost_{tjpad}.sh $RETURN\n")
            with open(f"{tjpad}/jobpost_{tjpad}.sh", 'w') as jpostfh:
                jpostfh.write("#!/usr/bin/env sh\n")
                jpostfh.write("sem --record-env\n")
                jpostfh.write(f"sem --fg --id jobpost -j 20 {pfwdir}/libexec/jobpost.py ../uberctrl/config.des {blockname} {tjpad} {jobdict['inputwcltar']} {jobdict['outputwcltar']} $1\n")
            os.chmod(f"{tjpad}/jobpost_{tjpad}.sh", stat.S_IRWXU | stat.S_IRWXG)


    miscutils.fwdebug_print("END\n\n")



#######################################################################
def tar_inputfiles(config, jobnum, inlist):
    """ Tar the input wcl files for a single job """
    inputtar = config.get_filename('inputwcltar', {pfwdefs.PF_CURRVALS:{'jobnum': jobnum}})
    tjpad = pfwutils.pad_jobnum(jobnum)
    miscutils.coremakedirs(tjpad)

    pfwutils.tar_list(f"{tjpad}/{inputtar}", inlist)
    return inputtar


#######################################################################
def create_runjob_condorfile(config, scriptfile):
    """ Write runjob condor description file for target job """
    miscutils.fwdebug_print("BEG")

    blkname = config.getfull('blockname')
    blockbase = config.get_filename('block', {pfwdefs.PF_CURRVALS: {'flabel': 'runjob', 'fsuffix':''}})
    initialdir = f"{config.getfull('block_dir')}/$(jobnum)"

    condorfile = f"{config.getfull('block_dir')}/{blockbase}condor"

    jobbase = config.get_filename('job', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:'$(jobnum)', 'flabel': 'runjob', 'fsuffix':''}})
    jobattribs = {'executable': f"{config.getfull('block_dir')}/{scriptfile}",
                  'arguments': '$(args)',
                  'initialdir': initialdir,
                  'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
                  'transfer_input_files': '$(transinput)',
                  'transfer_executable': 'True',
                  'notification': 'Never',
                  'output':f"{jobbase}out",
                  'error':f"{jobbase}err",
                  'log': f"{blockbase}log",
                  #'periodic_release': '((CurrentTime - EnteredCurrentStatus) > 1800) && (HoldReason =!= "via condor_hold (by user %s)")' % config.getfull('operator'),
                  #'periodic_remove' : '((JobStatus == 1) && (JobRunCount =!= Undefined))'
                  'periodic_remove': f"((JobStatus == 5) && (HoldReason =!= \"via condor_hold (by user {config.getfull('operator')})\"))",
                  'periodic_hold': '((NumJobStarts > 0) && (JobStatus == 1))'   # put jobs that have run once and are back in idle on hold
                  }


    userattribs = config.get_condor_attributes(blkname, '$(jobnum)')

    # set any job environment variables at the condor job level
    jobattribs['environment'] = {}
    for key, val in userattribs.items():
        jobattribs['environment'][key] = str(val)

    if 'condor_job_environment' in config:
        for key, val in config.get('condor_job_environment').items():
            jobattribs['environment'][key.upper()] = str(val)

    targetinfo = config.get_grid_info()
    if 'gridtype' not in targetinfo:
        miscutils.fwdie("Error:  Missing gridtype", pfwdefs.PF_EXIT_FAILURE)
    else:
        targetinfo['gridtype'] = targetinfo['gridtype'].lower()

    reqs = ['NumJobStarts == 0']   # don't want to rerun any job
    if targetinfo['gridtype'] == 'condor':
        jobattribs['universe'] = 'vanilla'

        if 'concurrency_limits' in config:
            jobattribs['concurrency_limits'] = config.getfull('concurrency_limits')
        if 'remote_initialdir' in config:
            jobattribs['remote_initialdir'] = config.getfull('remote_initialdir')
        if 'batchtype' not in targetinfo:
            miscutils.fwdie("Error: Missing batchtype", pfwdefs.PF_EXIT_FAILURE)
        else:
            targetinfo['batchtype'] = targetinfo['batchtype'].lower()

        if 'glidein' in targetinfo['batchtype']:
            if 'nodeset' in config and config.getfull('nodeset').lower() != 'none':
                userattribs['NODESET'] = config.getfull('nodeset')
                reqs.append(f"(Target.NODESET == \"{config.getfull('nodeset')}\")")
            elif 'uiddomain' in config:
                reqs.append(f"(UidDomain == \"{config.getfull('uiddomain')}\")")
            else:
                miscutils.fwdie("Error: Cannot determine uiddomain for matching to a glidein", pfwdefs.PF_EXIT_FAILURE)

            if 'glidein_name' in config and config.getfull('glidein_name').lower() != 'none':
                reqs.append(f"(Target.GLIDEIN_NAME == \"{config.getfull('glidein_name')}\")")

            reqs.append('(FileSystemDomain != "")')
            reqs.append('(Arch != "")')
            reqs.append('(OpSys != "")')
            reqs.append('(Disk != -1)')
            reqs.append('(Memory != -1)')

            if 'glidein_use_wall' in config and miscutils.convertBool(config.getfull('glidein_use_wall')):
                reqs.append(r"(TimeToLive > $(wall)*60)")   # wall is in mins, TimeToLive is in secs

        elif targetinfo['batchtype'] == 'local':
            jobattribs['universe'] = 'vanilla'
            if 'loginhost' in config:
                machine = config.getfull('loginhost')
            elif 'gridhost' in config:
                machine = config.getfull('gridhost')
            else:
                miscutils.fwdie("Error:  Cannot determine machine name (missing loginhost and gridhost)", pfwdefs.PF_EXIT_FAILURE)

            reqs.append(f'(machine == "{machine}")')
        elif targetinfo['batchtype'] == 'nodeset':
            if 'nodeset' in config and config.getfull('nodeset').lower() != 'none':
                userattribs['NODESET'] = config.getfull('nodeset')
                reqs.append(f"(Target.NODESET == \"{config.getfull('nodeset')}\")")

        if 'dynslots' in targetinfo['batchtype'] or \
           ('dynslots' in targetinfo and miscutils.convertBool(targetinfo['dynslots'])):
            if 'request_memory' in config:
                jobattribs['request_memory'] = config.getfull('request_memory')
            if 'request_cpus' in config:
                jobattribs['request_cpus'] = config.getfull('request_cpus')
    else:
        jobattribs['universe'] = 'grid'
        jobattribs['grid_resource'] = pfwcondor.create_resource(targetinfo)
        jobattribs['stream_output'] = 'False'
        jobattribs['stream_error'] = 'False'
        jobattribs['use_x509userproxy'] = 'True'   # required for condor-ce, defaults true for gt5
        use_condor_transfer_output = True
        if 'use_condor_transfer_output' in config:
            use_condor_transfer_output = miscutils.convertBool(config.getfull('use_condor_transfer_output'))
        if use_condor_transfer_output:
            jobattribs['transfer_output_files'] = '$(transoutput)'
        globus_rsl = pfwcondor.create_rsl(targetinfo)
        if globus_rsl:
            jobattribs['globus_rsl'] = globus_rsl
        if targetinfo['gridtype'] == 'condor-ce':
            if 'request_memory' in config:
                userattribs['maxMemory'] = int(config.getfull('request_memory'))
                jobattribs['request_memory'] = int(config.getfull('request_memory'))
            if 'request_cpus' in config:
                userattribs['xcount'] = int(config.getfull('request_cpus'))
            if 'request_disk' in config:
                jobattribs['request_disk'] = int(config.getfull('request_disk'))
            if 'condorjobclass' in config:
                userattribs['jobclass'] = config.getfull('condorjobclass')
            if 'condorjobreq' in config:
                reqs.append(config.getfull('condorjobreq'))

    if reqs:
        jobattribs['requirements'] = ' && '.join(reqs)

    pfwcondor.write_condor_descfile('runjob', condorfile, jobattribs, userattribs)

    miscutils.fwdebug_print("END\n\n")
    return condorfile


#######################################################################
def stage_inputs(config, inputfiles):
    """ Transfer inputs to target archive if using one """

    miscutils.fwdebug_print("BEG")
    miscutils.fwdebug_print(f"number of input files needed at target = {len(inputfiles)}")

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"input files {inputfiles}")

    if (pfwdefs.USE_HOME_ARCHIVE_INPUT in config and
            (config[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == pfwdefs.TARGET_ARCHIVE.lower() or
             config[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == 'all')):

        miscutils.fwdebug_print(f"home_archive = {config[pfwdefs.HOME_ARCHIVE]}")
        miscutils.fwdebug_print(f"target_archive = {config[pfwdefs.TARGET_ARCHIVE]}")
        sys.stdout.flush()
        sem = None
        if config.getfull('use_db'):
            if 'input_transfer_semname_prestage' in config:
                semname = config.getfull('input_transfer_semname_prestage')
            elif 'input_transfer_semname' in config:
                semname = config.getfull('input_transfer_semname')
            elif 'transfer_semname' in config:
                semname = config.getfull('transfer_semname')
            sem = dbsem.DBSemaphore(semname, None)
        archive_transfer_utils.archive_copy(config[pfwdefs.SW_ARCHIVESECT][config[pfwdefs.HOME_ARCHIVE]],
                                            config[pfwdefs.SW_ARCHIVESECT][config[pfwdefs.TARGET_ARCHIVE]],
                                            config.getfull('archive_transfer'),
                                            inputfiles, config)
        if sem is not None:
            del sem

    miscutils.fwdebug_print("END\n\n")



#######################################################################
def write_output_list(config, outputfiles):
    """ Write output list """

    miscutils.fwdebug_print("BEG")

    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print(f"output files {outputfiles}")

    if 'block_outputlist' not in config:
        miscutils.fwdie("Error:  Could not find block_outputlist in config.   Internal Error.", pfwdefs.PF_EXIT_FAILURE)

    with open(config.getfull('block_outputlist'), 'w') as outfh:
        for fname in outputfiles:
            outfh.write(f"{miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME)}\n")

    miscutils.fwdebug_print("END")


#######################################################################
def write_wrapper_wcl(config, filename, wrapperwcl):
    """ Write wrapper input wcl to file """

    if os.path.exists(filename):
        print(f"Error:   input wcl file already exists ({filename})")
        print("\t\tCheck modnamepat vs wrapperloop for a missing term in modnamepat")
        miscutils.fwdie("Input wcl file already exists", pfwdefs.PF_EXIT_FAILURE)
    else:
        wcldir = os.path.dirname(filename)
        miscutils.coremakedirs(wcldir)
        with open(filename, 'w') as wclfh:
            wrapperwcl.write(wclfh, True, 4)

######################################################################
def copy_input_lists_home_archive(config, filemgmt, archive_info, listfullnames):
    """ Copy list files to home archive """

    archdir = config.getfull(pfwdefs.ATTEMPT_ARCHIVE_PATH)
    if miscutils.fwdebug_check(6, 'BEGRUN_DEBUG'):
        miscutils.fwdebug_print(f"archive rel path = {archdir}")

    # copy the files to the home archive
    files2copy = {}
    for lfname in listfullnames:
        relpath = os.path.dirname(lfname)
        filename = miscutils.parse_fullname(lfname, miscutils.CU_PARSE_FILENAME)
        archfname = f"{archdir}/{relpath}/{filename}"
        files2copy[lfname] = {'src': lfname,
                              'filename': filename,
                              'dst': archfname,
                              'fullname': archfname}

    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
        miscutils.fwdebug_print(f"files2copy = {files2copy}")

    # load file mvmt class
    submit_files_mvmt = config.getfull('submit_files_mvmt')
    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
        miscutils.fwdebug_print(f"submit_files_mvmt = {submit_files_mvmt}")
    filemvmt_class = miscutils.dynamically_load_class(submit_files_mvmt)
    valdict = fmutils.get_config_vals(config['job_file_mvmt'], config,
                                      filemvmt_class.requested_config_vals())
    filemvmt = filemvmt_class(archive_info, None, None, None, valdict)

    results = filemvmt.job2home(files2copy)
    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
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
    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
        miscutils.fwdebug_print(f"files2register = {files2register}")
        miscutils.fwdebug_print(f"archive = {archive_info['name']}")
    filemgmt.register_file_in_archive(files2register, archive_info['name'])
