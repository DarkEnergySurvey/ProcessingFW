#!/usr/bin/env python
# $Id: genquerydb.py 41243 2016-01-27 17:10:19Z mgower $
# $Rev:: 41243                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-01-27 11:10:19 #$:  # Date of last commit.

""" Generic query to the DB to determine input files """

import argparse
import sys
import re
import despymisc.miscutils as miscutils
import intgutils.queryutils as queryutils
import intgutils.intgdefs as intgdefs
import intgutils.replace_funcs as replfuncs
import processingfw.pfwdb as pfwdb
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwconfig as pfwconfig

def main(argv):
    """ Program entry point """
    parser = argparse.ArgumentParser(description='genquery.py')
    parser.add_argument('--qoutfile', action='store')
    parser.add_argument('--qouttype', action='store')
    parser.add_argument('--config', action='store', dest='configfile')
    parser.add_argument('--module', action='store', dest='modulename')
    parser.add_argument('--search', action='store', dest='searchname')
    args = parser.parse_args(argv)

    if args.modulename is None:
        raise Exception("Error: Must specify module\n")

    print args.configfile
    config = pfwconfig.PfwConfig({'wclfile':args.configfile})

    if args.modulename not in config[pfwdefs.SW_MODULESECT]:
        raise Exception("Error: module '%s' does not exist.\n" % (args.modulename))

    module_dict = config[pfwdefs.SW_MODULESECT][args.modulename]

    if args.searchname is not None:
        if pfwdefs.SW_LISTSECT in module_dict and \
           args.searchname in module_dict[pfwdefs.SW_LISTSECT]:
            search_dict = module_dict[pfwdefs.SW_LISTSECT][args.searchname]
        elif pfwdefs.SW_FILESECT in module_dict and \
             args.searchname in module_dict[pfwdefs.SW_FILESECT]:
            search_dict = module_dict[pfwdefs.SW_FILESECT][args.searchname]
        else:
            raise Exception("Error: Could not find either list or file by name %s in module %s\n" % \
                            (args.searchname, args.modulename))
    else:
        raise Exception("Error: need to define either list or file or search\n")


    archive_names = []

    if config.getfull(pfwdefs.USE_HOME_ARCHIVE_INPUT) != 'never':
        archive_names.append(config.getfull(pfwdefs.HOME_ARCHIVE))

    if config.getfull(pfwdefs.USE_TARGET_ARCHIVE_INPUT) != 'never':
        archive_names.append(config.getfull(pfwdefs.TARGET_ARCHIVE))

    fields = miscutils.fwsplit(search_dict[pfwdefs.SW_QUERYFIELDS].lower())

    if ('query_run' in config and 'fileclass' in search_dict and
            'fileclass' in config and search_dict['fileclass'] == config['fileclass']):
        query_run = config['query_run'].lower()
        if query_run == 'current':
            fields.append('run')
        elif query_run == 'allbutfirstcurrent':
            if 'current' not in config:
                raise Exception("Internal Error:  Current object doesn't exist\n")
            elif 'curr_blocknum' not in config['current']:
                raise Exception("Internal Error:  current->curr_blocknum doesn't exist\n")
            else:
                block_num = config['current']['curr_blocknum']
                if block_num > 0:
                    fields.append('run')

    query = {}
    qtable = search_dict['query_table']
    for fld in fields:
        table = qtable
        if '.' in fld:
            table, fld = fld.split('.')

        if fld in search_dict:
            value = search_dict[fld]
        elif fld in module_dict:
            value = module_dict[fld]
        elif fld in config:
            value = config.getfull(fld)
        else:
            raise Exception("Error: genquery could not find value for query field %s\n" % (fld))

        value = replfuncs.replace_vars(value, config,
                                       {pfwdefs.PF_CURRVALS: {'modulename': args.modulename},
                                        'searchobj': search_dict,
                                        intgdefs.REPLACE_VARS: True,
                                        'expand': True})[0]
        if value is None:
            raise Exception("Value=None for query field %s\n" % (fld))

        if ',' in value:
            value = miscutils.fwsplit(value)

        if ':' in value:
            value = miscutils.fwsplit(value)

        if table not in query:
            query[table] = {}

        if 'key_vals' not in query[table]:
            query[table]['key_vals'] = {}

        query[table]['key_vals'][fld] = value


    # if specified, insert join into query hash
    if 'join' in search_dict:
        joins = miscutils.fwsplit(search_dict['join'].lower())
        for j in joins:
            jmatch = re.search(r"(\S+)\.(\S+)\s*=\s*(\S+)", j)
            if jmatch:
                table = jmatch.group(1)
                if table not in query:
                    query[table] = {}
                if 'join' not in query[table]:
                    query[table]['join'] = j
                else:
                    query[jmatch.group(1)]['join'] += "," + j
        #query[table]['join']=search_dict['join']


    query[qtable]['select_fields'] = ['filename']

    # check output fields for fields from other tables.
    if 'output_fields' in search_dict:
        output_fields = miscutils.fwsplit(search_dict['output_fields'].lower())


        for ofield in output_fields:
            ofmatch = re.search(r"(\S+)\.(\S+)", ofield)
            if ofmatch:
                table = ofmatch.group(1)
                field = ofmatch.group(2)
            else:
                table = qtable
                field = ofield
            if table not in query:
                query[table] = {}
            if 'select_fields' not in query[table]:
                query[table]['select_fields'] = []
            if field not in query[table]['select_fields']:
                query[table]['select_fields'].append(field)


    for tbl in query:
        if 'select_fields' in query[tbl]:
            query[tbl]['select_fields'] = ','.join(query[tbl]['select_fields'])

    if len(archive_names) > 0:
        #query[qtable]['join'] = "%s.filename=file_archive_info.filename" % qtable
        query['file_archive_info'] = {'select_fields': 'compression'}
        query['file_archive_info']['join'] = "file_archive_info.filename=%s.filename" % qtable
        query['file_archive_info']['key_vals'] = {'archive_name': ','.join(archive_names)}

    print "Calling gen_file_list with the following query:\n"
    miscutils.pretty_print_dict(query, out_file=None, sortit=False, indent=4)
    print "\n\n"
    dbh = pfwdb.PFWDB(config.getfull('submit_des_services'), 
                      config.getfull('submit_des_db_section'))
    files = queryutils.gen_file_list(dbh, query)

    if len(files) == 0:
        raise Exception("genquery: query returned zero results for %s\nAborting\n" % \
                        args.searchname)

    ## output list
    lines = queryutils.convert_single_files_to_lines(files)
    queryutils.output_lines(args.qoutfile, lines, args.qouttype)

    return 0

if __name__ == "__main__":
    print ' '.join(sys.argv)
    sys.exit(main(sys.argv[1:]))
