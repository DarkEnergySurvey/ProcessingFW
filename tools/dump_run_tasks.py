#!/usr/bin/env python

# $Id: dump_run_tasks.py 41123 2016-01-07 15:36:30Z mgower $
# $Rev:: 41123                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-01-07 09:36:30 #$:  # Date of last commit.

""" Print task information for a processing attempt """

import argparse
import re
import sys
import datetime

import processingfw.pfwdb as pfwdb

######################################################################
def print_header():
    """ Print report column headers """
    print "tid, parent_tid, root_tid, name, label, status, infotable, start_time, end_time"

######################################################################
def get_start_time(x):
    """ Get non-null start_time """
    maxdate = datetime.datetime.now()
    return x['start_time'] or maxdate

######################################################################
def print_task(taskd, indent=''):
    """ Print information for a single task """
    print indent, taskd['id'], taskd['parent_task_id'], taskd['root_task_id'],\
          taskd['name'], taskd['label'], taskd['status'], 

    if taskd['info_table'] is None:
        print "NO_INFO_TABLE",

    if taskd['start_time'] is None:
        print "    -  -     :  :  ",
    else:
        print taskd['start_time'].strftime("%Y-%m-%d %H:%M:%S"),
    if taskd['end_time'] is None:
        print "    -  -     :  :  ",
    else:
        print taskd['end_time'].strftime("%Y-%m-%d %H:%M:%S"),

    if taskd['start_time'] is not None and taskd['end_time'] is not None:
        print "%8.2f" % (taskd['end_time'] - taskd['start_time']).total_seconds() 
    else:
        print ""

######################################################################
def recurs_dump(tasks, tids, indent=''):
    """ Recursively print task information """
    tlist = [tasks[t] for t in tids]
    for taskd in sorted(tlist, key=lambda x: get_start_time(x), reverse=False):
    #for taskd in sorted(tlist, key=lambda x: x['created_date'], reverse=False):
    #for taskd in sorted(tlist, key=lambda x: x['id'], reverse=False):
        print_task(taskd, indent)
        if len(taskd['children']) > 0:
            recurs_dump(tasks, taskd['children'], indent+'    ')

######################################################################
def parse_args(argv):
    """ Parse command line arguments """
    parser = argparse.ArgumentParser(description='Print task information for a processing attempt')
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', '-s', action='store',
                        help='Must be specified if DES_DB_SECTION is not set in environment')
    parser.add_argument('attempt_str', nargs='?', action='store')
    parser.add_argument('-r', '--reqnum', action='store')
    parser.add_argument('-u', '--unitname', action='store')
    parser.add_argument('-a', '--attnum', action='store')

    args = vars(parser.parse_args(argv))   # convert to dict

    if args['attempt_str'] is not None:
        args['reqnum'], args['unitname'], args['attnum'] = parse_attempt_str(args['attempt_str'])
    elif args['reqnum'] is None:
        print "Error:  Must specify attempt_str or r,u,a"
        sys.exit(1)

    return args

######################################################################
def parse_attempt_str(attstr):
    """ Parse attempt string for reqnum, unitname, and attnum """
    amatch = re.search(r"(\S+)_r([^p]+)p([^_]+)", attstr)
    if amatch is None:
        print "Error:  cannot parse attempt string", attstr
        sys.exit(1)

    unitname = amatch.group(1)
    reqnum = amatch.group(2)
    attnum = amatch.group(3)

    return reqnum, unitname, attnum

######################################################################
def get_task_info(args):
    """ Query the DB for task information """

    dbh = pfwdb.PFWDB(args['des_services'], args['section'])

    # get the run info
    attinfo = dbh.get_attempt_info(args['reqnum'], args['unitname'], args['attnum'])
    attid = attinfo['task_id']
    print "attempt task id = ", attid

    #sql = """WITH alltasks (id, parent_task_id, root_task_id, lvl,
    #name, info_table, start_time, end_time, status, exec_host,label)
    #AS (SELECT c.id, c.parent_task_id, c.root_task_id, 1, c.name,
    #c.info_table, c.start_time, c.end_time, c.status, c.exec_host,c.label
    #FROM task c WHERE c.id=%s UNION ALL SELECT r.id, r.parent_task_id,
    #r.root_task_id, a.lvl+1, r.name, r.info_table, r.start_time,
    #r.end_time, r.status, r.exec_host,r.label FROM alltasks a INNER
    #JOIN task r ON r.parent_task_id = a.id) SELECT id, parent_task_id,
    #root_task_id, lvl, name, info_table, start_time, end_time, status,
    #exec_host, label FROM alltasks ORDER BY lvl ASC""" % (attid)

    sql = "select * from task where root_task_id=%d order by id" % attid
    print sql

    curs = dbh.cursor()
    curs.execute(sql)
    desc = [d[0].lower() for d in curs.description]
    #print desc

    tasks = {}
    for line in curs:
        lined = dict(zip(desc, line))
        lined['children'] = []
        tasks[lined['id']] = lined

    return attid, tasks


######################################################################
def add_children(tasks):
    """ To help printing, add children ids to tasks """

    orphans = {}
    for taskd in tasks.values():
        if taskd['parent_task_id'] is not None:
            if taskd['parent_task_id'] in tasks:
                tasks[taskd['parent_task_id']]['children'].append(taskd['id'])
            else:
                orphans[taskd['id']] = taskd
        elif taskd['name'] != 'attempt':  # attempt is not orphan
            orphans[taskd['id']] = taskd

    return orphans



######################################################################
def main(argv):
    """ Program entry point """

    args = parse_args(argv)
    print "unitname =", args['unitname']
    print "reqnum =", args['reqnum']
    print "attnum =", args['attnum']

    attid, tasks = get_task_info(args)
    orphans = add_children(tasks)

    print len(tasks), "tasks total"

    print_header()
    recurs_dump(tasks, [attid], '')

    if len(orphans) > 0:
        print "********** ORPHANS  **********"
        for taskd in sorted(orphans.values(), key=lambda x:x['parent_task_id'], reverse=False):
            print_task(taskd)


if __name__ == "__main__":
    main(sys.argv[1:])
