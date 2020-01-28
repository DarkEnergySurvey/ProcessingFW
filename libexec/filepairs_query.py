#!/usr/bin/env python3
# $Id: pfwblock.py 15793 2013-10-23 21:33:31Z mgower $
# $Rev:: 15793                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2013-10-23 16:33:31 #$:  # Date of last commit.


import argparse
import sys
import despydb.desdbi as desdbi
import intgutils.queryutils as queryutils

def main(argv):
    parser = argparse.ArgumentParser(description='filepairs_query.py')
    parser.add_argument('--qoutfile', action='store')
    parser.add_argument('--qouttype', action='store')
    parser.add_argument('--attnum', action='store')
    parser.add_argument('--reqnum', action='store')
    parser.add_argument('--filetype', action='append')
    parser.add_argument('--execname', action='store')
    parser.add_argument('--unitname', action='store')
    parser.add_argument('--column', action='append')

    args = parser.parse_args(argv)

    items = args.filetype[0].split(',')
    filetype1 = items[0]
    column1 = items[1]
    items = args.filetype[1].split(',')
    filetype2 = items[0]
    column2 = items[1]

    dbh = desdbi.DesDbi()

    query = f"select t1.filename,t2.filename,t1.{args.column[0]},t1.{args.column[1]} from (select image.filename,image.{args.column[0]},image.{args.column[1]},(regexp_substr(image.filename,'^[0-9a-zA-Z_-]+')) as n1 from wgb,image where image.filename=wgb.filename and wgb.exec_name='{args.execname}' and wgb.unitname='{args.unitname}' and wgb.attnum={args.attnum} and wgb.reqnum={args.reqnum} and image.filetype='{filetype1}') t1, (select image.filename,(regexp_substr(image.filename,'^[0-9a-zA-Z_-]+')) as n2 from wgb,image where image.filename=wgb.filename and wgb.exec_name='{args.execname}' and wgb.unitname='{args.unitname}' and wgb.attnum={args.attnum} and wgb.reqnum={args.reqnum} and image.filetype='{filetype2}') t2 where t1.n1=t2.n2"

    print("Executing the following query\n", query)

    cursor = dbh.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()

    listdict = {'list':{'line':{}}}
    linedict = {}

    length = len(result)
    for i in range(length):
        lineNum = i+1
        line = f"line{lineNum:05d}"
        filedict = {'file':{}}
        columndict = {}
        element1dict = {}
        element2dict = {}
        element1dict[args.column[0]] = result[i][2]
        element1dict[args.column[1]] = result[i][3]
        element1dict['filename'] = result[i][0]
        columndict[column1] = element1dict
        element2dict[args.column[0]] = result[i][2]
        element2dict[args.column[1]] = result[i][3]
        element2dict['filename'] = result[i][1]
        columndict[column2] = element2dict
        filedict['file'] = columndict
        linedict[line] = filedict

    listdict['list']['line'] = linedict

    print("Results returned from query\n", listdict)

    ## output list
    queryutils.output_lines(args.qoutfile, listdict, args.qouttype)

    return 0

if __name__ == "__main__":
    print(' '.join(sys.argv))
    sys.exit(main(sys.argv[1:]))
