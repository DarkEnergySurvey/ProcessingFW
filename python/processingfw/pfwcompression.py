#!/usr/bin/env python
# $Id: pfwcompression.py 44002 2016-09-15 18:37:31Z friedel $
# $Rev:: 44002                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2016-09-15 13:37:31 #$:  # Date of last commit.

# pylint: disable=print-statement

"""
    Functions used by the processing framework to compress/uncompress files
    within a pipeline job
"""

# reserved variables:  __UCFILE__ uncompressed file, __CFILE__ compressed file

import copy
import shlex
import os
import subprocess

import despymisc.miscutils as miscutils
import intgutils.replace_funcs as replfuncs

######################################################################
def run_compression_command(cmd, fname_compressed, max_try_cnt=1):
    """ run the compression command """

    trycnt = 1
    returncode = 1
    while trycnt <= max_try_cnt and returncode != 0:
        try:
            process_comp = subprocess.Popen(shlex.split(cmd), shell=False)
            process_comp.wait()

            # check exit code
            returncode = process_comp.returncode
        except OSError as exc:
            errstr = "I/O error({0}): {1}".format(exc.errno, exc.strerror)
            print errstr
            returncode = 1
            # check for partial compressed output and remove
            if os.path.exists(fname_compressed):
                miscutils.fwdebug_print("Compression failed.  Removing compressed file.")
                os.unlink(fname_compressed)

        trycnt += 1
    return returncode


######################################################################
def compress_files(listfullnames, compresssuffix, execname, argsorig, max_try_cnt=3, cleanup=True):
    """ Compress given files """

    if miscutils.fwdebug_check(3, 'PFWCOMPRESS_DEBUG'):
        miscutils.fwdebug_print("BEG num files to compress = %s" % (len(listfullnames)))

    results = {}
    tot_bytes_before = 0
    tot_bytes_after = 0
    for fname in listfullnames:
        errstr = None
        cmd = None
        fname_compressed = None
        returncode = 1
        try:
            if not os.path.exists(fname):
                errstr = "Error: Uncompressed file does not exist (%s)" % fname
                returncode = 1
            else:
                tot_bytes_before += os.path.getsize(fname)
                fname_compressed = fname + compresssuffix

                # create command
                args = copy.deepcopy(argsorig)
                args = replfuncs.replace_vars_single(args,
                                                     {'__UCFILE__': fname,
                                                      '__CFILE__': fname_compressed},
                                                     None)
                cmd = '%s %s' % (execname, args)
                if miscutils.fwdebug_check(3, 'PFWCOMPRESS_DEBUG'):
                    miscutils.fwdebug_print("compression command: %s" % cmd)

                returncode = run_compression_command(cmd, fname_compressed, max_try_cnt)
        except IOError as exc:
            errstr = "I/O error({0}): {1}".format(exc.errno, exc.strerror)
            returncode = 1

        if returncode != 0:
            errstr = "Compression failed with exit code %i" % returncode
            # check for partial compressed output and remove
            if os.path.exists(fname_compressed):
                miscutils.fwdebug_print("Compression failed.  Removing compressed file.")
                os.unlink(fname_compressed)
        elif miscutils.convertBool(cleanup): # if successful, remove uncompressed if requested
            os.unlink(fname)

        if returncode == 0:
            tot_bytes_after += os.path.getsize(fname_compressed)
        else:
            tot_bytes_after += os.path.getsize(fname)

        # save exit code, cmd and new name
        results[fname] = {'status': returncode,
                          'outname': fname_compressed,
                          'err': errstr,
                          'cmd': cmd}

    if miscutils.fwdebug_check(3, 'PFWCOMPRESS_DEBUG'):
        miscutils.fwdebug_print("END bytes %s => %s" % (tot_bytes_before, tot_bytes_after))
    return (results, tot_bytes_before, tot_bytes_after)
