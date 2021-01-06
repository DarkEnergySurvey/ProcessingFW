# $Id: pfwdb.py 48552 2019-05-20 19:38:27Z friedel $
# $Rev:: 48552                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:38:27 #$:  # Date of last commit.

"""
    Define a database utility class extending despydmdb.desdmdbi

    Developed at:
    The National Center for Supercomputing Applications (NCSA).

    Copyright (C) 2012 Board of Trustees of the University of Illinois.
    All rights reserved.
"""

__version__ = "$Rev: 48552 $"

import os
import socket
import sys
from datetime import datetime
import collections
import pytz

from despydmdb import desdmdbi
from processingfw import pfwdefs
from despymisc import miscutils
import qcframework.Messaging as Messaging
import qcframework.qcfdb as qcfdb


TIME_ZONE = pytz.timezone("America/Chicago")

class PFWDB(desdmdbi.DesDmDbi):
    """
        Extend despydmdb.desdmdbi to add database access methods

        Add methods to retrieve the metadata headers required for one or more
        filetypes and to ingest metadata associated with those headers.
    """

    def __init__(self, desfile=None, section=None, threaded=False):
        """ Initialize object """
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"{desfile},{section}")

        desdmdbi.DesDmDbi.__init__(self, desfile, section, threaded=threaded)
        self.desfile = desfile
        self.mirror = None

    def activateMirror(self, config, setup=True):
        """ Connect to the mirror database (sqlite).

            Parameters
            ----------
            config: WCL of the config file

            setup: bool, whether to set up the initial entries

        """
        if self.mirror is None:
            sqlite = config.getfull(pfwdefs.SQLITE_FILE)
            if sqlite is None:
                raise Exception(f"Cannot create sqlite database if {pfwdefs.SQLITE_FILE} is not defnied in the wcl.")
            os.environ[desdmdbi.dbdefs.DES_SQLITE_FILE] = config[pfwdefs.SQLITE_FILE]
            self.mirror = desdmdbi.DesDmDbi(self.desfile, config['target_des_db_section'])
            if setup:
                self.setupMirror()

    def get_database_defaults(self):
        """ Grab default configuration information stored in database """

        result = collections.OrderedDict({pfwdefs.SW_ARCHIVESECT: self.get_archive_info(),
                                          'archive_transfer': self.get_archive_transfer_info(),
                                          'job_file_mvmt': self.get_job_file_mvmt_info(),
                                          pfwdefs.DIRPATSECT: self.get_database_table('OPS_DIRECTORY_PATTERN', 'NAME'),
                                          pfwdefs.SW_FILEPATSECT: self.get_filename_pattern(),
                                          pfwdefs.SW_SITESECT: self.get_site_info(),
                                          pfwdefs.SW_EXEC_DEF: self.get_database_table('OPS_EXEC_DEF', 'NAME'),
                                          'filetype_metadata': self.get_all_filetype_metadata(),
                                          'file_header': self.query_results_dict('select * from OPS_FILE_HEADER', 'name')
                                          })

        return result

    def get_transfer_data(self, site, archive):
        """ Get server specific transfer data"""
        sql = f"select key, val from ops_transfer_val where site='{site}' and archive='{archive}'"
        curs = self.cursor()
        curs.execute(sql)
        results = curs.fetchall()
        if not results:
            print(f"\nInformational: Data for transfer site {site} was not found in the database, continuing with defaults")
        data = {}
        for res in results:
            if self.mirror is not None:
                if 'semname' in res[0]:
                    continue
            data[res[0]] = res[1]
        return data


    def get_database_table(self, tname, tkey):
        """ Get all rows from a database table """
        sql = f"select * from {tname}"
        results = self.query_results_dict(sql, tkey)
        return results

    def get_filename_pattern(self):
        """ Get data from OPS_FILENAME_PATTERN table """
        sql = "select * from OPS_FILENAME_PATTERN"
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = collections.OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            result[d['name'].lower()] = d['pattern']

        curs.close()
        return result


    ##### request, unit, attempt #####
    def insert_run(self, config):
        """ Insert entries into the pfw_request, pfw_unit, pfw_attempt tables for a
            single run submission.    Saves attempt and task id in config """

        pfw_attempt_id = self.get_seq_next_value('pfw_attempt_seq')

        maxtries = 1
        from_dual = self.from_dual()

        allparams = {'task_id': self.create_task(name='attempt',
                                                 info_table='pfw_attempt',
                                                 parent_task_id=None,
                                                 root_task_id=None,
                                                 label=None,
                                                 i_am_root=True,
                                                 do_commit=False),
                     'reqnum': config.getfull(pfwdefs.REQNUM),
                     'unitname': config.getfull(pfwdefs.UNITNAME),
                     'project': config.getfull('project'),
                     'jiraid': config.getfull('jira_id'),
                     'pipeline': config.getfull('pipeline'),
                     'operator': config.getfull('operator'),
                     'numexpblk': len(miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST]))
                    }

        if 'DESDM_PIPEPROD' in os.environ:
            allparams['subpipeprod'] = os.environ['DESDM_PIPEPROD']
        else:
            allparams['subpipeprod'] = None

        if 'DESDM_PIPEVER' in os.environ:
            allparams['subpipever'] = os.environ['DESDM_PIPEVER']
        else:
            allparams['subpipever'] = None

        allparams['basket'] = config.getfull('basket')
        allparams['group_submit_id'] = config.getfull('group_submit_id')
        allparams['campaign'] = config.getfull('campaign')

        # create named bind strings for all parameters
        namebinds = {}
        for k in allparams.keys():
            namebinds[k] = self.get_named_bind_string(k)

        # loop to try again, esp. for race conditions
        loopcnt = 1
        done = False
        while not done and loopcnt <= maxtries:
            sql = None
            params = None
            try:
                curs = self.cursor()

                # pfw_request
                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print("Inserting to pfw_request table\n")
                sql = "insert into pfw_request (reqnum, project, campaign, jira_id, pipeline) "
                sql += f"select {namebinds['reqnum']}, {namebinds['project']}, {namebinds['campaign']}, {namebinds['jiraid']}, {namebinds['pipeline']} {from_dual} where not exists (select null from pfw_request where reqnum={namebinds['reqnum']})"
                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print(f"\t{sql}\n")

                params = {}
                for k in ['reqnum', 'project', 'jiraid', 'pipeline', 'campaign']:
                    params[k] = allparams[k]
                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print(f"\t{params}\n")
                curs.execute(sql, params)

                # pfw_unit
                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print("Inserting to pfw_unit table\n")
                curs = self.cursor()
                sql = f"insert into pfw_unit (reqnum, unitname) select {namebinds['reqnum']}, {namebinds['unitname']} {from_dual} where not exists (select null from pfw_unit where reqnum={namebinds['reqnum']} and unitname={namebinds['unitname']})"
                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print("\t%s\n" % sql)
                params = {}
                for k in ['reqnum', 'unitname']:
                    params[k] = allparams[k]
                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print(f"\t{params}\n")
                curs.execute(sql, params)

                # pfw_attempt
                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print("Inserting to pfw_attempt table\n")
                ## get current max attnum and try next value
                sql = f"select max(attnum) from pfw_attempt where reqnum={namebinds['reqnum']} and unitname={namebinds['unitname']}"
                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print(f"\t{sql}\n")
                params = {}
                for k in ['reqnum', 'unitname']:
                    params[k] = allparams[k]
                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print(f"\t{params}\n")
                curs.execute(sql, params)
                maxarr = curs.fetchall()
                if not maxarr:
                    maxatt = 0
                elif maxarr[0][0] is None:
                    maxatt = 0
                else:
                    maxatt = int(maxarr[0][0])

                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print(f"maxatt = {maxatt}")
                allparams['attnum'] = maxatt + 1
                namebinds['attnum'] = self.get_named_bind_string('attnum')

                allparams['id'] = pfw_attempt_id
                namebinds['id'] = self.get_named_bind_string('id')

                # execute will fail if extra params
                params = {}
                needed_vals = ['id', 'reqnum', 'unitname', 'attnum', 'operator',
                               'numexpblk', 'basket', 'group_submit_id',
                               'task_id', 'subpipeprod', 'subpipever']
                for k in needed_vals:
                    params[k] = allparams[k]

                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print(f"\t{params}\n")

                #sql = "insert into pfw_attempt (reqnum, unitname, attnum, operator, submittime, numexpblk, basket, group_submit_id, task_id, subpipeprod, subpipever) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s %s where not exists (select null from pfw_attempt where reqnum=%s and unitname=%s and attnum=%s)" % (namebinds['reqnum'], namebinds['unitname'], namebinds['attnum'], namebinds['operator'], self.get_current_timestamp_str(), namebinds['numexpblk'], namebinds['basket'], namebinds['group_submit_id'], namebinds['task_id'], namebinds['subpipeprod'], namebinds['subpipever'], from_dual, namebinds['reqnum'], namebinds['unitname'], namebinds['attnum'])
                subsql = f"select null from pfw_attempt where reqnum={namebinds['reqnum']} and unitname={namebinds['unitname']} and attnum={namebinds['attnum']}"

                sql = f"insert into pfw_attempt ({','.join(needed_vals)}, submittime) select {','.join(namebinds[x] for x in needed_vals)}, {self.get_current_timestamp_str()} {from_dual} where not exists ({subsql})"

                if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                    miscutils.fwdebug_print(f"\t{sql}\n")

                curs.execute(sql, params)

                config[pfwdefs.ATTNUM] = allparams['attnum']
                config['task_id'] = {'attempt': allparams['task_id'],
                                     'block': {},
                                     'job': {}}
                config['pfw_attempt_id'] = pfw_attempt_id
                done = True
            except Exception:
                print("\n\n")
                print("sql> ", sql)
                print("params> ", params)
                print("namebinds> ", namebinds)
                (_, value, _) = sys.exc_info()
                if loopcnt < maxtries:
                    miscutils.fwdebug_print(f"Warning: {value}")
                    miscutils.fwdebug_print("Retrying inserting run into database\n\n")
                    loopcnt = loopcnt + 1
                    self.rollback()
                    continue
                raise

        if not done:
            raise Exception("Exceeded max tries for inserting into pfw_attempt table")


        curs.close()
        self.commit()


    def insert_attempt_label(self, config):
        """ Insert label for an attempt into pfw_attempt_label table """
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print("Inserting into pfw_attempt_label table\n")

        row = {}
        row['pfw_attempt_id'] = config['pfw_attempt_id']

        if pfwdefs.SW_LABEL in config:
            labels = config.getfull(pfwdefs.SW_LABEL)
            labels = miscutils.fwsplit(labels, ',')
            for label in labels:
                row['label'] = label
                self.insert_PFW_row('PFW_ATTEMPT_LABEL', row)


    def insert_attempt_val(self, config):
        """ Insert key/val pairs of information about an attempt into the pfw_attempt_val table """
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print("Inserting into pfw_attempt_val table\n")

        row = {'pfw_attempt_id': config['pfw_attempt_id']}

        if pfwdefs.SW_SAVE_RUN_VALS in config:
            keys2save = config.getfull(pfwdefs.SW_SAVE_RUN_VALS)
            keys = miscutils.fwsplit(keys2save, ',')
            for key in keys:
                row['key'] = key
                val = config.getfull(key)
                if isinstance(val, list):
                    for v in val:
                        row['val'] = v
                        self.insert_PFW_row('PFW_ATTEMPT_VAL', row)
                else:
                    row['val'] = val
                    self.insert_PFW_row('PFW_ATTEMPT_VAL', row)


    def update_attempt_archive_path(self, config):
        """ update row in pfw_attempt with relative path in archive """

        updatevals = {'archive_path': config.getfull(pfwdefs.ATTEMPT_ARCHIVE_PATH)}

        wherevals = {'id': config['pfw_attempt_id']}

        self.update_PFW_row('PFW_ATTEMPT', updatevals, wherevals)



    def update_attempt_cid(self, config, condorid):
        """ update row in pfw_attempt with condorid """

        updatevals = {'condorid': condorid}

        wherevals = {'id': config['pfw_attempt_id']}

        self.update_PFW_row('PFW_ATTEMPT', updatevals, wherevals)


    def update_attempt_end_vals(self, pfw_attempt_id, exitcode):
        """ update row in pfw_attempt with end of attempt info """

        updatevals = {'endtime': self.get_current_timestamp_str(),
                      'status': exitcode}

        wherevals = {'id': pfw_attempt_id}

        self.update_PFW_row('PFW_ATTEMPT', updatevals, wherevals)


    ##### BLOCK #####
    def insert_block(self, config):
        """ Insert an entry into the pfw_block table """
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print("Inserting to pfw_block table\n")

        row = {'pfw_attempt_id': config['pfw_attempt_id'],
               'blknum': config.getfull(pfwdefs.PF_BLKNUM),
               'name': config.getfull('blockname'),
               'target_site': config.getfull('target_site'),
               'modulelist': config.getfull(pfwdefs.SW_MODULELIST),
               'task_id': self.create_task(name='block',
                                           info_table='pfw_block',
                                           parent_task_id=int(config['task_id']['attempt']),
                                           root_task_id=int(config['task_id']['attempt']),
                                           label=None,
                                           do_commit=False)
               }
        self.begin_task(row['task_id'])
        self.insert_PFW_row('PFW_BLOCK', row)
        #if self.mirror is not None:
        #    trow = {'name': 'block',
        #            'info_table': 'pfw_block',
        #            'parent_task_id': int(config['task_id']['attempt']),
        #            'root_task_id': int(config['task_id']['attempt']),
        #            'id': row['task_id']
        #            }
        #    self.mirror.basic_insert_row('task', trow)
        #    self.mirror.begin_task(row['task_id'])
        #    self.mirror.basic_insert_row('PFW_BLOCK', row)
        #    self.mirror.commit()

        config['task_id']['block'][str(row['blknum'])] = row['task_id']


    def update_block_numexpjobs(self, config, numexpjobs):
        """ update numexpjobs in pfw_block """

        updatevals = {'numexpjobs': numexpjobs}

        wherevals = {'task_id': config['task_id']['block'][config[pfwdefs.PF_BLKNUM]]}

        self.update_PFW_row('PFW_BLOCK', updatevals, wherevals)
        #if self.mirror is not None:
        #    self.mirror.basic_update_row('PFW_BLOCK', updatevals, wherevals)
        #    self.mirror.commit()


    ##### JOB #####
    def insert_job(self, wcl, jobdict):
        """ Insert an entry into the pfw_job table """
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print("Inserting to pfw_job table\n")

        blknum = wcl[pfwdefs.PF_BLKNUM]
        blktid = int(wcl['task_id']['block'][blknum])

        row = {'pfw_attempt_id': wcl['pfw_attempt_id'],
               'pfw_block_task_id': blktid,
               'jobnum': int(jobdict['jobnum']),
               'expect_num_wrap': jobdict['numexpwrap'],
               'pipeprod': wcl['pipeprod'],
               'pipever': wcl['pipever'],
               'task_id': self.create_task(name='job',
                                           info_table='pfw_job',
                                           parent_task_id=wcl['task_id']['block'][blknum],
                                           root_task_id=int(wcl['task_id']['attempt']),
                                           label=None,
                                           do_commit=False)
               }
        wcl['task_id']['job'][jobdict['jobnum']] = row['task_id']

        if 'jobkeys' in jobdict:
            row['jobkeys'] = jobdict['jobkeys']
        self.insert_PFW_row('PFW_JOB', row)
        if self.mirror is not None:
            self.mirror.basic_insert_row('PFW_JOB', row)
            self.mirror.commit()


    def update_job_target_info(self, wcl, submit_condor_id=None,
                               target_batch_id=None, exechost=None):
        """ Save information about target job from pfwrunjob """


        params = {}
        setvals = []
        if submit_condor_id is not None:
            setvals.append(f"condor_job_id={self.get_named_bind_string('condor_job_id')}")
            params['condor_job_id'] = float(submit_condor_id)

        if target_batch_id is not None:
            setvals.append(f"target_job_id={self.get_named_bind_string('target_job_id')}")
            params['target_job_id'] = target_batch_id

        if 'jobroot' in wcl:
            setvals.append(f"jobroot={self.get_named_bind_string('jobroot')}")
            params['jobroot'] = wcl['jobroot']

        if setvals:
            params['task_id'] = wcl['task_id']['job']

            sql = f"update pfw_job set {','.join(setvals)} where task_id={self.get_named_bind_string('task_id')} and condor_job_id is NULL"

            if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                miscutils.fwdebug_print(f"sql> {sql}")
            if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                miscutils.fwdebug_print(f"params> {params}")
            curs = self.cursor()
            try:
                curs.execute(sql, params)
            except:
                (typ, value, _) = sys.exc_info()
                print("******************************")
                print("Error:", typ, value)
                print(f"sql> {sql}\n")
                print(f"params> {params}\n")
                raise

            if curs.rowcount == 0:
                Messaging.pfw_message(self, wcl['pfw_attempt_id'], wcl['task_id']['job'],
                                      "Job attempted to run more than once", pfwdefs.PFWDB_MSG_ERROR)

                print("******************************")
                print("Error:  This job has already been run before.")
                print("pfw_attempt_id = ", wcl['pfw_attempt_id'])
                print("reqnum = ", wcl[pfwdefs.REQNUM])
                print("unitname = ", wcl[pfwdefs.UNITNAME])
                print("attnum = ", wcl[pfwdefs.ATTNUM])
                print("blknum = ", wcl[pfwdefs.PF_BLKNUM])
                print("jobnum = ", wcl[pfwdefs.PF_JOBNUM])
                print("job task_id = ", wcl['task_id']['job'])

                print("\nThe 1st job information:")
                curs2 = self.cursor()
                sql = f"select * from pfw_job, task where pfw_job.task_id=task.id and pfw_job.task_id={self.get_named_bind_string('task_id')}"
                curs2.execute(sql, {'task_id': wcl['task_id']['job']})
                desc = [d[0].lower() for d in curs2.description]
                for row in curs2:
                    d = dict(zip(desc, row))
                    for k, v in d.items():
                        print(k, v)
                    print("\n")



                print("\nThe 2nd job information:")
                print("submit_condor_id = ", submit_condor_id)
                print("target_batch_id = ", target_batch_id)
                print("exechost = ", exechost)
                print("current time = ", str(datetime.now()))

                print("\nupdate statement information")
                print(f"sql> {sql}\n")
                print(f"params> %{params}\n")


                raise Exception("Error: job attempted to run more than once")

        if exechost is not None:
            sql = f"update task set exec_host='{exechost}'"

            if 'PFW_JOB_START_EPOCH' in os.environ:
                # doing conversion on DB to avoid any timezone issues
                sql += f", start_time = (from_tz(to_timestamp('1970-01-01','YYYY-MM-DD') + numtodsinterval({os.environ['PFW_JOB_START_EPOCH']},'SECOND'), 'UTC') at time zone 'US/Central')"

            sql += f" where id={wcl['task_id']['job']}"
            curs = self.cursor()
            curs.execute(sql)
            self.commit()
            #wherevals = {}
            #wherevals['id'] = wcl['task_id']['job']
            #updatevals = {}
            #updatevals['exec_host'] = exechost
            #self.update_PFW_row('TASK', updatevals, wherevals)


    def update_job_junktar(self, wcl, junktar=None):
        """ update row in pfw_job with junk tarball name """

        if junktar is not None:
            if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                miscutils.fwdebug_print(f"Saving junktar ({junktar}) to pfw_job")
            updatevals = {}
            updatevals['junktar'] = junktar

            wherevals = {}
            wherevals['task_id'] = wcl['task_id']['job']

            self.update_PFW_row('PFW_JOB', updatevals, wherevals)


    def update_job_info(self, wcl, jobnum, jobinfo):
        """ update row in pfw_job with information gathered post job from condor log """

        if miscutils.fwdebug_check(1, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"Updating job information post job ({jobnum})")
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"jobinfo={jobinfo}")

        wherevals = {}
        wherevals['task_id'] = wcl['task_id']['job'][jobnum]
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"wherevals = {wherevals}")

        if jobinfo:
            self.update_PFW_row('PFW_JOB', jobinfo, wherevals)
        else:
            if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
                miscutils.fwdebug_print(f"Found 0 values to update ({wherevals})")
            if miscutils.fwdebug_check(6, 'PFWDB_DEBUG'):
                miscutils.fwdebug_print(f"\tjobnum = {jobnum}, jobinfo = {jobinfo}")


    def update_tjob_info(self, task_id, jobinfo):
        """ update a row in the task table because couldn't do so at run time """

        wherevals = {}
        wherevals['task_id'] = task_id
        self.basic_update_row('pfw_job', jobinfo, wherevals)
        self.commit()

    ##### WRAPPER #####
    def insert_wrapper(self, wcl, iwfilename, parent_tid):
        """ insert row into pfw_wrapper """
        #  called from pfwrunjob so job wcl not full config wcl

        row = {'pfw_attempt_id': wcl['pfw_attempt_id'],
               'wrapnum': wcl[pfwdefs.PF_WRAPNUM],
               'modname': wcl['modname'],
               'name': wcl['wrapper']['wrappername'],
               'task_id': self.create_task(name='wrapper',
                                           info_table='pfw_wrapper',
                                           parent_task_id=parent_tid,
                                           root_task_id=int(wcl['task_id']['attempt']),
                                           label=wcl['modname'],
                                           do_commit=True),
               'pfw_block_task_id': int(wcl['task_id']['block']),
               'pfw_job_task_id': int(wcl['task_id']['job']),
               'inputwcl': os.path.split(iwfilename)[-1]
               }

        if 'wrapkeys' in wcl:
            row['wrapkeys'] = wcl['wrapkeys']

        self.insert_PFW_row('PFW_WRAPPER', row)
        return row['task_id']


    def update_wrapper_end(self, wcl, owclfile, logfile, exitcode, diskusage):
        """ update row in pfw_wrapper with end of wrapper info """

        self.end_task(wcl['task_id']['wrapper'], exitcode, True)

        updatevals = {}
        if owclfile is not None:
            updatevals['outputwcl'] = os.path.split(owclfile)[-1]
        if logfile is not None:
            updatevals['log'] = os.path.split(logfile)[-1]
        if diskusage is not None:
            updatevals['diskusage'] = diskusage

        wherevals = {'task_id': wcl['task_id']['wrapper']}

        self.update_PFW_row('PFW_WRAPPER', updatevals, wherevals)



    ##### PFW_EXEC
    def insert_exec(self, wcl, sect):
        """ insert row into pfw_exec """

        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(sect)
            miscutils.fwdebug_print(wcl[sect])

        row = {'pfw_attempt_id': wcl['pfw_attempt_id'],
               'pfw_block_task_id': wcl['task_id']['block'],
               'pfw_job_task_id': wcl['task_id']['job'],
               'pfw_wrapper_task_id': wcl['task_id']['wrapper'],
               'execnum': wcl[sect]['execnum'],
               'name': wcl[sect]['execname'],
               'task_id': self.create_task(name=sect,
                                           info_table='pfw_exec',
                                           parent_task_id=wcl['task_id']['wrapper'],
                                           root_task_id=int(wcl['task_id']['attempt']),
                                           label=wcl[sect]['execname'],
                                           do_commit=True)
               }
        if 'version' in wcl[sect] and wcl[sect]['version'] is not None:
            row['version'] = wcl[sect]['version']

        self.insert_PFW_row('PFW_EXEC', row)
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print("end")
        return row['task_id']


    def update_exec_version(self, taskid, version):
        """ update row in pfw_exec with exec version info """
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(taskid)

        updatevals = {'version': version}

        wherevals = {'task_id': taskid}

        self.update_PFW_row('PFW_EXEC', updatevals, wherevals)


    def update_exec_end(self, execwcl, taskid):
        """ update row in pfw_exec with end of exec info """
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(taskid)

        # update pfw_exec table
        updatevals = {}
        cmdargs = ''
        if 'cmdline' in execwcl:
            (_, _, cmdargs) = execwcl['cmdline'].partition(' ')
        if cmdargs:
            updatevals['cmdargs'] = cmdargs
        if 'version' in execwcl:
            updatevals['version'] = execwcl['version']
        if 'procinfo' in execwcl:
            prockeys = ['idrss', 'inblock', 'isrss', 'ixrss', 'majflt', 'maxrss',
                        'minflt', 'msgrcv', 'msgsnd', 'nivcsw', 'nsignals', 'nswap',
                        'nvcsw', 'oublock', 'stime', 'utime']
            for pkey in prockeys:
                rkey = f'ru_{pkey}'
                if rkey in execwcl['procinfo']:
                    updatevals[pkey] = execwcl['procinfo'][rkey]
                else:
                    print(f"Warn:  didn't find {rkey} in proc info")

        if updatevals:
            wherevals = {'task_id': taskid}

            self.update_PFW_row('PFW_EXEC', updatevals, wherevals)

        # update task table
        updatevals = {}
        if 'task_info' in execwcl and 'run_exec' in execwcl['task_info']:
            wcl_task_info = execwcl['task_info']['run_exec']
            if 'start_time' in wcl_task_info:
                updatevals['start_time'] = datetime.fromtimestamp(float(wcl_task_info['start_time']), tz=TIME_ZONE)
            if 'end_time' in wcl_task_info:
                updatevals['end_time'] = datetime.fromtimestamp(float(wcl_task_info['end_time']), tz=TIME_ZONE)
            else:
                updatevals['end_time'] = self.get_current_timestamp_str()
            if 'exec_host' in wcl_task_info:
                updatevals['exec_host'] = wcl_task_info['exec_host']
            else:
                updatevals['exec_host'] = socket.gethostname()

            if 'status' in wcl_task_info:
                updatevals['status'] = wcl_task_info['status']
            else:    # assume failure
                updatevals['status'] = pfwdefs.PF_EXIT_FAILURE

        if updatevals:
            wherevals = {'id': taskid}
            self.basic_update_row('TASK', updatevals, wherevals)
        self.commit()


    ######################################################################
    def insert_compress_task(self, task_id, exec_name, exec_version, exec_args, files_to_compress):
        """ Insert information into compress_task table """

        # get sum of filesizes before compression
        gtt_name = self.load_filename_gtt(files_to_compress)
        sql = f"select sum(filesize) from desfile d, {gtt_name} g where g.filename=d.filename and d.compression is NULL"
        curs = self.cursor()
        curs.execute(sql)
        tot_bytes_before = curs.fetchone()[0]

        params = {'task_id': task_id,
                  'name': exec_name,
                  'version': exec_version,
                  'cmdargs': exec_args,
                  'num_requested': len(files_to_compress),
                  'tot_bytes_before': tot_bytes_before}
        sql = f"insert into compress_task ({','.join(list(params.keys()))}) values ({','.join([self.get_named_bind_string(x) for x in params.keys()])})"
        curs = self.cursor()
        curs.execute(sql, params)
        self.commit()


    ######################################################################
    def update_compress_task(self, task_id, errcnt, tot_bytes_after):
        """ Update compress_task row with info after compression """
        wherevals = {'task_id': task_id}
        updatevals = {'num_failed': errcnt,
                      'tot_bytes_after': tot_bytes_after}
        self.basic_update_row('COMPRESS_TASK', updatevals, wherevals)
        self.commit()


    #####
    def insert_data_query(self, wcl, modname, datatype, dataname, execname, cmdargs, version):
        """ insert row into pfw_data_query table """
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print("BEG")

        parent_tid = wcl['task_id']['begblock']

        row = {'pfw_attempt_id': wcl['pfw_attempt_id'],
               'pfw_block_task_id': wcl['task_id']['block'][wcl['blknum']],
               'modname': modname,
               'datatype': datatype,   # file, list
               'dataname': dataname,
               'task_id': self.create_task(name='dataquery',
                                           info_table='PFW_DATA_QUERY',
                                           parent_task_id=parent_tid,
                                           root_task_id=int(wcl['task_id']['attempt']),
                                           label=None,
                                           do_begin=True,
                                           do_commit=True),
               'execname': os.path.basename(execname),
               'cmdargs': cmdargs,
               'version': version
               }
        self.insert_PFW_row('PFW_DATA_QUERY', row)
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print("END")
        return row['task_id']


    ##########
    def insert_PFW_row(self, pfwtable, row):
        """ Insert a row into a PFW table and commit """

        self.basic_insert_row(pfwtable, row)
        self.commit()
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print("end")


    ##########
    def update_PFW_row(self, pfwtable, updatevals, wherevals):
        """ Update a row in a PFW table and commit """

        self.basic_update_row(pfwtable, updatevals, wherevals)
        self.commit()


    def get_job_info(self, wherevals):
        """ Get job information """
        whclause = []
        for c in wherevals.keys():
            whclause.append(f"{c}={self.get_named_bind_string(c)}")
        sql = f"select j.jobkeys as jobkeys,j.jobnum as jobnum, j.expect_num_wrap as expect_num_wrap, j.task_id as task_id, j.pfw_block_task_id as pfw_block_task_id, t.status as status, t.start_time as start_time, t.end_time as end_time from pfw_job j, task t where t.id=j.task_id and {' and '.join(whclause)}"
        #sql = "select j.*,t.* from pfw_job j, task t where t.id=j.task_id and %s" % (' and '.join(whclause))
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"sql> {sql}")
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"params> {wherevals}")
        curs = self.cursor()
        curs.execute(sql, wherevals)
        desc = [d[0].lower() for d in curs.description]


        jobinfo = {}
        get_messages = []
        for line in curs:
            d = dict(zip(desc, line))
            d['message'] = []
            if d['status'] != pfwdefs.PF_EXIT_SUCCESS:
                get_messages.append(d['task_id'])
            jobinfo[d['task_id']] = d
        if not get_messages:
            qdbh = qcfdb.QCFDB(connection=self)
            qcmsg = qdbh.get_all_qcf_messages_by_task_id(get_messages, level=3)
            for tid, val in qcmsg.items():
                jobinfo[tid]['message'] = val

        return jobinfo


    def get_attempt_info(self, reqnum, unitname, attnum, attid=None):
        """ Get information about an attempt """

        sql = None
        if attid is not None:
            sql = f"select * from pfw_attempt where id={attid}"
        else:
            sql = f"select * from pfw_attempt where reqnum={reqnum} and attnum={attnum} and unitname='{unitname}'"

        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"sql> {sql}")
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        attinfo = None
        row = curs.fetchone()    # should only be 1 row
        if row is not None:
            attinfo = dict(zip(desc, row))
        return attinfo


    def get_block_info(self, **kwargs):
        """ Get block information for an attempt """

        if 'reqnum' in kwargs or 'unitname' in kwargs or 'attnum' in kwargs:   # join to attempt table
            sql = 'select * from pfw_attempt, pfw_block where pfw_attempt.id=pfw_block.pfw_attempt_id and '
        else:
            sql = 'select * from pfw_block where '

        wherevals = [f"{k}='{v}'" for k, v in kwargs.items()]
        sql += ' and '.join(wherevals)

        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"sql> {sql}")
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        blockinfo = {}
        for line in curs:
            b = dict(zip(desc, line))
            blockinfo[b['task_id']] = b
        return blockinfo

    def get_jobwrapper_info(self, **kwargs):
        """ Get wrapper information for an attempt """

        sql = "select task.* from pfw_attempt, task where pfw_attempt.task_id=task.root_task_id and task.name='jobwrapper' and "

        wherevals = [f"pfw_attempt.{k}='{v}'" for k, v in kwargs.items()]
        sql += ' and '.join(wherevals)

        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"sql> {sql}")
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        jobwraps = {}
        for line in curs:
            d = dict(zip(desc, line))
            jobwraps[d['id']] = d

        return jobwraps


    def get_wrapper_info(self, **kwargs):
        """ Get wrapper information for an attempt """

        if 'reqnum' in kwargs or 'unitname' in kwargs or 'attnum' in kwargs:   # join to attempt table
            sql = 'select * from pfw_attempt, pfw_wrapper, task where pfw_attempt.id=pfw_wrapper.pfw_attempt_id and pfw_attempt.task_id=task.id and '
        else:
            sql = 'select pw.*,t.* from pfw_wrapper pw, task t where pw.task_id=t.id and '

        wherevals = [f"{k}='{v}'" for k, v in kwargs.items()]
        sql += ' and '.join(wherevals)

        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"sql> {sql}")
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        wrappers = {}
        for line in curs:
            d = dict(zip(desc, line))
            wrappers[d['task_id']] = d

        return wrappers


    def get_block_task_info(self, blktid):
        """ Return task information for tasks for given block """

        sql = f"select * from task where parent_task_id={self.get_named_bind_string('parent_task_id')} and (info_table is Null or info_table != {self.get_named_bind_string('info_table')})"
        curs = self.cursor()
        curs.execute(sql, {'parent_task_id': blktid,
                           'info_table': 'pfw_job'})
        desc = [d[0].lower() for d in curs.description]
        info = {}
        for line in curs:
            d = dict(zip(desc, line))
            info[d['name']] = d
        return info


    def get_run_filelist(self, reqnum, unitname, attnum,
                         blknum=None, archive=None):

        # store filenames in dictionary just to ensure don't get filename multiple times
        filedict = {}

        # setup up common where clauses and params
        wherevals = {'reqnum': reqnum, 'unitname':unitname, 'attnum': attnum}
        if blknum is not None:
            wherevals['blknum'] = blknum

        whclause = []
        for k in wherevals.keys():
            whclause.append(f"{k}={self.get_named_bind_string(k)}")


        # search for output files
        sql = f"select wgb.filename from wgb where {' and '.join(whclause)}"

        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"sql> {sql}")
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"params> {wherevals}")

        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True


        # search for logs
        # (not all logs show up in wgb, example ingestions which don't have output file)
        sql = f"select log from pfw_wrapper where log is not NULL and {' and '.join(whclause)}"

        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"sql> {sql}")
            miscutils.fwdebug_print(f"params> {wherevals}")

        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True

        # search for junk tarball
        sql = f"select junktar from pfw_job where junktar is not NULL and {' and '.join(whclause)}"

        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"sql> {sql}")
            miscutils.fwdebug_print(f"params> {wherevals}")

        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True


        # convert dictionary to list
        filelist = list(filedict.keys())
        if miscutils.fwdebug_check(3, 'PFWDB_DEBUG'):
            miscutils.fwdebug_print(f"filelist = {filelist}")

        if archive is not None:   # limit to files on a specified archive
            gtt_name = self.load_filename_gtt(filelist)
            sqlstr = f"SELECT f.filename FROM file_archive_info a, {gtt_name} f WHERE a.filename=f.filename and a.archive_name={self.get_named_bind_string('archive_name')}"
            cursor = self.cursor()
            cursor.execute(sqlstr, {'archive_name':archive})
            results = cursor.fetchall()
            cursor.close()
            filelist = [x[0] for x in results]

        return filelist


    def get_fail_log_fullnames(self, pfw_attempt_id, archive):
        curs = self.cursor()

        if archive is not None:
            sqlstr = f"select a.root, fai.path, fai.filename from ops_archive a, task t, pfw_wrapper w, file_archive_info fai where w.log=fai.filename and a.name = {self.get_named_bind_string('archive_name')} and fai.archive_name={self.get_named_bind_string('archive_name')} and pfw_attempt_id={self.get_named_bind_string('pfw_attempt_id')} and w.task_id=t.id and (t.status is null or t.status != 0)"
            curs.execute(sqlstr, {'archive_name': archive, 'pfw_attempt_id': pfw_attempt_id})
        else:
            sqlstr = f"select 'NO-HOME-ARCHIVE-ROOT', fai.path, fai.filename from task t, pfw_wrapper w, file_archive_info fai where w.log=fai.filename and pfw_attempt_id={self.get_named_bind_string('pfw_attempt_id')} and w.task_id=t.id and (t.status is null or t.status != 0)"
            curs.execute(sqlstr, {'pfw_attempt_id': pfw_attempt_id})

        results = curs.fetchall()
        curs.close()

        logfullnames = {}
        for x in results:
            logfullnames[x[2]] = f"{x[0]}/{x[1]}/{x[2]}"

        return logfullnames

    def get_log_fullnames(self, pfw_attempt_id, archive):
        curs = self.cursor()

        if archive is not None:
            sqlstr = f"select a.root, fai.path, fai.filename from ops_archive a, pfw_wrapper w, file_archive_info fai where w.log=fai.filename and a.name = {self.get_named_bind_string('archive_name')} and fai.archive_name={self.get_named_bind_string('archive_name')} and w.pfw_attempt_id={self.get_named_bind_string('pfw_attempt_id')}"
            curs.execute(sqlstr, {'archive_name': archive, 'pfw_attempt_id' :pfw_attempt_id})
        else:
            sqlstr = f"select 'NO-HOME-ARCHIVE-ROOT', fai.path, fai.filename from pfw_wrapper w, file_archive_info fai where w.log=fai.filename and w.pfw_attempt_id={self.get_named_bind_string('pfw_attempt_id')}"
            curs.execute(sqlstr, {'pfw_attempt_id': pfw_attempt_id})

        results = curs.fetchall()
        curs.close()

        logfullnames = {}
        for x in results:
            logfullnames[x[2]] = f"{x[0]}/{x[1]}/{x[2]}"

        return logfullnames

    def check_files(self, config, filelist):
        missingfiles = []
        curs = self.cursor()
        home_archive = config.getfull('home_archive')

        gtt = self.load_filename_gtt(filelist)

        curs.execute(f"select filename, compression from {gtt} gtt where not exists (select df.filename,df.compression from desfile df, file_archive_info fai where gtt.filename=df.filename and nullcmp(gtt.compression, df.compression)=1 and df.id=fai.desfile_id and fai.archive_name='{home_archive}')")

        results = curs.fetchall()
        for res in results:
            if res[1] is not None:
                missingfiles.append(res[0] + res[1])
            else:
                missingfiles.append(res[0])

        return missingfiles


    def updateMirrorFiles(self, filelist):
        if self.mirror is None:
            return
        curs = self.cursor()
        gtt = self.load_filename_gtt(filelist)
        curs.execute(f"select fai.* from file_archive_info fai, {gtt} gtt where gtt.filename=fai.filename")
        results = curs.fetchall()
        cols = [desc[0].lower() for desc in curs.description]
        mcurs = self.mirror.cursor()
        binds = ['?'] * len(cols)
        mcurs.executemany(f"insert into file_archive_info ({','.join(cols)}, ORIG) values ({','.join(binds)},1)", results)
        curs.execute(f"select df.* from desfile df, {gtt} gtt where gtt.filename=df.filename")
        results = curs.fetchall()
        cols = [desc[0].lower() for desc in curs.description]
        binds = ['?'] * len(cols)
        mcurs.executemany(f"insert into desfile ({','.join(cols)}, ORIG) values ({','.join(binds)}, 1)", results)
        mcurs.close()
        self.mirror.commit()

    def setupMirror(self):
        """ Populate the job side sqlite database with the initial entries. This ensures
            that the initial state of the database is current. To add additional tables to
            this, just add them to the tables list.
        """
        tables = ['exclude_list',
                  'ops_archive',
                  #'ops_transfer',
                  'ops_transfer_val',
                  'ops_archive_val',
                  'ops_datafile_metadata',
                  'ops_datafile_table',
                  'ops_data_state_def',
                  'ops_directory_pattern',
                  'ops_exec_def',
                  'ops_filename_pattern',
                  'ops_filetype',
                  'ops_filetype_metadata',
                  'ops_file_header',
                  'ops_job_file_mvmt',
                  'ops_job_file_mvmt_val',
                  'ops_message_filter',
                  'ops_message_ignore',
                  'ops_message_pattern',
                  'ops_metadata',
                  'ops_site',
                  'ops_site_val',
                  'ops_transfer_val',
                  #'proctag',
                  #zeropoint',
                  ]
        curs = self.cursor()
        mcurs = self.mirror.cursor()
        for tbl in tables:
            print(f"Updating table {tbl}")
            curs.execute(f"select * from {tbl}")
            results = curs.fetchall()
            cols = [desc[0].lower() for desc in curs.description]
            binds = ['?'] * len(cols)
            mcurs.executemany(f"insert into {tbl} ({','.join(cols)}) values ({','.join(binds)})", results)
        self.mirror.commit()
        mcurs.close()
        curs.close()


    def integrateMirror(self):
        """ Copy any entries that were generated in the job side sqlite database and ingest them into
            the main oracle database. Before ingestion any sequences that were used (typically id's)
            are adjusted to unique values in the oracle database. Column dependencies are also updated to
            the new sequence numbers (column dependencies are defined as a column value in one table that
            is tied to a value in another table, e.g. desfile_id in file_archive_info is dependent on
            values in the id column of desfile).
            The table entries have three components:

            depends: a dict where the key(s) is the name of the dependent column and the value(s) is
                     a tuple of the parent table and column name.

                     Example:
                         In the file_archive_info entry {'desfile_id': ('desfile', 'id')} indicates that
                         file_archive_info.desfile_id depends on desfile.id

            sequence: The column name of any sequence in the table, None if there are no sequences

            columns: a list of the column names to ingest. Typically all columns are ingested ["*"],
                     however a few tables have extra columns to indicate entries that already exist in
                     the oracle database (e.g., input files) that do not need to be re-ingested.

            To add a new table to be ingested just add an entry to the tables dict with the above
            keys. Note that order in the dict matters as any dependent tables must be ingested after the
            table they are dependent on.
        """
        depends = 'depends'
        sequence = 'seq'
        columns = 'columns'

        tables = collections.OrderedDict({
            #  tables with no dependencies and no sequences
            'catalog': {depends: {},
                        sequence: None,
                        columns: ["*"]},
            'coadd': {depends: {},
                      sequence: None,
                      columns: ["*"]},
            'coadd_astrom_qa': {depends: {},
                                sequence: None,
                                columns: ["*"]},
            'coadd_exposure_astrom_qa': {depends: {},
                                         sequence: None,
                                         columns: ["*"]},
            'se_object': {depends: {},
                          sequence: None,
                          columns: ["*"]},
            'image': {depends: {},
                      sequence: None,
                      columns: ["*"]},
            'miscfile': {depends: {},
                         sequence: None,
                         columns: ["*"]},
            'ccdgon':{depends: {},
                      sequence: None,
                      columns: ["*"]},
            'molygon': {depends: {},
                        sequence: None,
                        columns: ["*"]},
            'molygon_ccdgon': {depends: {},
                               sequence: None,
                               columns: ["*"]},

            # tables with dependencies on themselves
            'task':{depends: {'parent_task_id': ('task', 'id')},
                    sequence: 'id',
                    columns: ["*"]},

            # tables with no dependencies and sequences
            'coadd_object': {depends: {},
                             sequence: 'id',
                             columns: ["*"]},

            # tables with both dependencies and sequences
            'desfile': {depends: {'wgb_task_id': ('task', 'id')},
                        sequence: 'id',
                        columns: ["ID", "PFW_ATTEMPT_ID", "WGB_TASK_ID", "FILETYPE", "FILENAME", "COMPRESSION", "FILESIZE", "MD5SUM", "USER_CREATED_BY", "MODULE_CREATED_BY", "CREATED_DATE"]},

            # tables with dependencies and no sequences
            'coadd_object_extinction': {depends: {'coadd_object_id': ('coadd_object', 'id')},
                                        sequence: None,
                                        columns: ["*"]},
            'coadd_object_extinction_band': {depends: {'coadd_object_id': ('coadd_object', 'id')},
                                             sequence: None,
                                             columns: ["*"]},
            'coadd_object_hpix': {depends: {'coadd_object_id': ('coadd_object', 'id')},
                                  sequence: None,
                                  columns: ["*"]},
            'coadd_object_molygon': {depends: {'coadd_object_id': ('coadd_object', 'id')},
                                     sequence: None,
                                     columns: ["*"]},
            'compress_task': {depends: {'task_id': ('task', 'id')},
                              sequence: None,
                              columns: ["*"]},
            'file_archive_info': {depends: {'desfile_id': ('desfile', 'id')},
                                  sequence: None,
                                  columns: ["FILENAME", "ARCHIVE_NAME", "PATH", "COMPRESSION", "DESFILE_ID"]
                                  },
            'opm_used': {depends: {'task_id': ('task', 'id'),
                                   'desfile_id': ('desfile', 'id')},
                         sequence: None,
                         columns: ["*"]},
            'opm_was_derived_from' : {depends: {'desfile_id': ('desfile', 'id')},
                                      sequence: None,
                                      columns: ["*"]},
            'task_message':{depends: {'task_id': ('task', 'id')},
                            sequence: None,
                            columns: ["*"]},
            'transfer_batch': {depends: {'task_id': ('task', 'id'),
                                         'parent_task_id': ('task', 'id')},
                               sequence: None,
                               columns: ["*"]},
            'transfer_file': {depends: {'task_id': ('task', 'id'),
                                        'batch_task_id': ('task', 'id')},
                              sequence: None,
                              columns: ["*"]},
            'wavg': {depends: {'coadd_object_id': ('coadd_object', 'id')},
                     sequence: None,
                     columns: ["*"]},
            'wavg_oclink': {depends: {'coadd_object_id': ('coadd_object', 'id')},
                            sequence: None,
                            columns: ["*"]}
        })
        dependencies = {}
        print("Have tables")
        for table, item in tables.items():
            print(f"Processing {table}")
            curs = self.cursor()
            mcurs = self.mirror.cursor()
            sql = f"select {','.join(item[columns])} from {table}"
            if len(item[columns]) > 1:
                sql += f" where orig=0"
            if item[sequence] is not None:
                sql += f" order by {item[sequence]} asc"
            print("   " + sql)
            mcurs.execute(sql)
            results = mcurs.fetchall()
            if results:
                print(f"   Have {len(results)}")
                cols = [desc[0].lower() for desc in mcurs.description]
                binds = ['?'] * len(cols)
                # if we are modifying the contents then we need to convert to a list of lists
                if item[sequence] is not None or item[depends]:
                    r2 = []
                    for r in results:
                        r2.append(list(r))
                    results = r2
                if item[sequence] is not None:
                    idx = cols.index(item[sequence])
                    curs.execute(f"select {table}_seq.nextval from dual connect by level < {len(results) + 1}")
                    nums = [i[0] for i in curs.fetchall()]
                    print(f"  Have {len(nums)} new seq")
                    if table not in dependencies:
                        dependencies[table] = {}
                    theseq = {}
                    for i in range(len(results)):
                        theseq[results[i][idx]] = nums[i]
                        results[i][idx] = nums[i]
                    dependencies[table][item[sequence]] = theseq
                if item[depends]:
                    for col, (deptable, depcol) in item[depends].items():
                        idx = cols.index(col)
                        for i in range(len(results)):
                            try:
                                results[i][idx] = dependencies[deptable][depcol][results[i][idx]]
                            except KeyError:  # some may already be filled out correctly
                                pass
                binds = []
                for i in range(1, len(cols) + 1):
                    binds.append(self.get_positional_bind_string(i))
                sql = f"insert into {table} ({','.join(cols)}) values ({','.join(binds)})"
                print("    " + sql)
                curs.executemany(sql, results)
                self.commit()
            curs.close()
            mcurs.close()
