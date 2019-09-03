#!/usr/bin/env python
# $Id: pfwconfig.py 48065 2019-01-11 16:09:42Z friedel $
# $Rev:: 48065                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-01-11 10:09:42 #$:  # Date of last commit.

# pylint: disable=print-statement

""" Contains class definition that stores configuration and state information for PFW """

from collections import OrderedDict
import sys
import re
import copy
import os
import time
import random

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils
import intgutils.intgdefs as intgdefs
import intgutils.replace_funcs as replfuncs
from intgutils.wcl import WCL
import processingfw.pfwdb as pfwdb

# order in which to search for values
PFW_SEARCH_ORDER = [pfwdefs.SW_FILESECT, pfwdefs.SW_LISTSECT, 'exec', 'job',
                    pfwdefs.SW_MODULESECT, pfwdefs.SW_BLOCKSECT,
                    pfwdefs.SW_ARCHIVESECT, pfwdefs.SW_SITESECT]

class PfwConfig(WCL):
    """ Contains configuration and state information for PFW """

    ###########################################################################
    def __init__(self, args):
        """ Initialize configuration object, typically reading from wclfile """

        WCL.__init__(self)

        # data which needs to be kept across programs must go in self
        # data which needs to be searched also must go in self
        self.set_search_order(PFW_SEARCH_ORDER)

        wclobj = WCL()
        if 'wclfile' in args:
            if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
                miscutils.fwdebug_print("Reading wclfile: %s" % (args['wclfile']))
            try:
                starttime = time.time()
                print "\tReading submit wcl...",
                with open(args['wclfile'], "r") as wclfh:
                    wclobj.read(wclfh, filename=args['wclfile'])
                print "DONE (%0.2f secs)" % (time.time()-starttime)
                #wclobj['wclfile'] = args['wclfile']
            except IOError as err:
                miscutils.fwdie("Error: Problem reading wcl file '%s' : %s" % \
                                (args['wclfile'], err), pfwdefs.PF_EXIT_FAILURE)

        # location of des services file
        if 'submit_des_services' in args and args['submit_des_services'] is not None:
            wclobj['submit_des_services'] = args['submit_des_services']
        elif 'submit_des_services' not in wclobj:
            if 'DES_SERVICES' in os.environ:
                wclobj['submit_des_services'] = os.environ['DES_SERVICES']
            else:
                # let it default to $HOME/.desservices.init
                wclobj['submit_des_services'] = None

        # which section to use in des services file
        if 'submit_des_db_section' in args and args['submit_des_db_section'] is not None:
            wclobj['submit_des_db_section'] = args['submit_des_db_section']
        elif 'submit_des_db_section' not in wclobj:
            if 'DES_DB_SECTION' in os.environ:
                wclobj['submit_des_db_section'] = os.environ['DES_DB_SECTION']
            else:
                # let DB connection code print error message
                wclobj['submit_des_db_section'] = None

        # for values passed in on command line, set top-level config
        for var in (pfwdefs.PF_DRYRUN, pfwdefs.PF_USE_DB_IN,
                    pfwdefs.PF_USE_DB_OUT, pfwdefs.PF_USE_QCF, pfwdefs.PF_VERIFY_FILES):
            if var in args and args[var] is not None:
                wclobj[var] = args[var]

        if 'usePFWconfig' in args:
            pfwconfig = os.environ['PROCESSINGFW_DIR'] + '/etc/pfwconfig.des'
            if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
                miscutils.fwdebug_print("Reading pfwconfig: %s" % (pfwconfig))
            starttime = time.time()
            print "\tReading config from software install...",
            pfwcfg_wcl = WCL()
            with open(pfwconfig, "r") as wclfh:
                pfwcfg_wcl.read(wclfh, filename=pfwconfig)
            self.update(pfwcfg_wcl)
            print "DONE (%0.2f secs)" % (time.time()-starttime)

        self.use_db_in = None
        if pfwdefs.PF_USE_DB_IN in wclobj:
            self.use_db_in = miscutils.convertBool(wclobj[pfwdefs.PF_USE_DB_IN])
        elif pfwdefs.PF_USE_DB_IN in self:
            self.use_db_in = miscutils.convertBool(self[pfwdefs.PF_USE_DB_IN])

        if (self.use_db_in and 'get_db_config' in args and args['get_db_config']):
            print "\tGetting defaults from DB...",
            sys.stdout.flush()
            starttime = time.time()
            self.dbh = pfwdb.PFWDB(wclobj['submit_des_services'], wclobj['submit_des_db_section'])
            print "DONE (%0.2f secs)" % (time.time()-starttime)
            self.update(self.dbh.get_database_defaults())
        else:
            self.dbh = None

        # wclfile overrides all, so must be added last
        if 'wclfile' in args:
            if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
                miscutils.fwdebug_print("Reading wclfile: %s" % (args['wclfile']))
            self.update(wclobj)

        self.set_names()

        # store the file name of the top-level submitwcl in dict:
        if 'submitwcl' not in self and 'wclfile' in args:
            self['submitwcl'] = args['wclfile']

        if 'processingfw_dir' not in self and \
           'PROCESSINGFW_DIR' in os.environ:
            self['processingfw_dir'] = os.environ['PROCESSINGFW_DIR']

        if 'current' not in self:
            self['current'] = OrderedDict({'curr_block': '',
                                           'curr_archive': '',
                                           #'curr_software': '',
                                           'curr_site' : ''})
            self[pfwdefs.PF_WRAPNUM] = '0'
            self[pfwdefs.PF_BLKNUM] = '1'
            self[pfwdefs.PF_TASKNUM] = '0'
            self[pfwdefs.PF_JOBNUM] = '0'

        if pfwdefs.SW_BLOCKLIST in self:
            block_array = miscutils.fwsplit(self[pfwdefs.SW_BLOCKLIST])
            if self[pfwdefs.PF_BLKNUM] <= len(block_array):
                self.set_block_info()

    ###########################################################################
    # assumes already run through chk
    def set_submit_info(self):
        """ Initialize submit time values """

        self['des_home'] = os.path.abspath(os.path.dirname(__file__)) + "/.."
        self['submit_dir'] = os.getcwd()
        self['submit_host'] = os.uname()[1]

        if 'submit_time' in self:   # operator providing submit_time
            submit_time = self['submit_time']
            submit_epoch = int(time.mktime(time.strptime(submit_time,"%Y%m%d%H%M%S")))
        else:
            submit_epoch = time.time()
            submit_time = time.strftime("%Y%m%d%H%M%S", time.localtime(submit_epoch))
            self['submit_time'] = submit_time

        self['submit_epoch'] = submit_epoch
        self[pfwdefs.PF_JOBNUM] = '0'
        self[pfwdefs.PF_BLKNUM] = '1'
        self[pfwdefs.PF_TASKNUM] = '0'
        self[pfwdefs.PF_WRAPNUM] = '0'
        self[pfwdefs.UNITNAME] = self.getfull(pfwdefs.UNITNAME)

        self.reset_blknum()
        self.set_block_info()

        self['submit_run'] = replfuncs.replace_vars_single("${unitname}_r${reqnum}p${attnum:2}",
                                                           self, None)
        self['submit_%s' % pfwdefs.REQNUM] = self.getfull(pfwdefs.REQNUM)
        self['submit_%s' % pfwdefs.UNITNAME] = self.getfull(pfwdefs.UNITNAME)
        self['submit_%s' % pfwdefs.ATTNUM] = self.getfull(pfwdefs.ATTNUM)
        self['run'] = self.getfull('submit_run')


        work_dir = ''
        if pfwdefs.SUBMIT_RUN_DIR in self:
            work_dir = self.getfull(pfwdefs.SUBMIT_RUN_DIR)
            if work_dir[0] != '/':    # submit_run_dir was relative path
                work_dir = self.getfull('submit_dir') + '/' + work_dir

        else:  # make a timestamp-based directory in cwd
            work_dir = "%s/%s_%s" % (self.getfull('submit_dir'),
                                     os.path.splitext(self['submitwcl'])[0],
                                     submit_time)

        self['work_dir'] = work_dir
        self['uberctrl_dir'] = work_dir + "/uberctrl"

        (exists, master_save_file) = self.search(pfwdefs.MASTER_SAVE_FILE,
                                                 {intgdefs.REPLACE_VARS: True})
        if exists:
            if master_save_file not in pfwdefs.VALID_MASTER_SAVE_FILE:
                match = re.match(r'rand_(\d\d)', master_save_file.lower())
                if match:
                    if random.randrange(100) <= int(match.group(1)):
                        if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
                            miscutils.fwdebug_print('Changing %s to %s' % \
                                                    (pfwdefs.MASTER_SAVE_FILE, 'always'))
                        self[pfwdefs.MASTER_SAVE_FILE] = 'always'
                    else:
                        if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
                            miscutils.fwdebug_print('Changing %s to %s' % \
                                                    (pfwdefs.MASTER_SAVE_FILE, 'file'))
                        self[pfwdefs.MASTER_SAVE_FILE] = 'file'
                else:
                    miscutils.fwdie("Error:  Invalid value for %s (%s)" % \
                                    (pfwdefs.MASTER_SAVE_FILE,
                                     master_save_file),
                                    pfwdefs.PF_EXIT_FAILURE)
        else:
            self[pfwdefs.MASTER_SAVE_FILE] = pfwdefs.MASTER_SAVE_FILE_DEFAULT




    ###########################################################################
    def set_block_info(self):
        """ Set current vals to match current block number """
        if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("BEG")

        curdict = self['current']

        if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("\tcurdict = %s" % (curdict))

        # current block number
        blknum = self[pfwdefs.PF_BLKNUM]

        # update current block name for accessing block information
        blockname = self.get_block_name(blknum)
        if not blockname:
            miscutils.fwdie("Error: Cannot determine block name value for blknum=%s" % \
                            blknum, pfwdefs.PF_EXIT_FAILURE)
        curdict['curr_block'] = blockname

        self['block_dir'] = '../B%02d-%s' % (int(blknum), blockname)

        # update current target site name
        (exists, site) = self.search('target_site')
        if not exists:
            miscutils.fwdie("Error:  Cannot determine target site.", pfwdefs.PF_EXIT_FAILURE)

        site = site.lower()
        if site not in self[pfwdefs.SW_SITESECT]:
            print "Error: invalid site value (%s)" % (site)
            print "\tsite defs contain entries for sites: ", self[pfwdefs.SW_SITESECT].keys()
            miscutils.fwdie("Error: Invalid site value (%s)" % (site), pfwdefs.PF_EXIT_FAILURE)
        curdict['curr_site'] = site
        self['runsite'] = site

        # update current target archive name if using archive
        if ((pfwdefs.USE_TARGET_ARCHIVE_INPUT in self and
             miscutils.convertBool(self[pfwdefs.USE_TARGET_ARCHIVE_INPUT])) or
                (pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in self and
                 miscutils.convertBool(self[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT]))):
            (exists, archive) = self.search(pfwdefs.TARGET_ARCHIVE)
            if not exists:
                miscutils.fwdie("Error: Cannot determine target_archive value.   \n" \
                                "\tEither set target_archive or set to FALSE both %s and %s" % \
                                (pfwdefs.USE_TARGET_ARCHIVE_INPUT,
                                pfwdefs.USE_TARGET_ARCHIVE_OUTPUT), pfwdefs.PF_EXIT_FAILURE)

            archive = archive.lower()
            if archive not in self[pfwdefs.SW_ARCHIVESECT]:
                print "Error: invalid target_archive value (%s)" % archive
                print "\tarchive contains: ", self[pfwdefs.SW_ARCHIVESECT]
                miscutils.fwdie("Error: Invalid target_archive value (%s)" % archive,
                                pfwdefs.PF_EXIT_FAILURE)

            curdict['curr_archive'] = archive

            if 'list_target_archives' in self:
                if not archive in self['list_target_archives']:
                    # assumes target archive names are not substrings of one another
                    self['list_target_archives'] += ',' + archive
            else:
                self['list_target_archives'] = archive

        elif ((pfwdefs.USE_HOME_ARCHIVE_INPUT in self and
               self[pfwdefs.USE_HOME_ARCHIVE_INPUT] != 'never') or
              (pfwdefs.USE_HOME_ARCHIVE_OUTPUT in self and
               self[pfwdefs.USE_HOME_ARCHIVE_OUTPUT] != 'never')):
            (exists, archive) = self.search(pfwdefs.HOME_ARCHIVE)
            if not exists:
                miscutils.fwdie("Error: Cannot determine home_archive value.\n" \
                                "\tEither set home_archive or set correctly both %s and %s" % \
                                (pfwdefs.USE_HOME_ARCHIVE_INPUT, pfwdefs.USE_HOME_ARCHIVE_OUTPUT),
                                pfwdefs.PF_EXIT_FAILURE)

            archive = archive.lower()
            if archive not in self[pfwdefs.SW_ARCHIVESECT]:
                print "Error: invalid home_archive value (%s)" % archive
                print "\tarchive contains: ", self[pfwdefs.SW_ARCHIVESECT]
                miscutils.fwdie("Error: Invalid home_archive value (%s)" % archive,
                                pfwdefs.PF_EXIT_FAILURE)
            # dynamically choose a transfer node if a list is given
            if 'transfer_server' in self[pfwdefs.SW_ARCHIVESECT][archive]:
                if self.use_db_in:
                    if self.dbh is None:
                        self.dbh = pfwdb.PFWDB(self['submit_des_services'], self['submit_des_db_section'])
                    servers = self[pfwdefs.SW_ARCHIVESECT][archive]['transfer_server'].replace(' ','').split(',')
                    server = servers[random.randint(0, len(servers) - 1)]
                    self[pfwdefs.SW_ARCHIVESECT][archive].update(self.dbh.get_transfer_data(server, archive))
                else:
                    miscutils.fwdie("Error: transfer_servers was specified, but %s was set to False. Must be able to use database to use transfer_servers option." % (pfwdefs.PF_USE_DB_IN), pfwdefs.PF_EXIT_FAILURE)


            curdict['curr_archive'] = archive
        else:
            # make sure to reset curr_archive from possible prev block value
            curdict['curr_archive'] = None


        if 'submit_des_services' in self:
            self['des_services'] = self['submit_des_services']

        if 'submit_des_db_section' in self:
            self['des_db_section'] = self['submit_des_db_section']

        if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("END")


    def inc_blknum(self):
        """ increment the block number """
        # note config stores numbers as strings
        self[pfwdefs.PF_BLKNUM] = str(int(self[pfwdefs.PF_BLKNUM]) + 1)

    ###########################################################################
    def reset_blknum(self):
        """ reset block number to 1 """
        self[pfwdefs.PF_BLKNUM] = '1'

    ###########################################################################
    def inc_jobnum(self, inc=1):
        """ Increment running job number """
        self[pfwdefs.PF_JOBNUM] = str(int(self[pfwdefs.PF_JOBNUM]) + inc)
        return self[pfwdefs.PF_JOBNUM]


    ###########################################################################
    def inc_tasknum(self, inc=1):
        """ Increment blktask number """
        self[pfwdefs.PF_TASKNUM] = str(int(self[pfwdefs.PF_TASKNUM]) + inc)
        return self[pfwdefs.PF_TASKNUM]


    ###########################################################################
    def inc_wrapnum(self):
        """ Increment running wrapper number """
        self[pfwdefs.PF_WRAPNUM] = str(int(self[pfwdefs.PF_WRAPNUM]) + 1)


    ###########################################################################
    def get_block_name(self, blknum):
        """ Return block name based upon given block num """
        blknum = int(blknum)   # read in from file as string

        blockname = ''
        blockarray = miscutils.fwsplit(self[pfwdefs.SW_BLOCKLIST], ',')
        if (1 <= blknum) and (blknum <= len(blockarray)):
            blockname = blockarray[blknum-1]
        return blockname


    ###########################################################################
    def get_condor_attributes(self, block, subblock):
        """Create dictionary of attributes for condor jobs"""
        attribs = {}
        attribs[pfwdefs.ATTRIB_PREFIX + 'isjob'] = 'TRUE'
        attribs[pfwdefs.ATTRIB_PREFIX + 'project'] = self['project']
        attribs[pfwdefs.ATTRIB_PREFIX + 'pipeline'] = self['pipeline']
        attribs[pfwdefs.ATTRIB_PREFIX + 'run'] = self['submit_run']
        attribs[pfwdefs.ATTRIB_PREFIX + 'operator'] = self['operator']
        attribs[pfwdefs.ATTRIB_PREFIX + 'runsite'] = self['runsite']
        attribs[pfwdefs.ATTRIB_PREFIX + 'block'] = block
        attribs[pfwdefs.ATTRIB_PREFIX + 'subblock'] = subblock
        attribs[pfwdefs.ATTRIB_PREFIX + 'campaign'] = self['campaign']

        if (subblock == '$(jobnum)'):
            if 'numjobs' in self:
                attribs[pfwdefs.ATTRIB_PREFIX + 'numjobs'] = self['numjobs']
            if 'glidein_name' in self:
                attribs['GLIDEIN_NAME'] = self['glidein_name']
        return attribs


    ###########################################################################
    def get_dag_cmd_opts(self):
        """Create dictionary of condor_submit_dag command line options"""
        cmdopts = {}
        for key in ['max_pre', 'max_post', 'max_jobs', 'max_idle']:
            (exists, value) = self.search('dagman_' + key)
            if exists:
                cmdopts[key] = value
        return cmdopts


    ###########################################################################
    def get_grid_info(self):
        """Create dictionary of grid job submission options"""
        vals = {}
        for key in ['stdout', 'stderr', 'queue', 'psn', 'job_type',
                    'max_wall_time', 'max_time', 'max_cpu_time',
                    'max_memory', 'min_memory', 'count', 'host_count',
                    'host_types', 'host_xcount', 'xcount', 'reservation_id',
                    'grid_resource', 'grid_type', 'grid_host', 'grid_port',
                    'batch_type', 'globus_extra', 'environment', 'dynslots']:
            newkey = key.replace('_', '')
            (exists, value) = self.search(key)
            if exists:
                vals[newkey] = value
            else:
                (exists, value) = self.search(newkey)
                if exists:
                    vals[newkey] = value
                elif miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
                    miscutils.fwdebug_print("Could not find value for %s(%s)" % (key, newkey))

        return vals

    ###########################################################################
    def stagefile(self, opts):
        """ Determine whether should stage files or not """
        retval = True
        (dryrun_exists, dryrun) = self.search(pfwdefs.PF_DRYRUN, opts)
        if dryrun_exists and miscutils.convertBool(dryrun):
            retval = False
        (stagefiles_exists, stagefiles) = self.search(pfwdefs.STAGE_FILES, opts)
        if stagefiles_exists and not miscutils.convertBool(stagefiles):
            retval = False
        return retval


    ###########################################################################
    def get_filename(self, filepat=None, searchopts=None):
        """ Return filename based upon given file pattern name """

        if miscutils.fwdebug_check(6, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("given filepat = %s, type = %s" % (filepat, type(filepat)))
            miscutils.fwdebug_print("given searchopts = %s" % (searchopts))

        origreq = False
        if searchopts is not None and 'required' in searchopts:
            origreq = searchopts['required']
            searchopts['required'] = False

        if filepat is None:
            # first check for filename pattern override
            if miscutils.fwdebug_check(6, 'PFWCONFIG_DEBUG'):
                miscutils.fwdebug_print("first check for filename pattern override")
            (found, filenamepat) = self.search('filename', searchopts)

            if not found:
                # get filename pattern from global settings:
                if miscutils.fwdebug_check(6, 'PFWCONFIG_DEBUG'):
                    miscutils.fwdebug_print("get filename pattern from global settings")
                (found, filepat) = self.search(pfwdefs.SW_FILEPAT, searchopts)

                if not found:
                    islist = 'searchobj' in searchopts and 'fsuffix' in searchopts['searchobj'] and searchopts['searchobj']['fsuffix'] == pfwdefs.SW_LISTSECT
                    msg = "Error: Could not find file pattern (%s) in " % pfwdefs.SW_FILEPAT
                    if islist:
                        msg += "list def section"
                    else:
                        msg += "file def section"
                    if pfwdefs.PF_CURRVALS in searchopts and 'curr_module' in searchopts[pfwdefs.PF_CURRVALS]:
                        msg += " of %s" % searchopts[pfwdefs.PF_CURRVALS]['curr_module']
                    if 'searchobj' in searchopts and 'flabel' in searchopts['searchobj']:
                        if islist:
                            msg += ", list"
                        else:
                            msg += ", file"

                        msg += " %s" % searchopts['searchobj']['flabel']
                    miscutils.fwdie(msg, pfwdefs.PF_EXIT_FAILURE, 2)

        elif miscutils.fwdebug_check(6, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("working with given filepat = %s" % (filepat))

        if miscutils.fwdebug_check(6, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("filepat = %s" % (filepat))

        if pfwdefs.SW_FILEPATSECT not in self:
            self.write()
            miscutils.fwdie("Error: Could not find filename pattern section (%s) in config" % \
                            pfwdefs.SW_FILEPATSECT, pfwdefs.PF_EXIT_FAILURE)
        elif filepat in self[pfwdefs.SW_FILEPATSECT]:
            filenamepat = self[pfwdefs.SW_FILEPATSECT][filepat]
        else:
            miscutils.fwdebug_print("%s keys: %s" % (pfwdefs.SW_FILEPATSECT,
                                                     self[pfwdefs.SW_FILEPATSECT].keys()))
            print "searchopts =", searchopts
            miscutils.fwdie("Error: Could not find value for filename pattern '%s' in file pattern section" % filepat, pfwdefs.PF_EXIT_FAILURE, 2)

        if searchopts is not None:
            searchopts['required'] = origreq

        retval = filenamepat

        if (searchopts is None or intgdefs.REPLACE_VARS not in searchopts or
               miscutils.convertBool(searchopts[intgdefs.REPLACE_VARS])):
            sopt2 = {}
            if searchopts is not None:
                sopt2 = copy.deepcopy(searchopts)
            sopt2[intgdefs.REPLACE_VARS] = True
            if 'expand' not in sopt2:
                sopt2['expand'] = True
            if 'keepvars' not in sopt2:
                sopt2['keepvars'] = False
            retval = replfuncs.replace_vars(filenamepat, self, sopt2)
            if not miscutils.convertBool(sopt2['keepvars']):
                retval = retval[0]

        return retval


    ###########################################################################
    def get_filepath(self, pathtype, dirpat=None, searchopts=None):
        """ Return filepath based upon given pathtype and directory pattern name """

        # get filename pattern from global settings:
        if not dirpat:
            (found, dirpat) = self.search(pfwdefs.DIRPAT, searchopts)

            if not found:
                miscutils.fwdie("Error: Could not find dirpat", pfwdefs.PF_EXIT_FAILURE)

        if dirpat in self[pfwdefs.DIRPATSECT]:
            filepathpat = self[pfwdefs.DIRPATSECT][dirpat][pathtype]
        else:
            miscutils.fwdie("Error: Could not find pattern %s in directory patterns" % \
                            dirpat, pfwdefs.PF_EXIT_FAILURE)

        results = replfuncs.replace_vars_single(filepathpat, self, searchopts)
        return results


    ###########################################################################
    def combine_lists_files(self, modulename):
        """ Return python list of file and file list objects """

        if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("BEG")

        moduledict = self[pfwdefs.SW_MODULESECT][modulename]

        # create python list of files and lists for this module
        dataset = []
        if pfwdefs.SW_LISTSECT in moduledict and len(moduledict[pfwdefs.SW_LISTSECT]) > 0:
            if 'list_order' in moduledict:
                listorder = moduledict['list_order'].replace(' ', '').split(',')
            else:
                listorder = moduledict[pfwdefs.SW_LISTSECT].keys()
            for key in listorder:
                dataset.append(('list-%s' % key, moduledict[pfwdefs.SW_LISTSECT][key]))
        elif miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("no lists")

        if pfwdefs.SW_FILESECT in moduledict and len(moduledict[pfwdefs.SW_FILESECT]) > 0:
            for key, val in moduledict[pfwdefs.SW_FILESECT].items():
                dataset.append(('file-%s' % key, val))
        elif miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("no files")

        if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("END")
        return dataset

    ###########################################################################
    def set_names(self):
        """ set names for use in patterns (i.e., blockname, modulename) """

        for tsname, tsval in self.items():
            if isinstance(tsval, dict):
                for nsname, nsval in tsval.items():
                    if isinstance(nsval, dict):
                        namestr = '%sname' % tsname
                        if namestr not in nsval:
                            nsval[namestr] = nsname



    ###########################################################################
    # Determine whether should stage files or not
    def stagefiles(self, opts=None):
        """ Return whether to save stage files to target archive """
        retval = True

        notarget_exists, notarget = self.search(pfwdefs.PF_DRYRUN, opts)
        if notarget_exists and miscutils.convertBool(notarget):
            print "Do not stage file due to dry run\n"
            retval = False
        else:
            stagefiles_exists, stagefiles = self.search(pfwdefs.STAGE_FILES, opts)
            if stagefiles_exists:
                #print "checking stagefiles (%s)" % stagefiles
                results = replfuncs.replace_vars_single(stagefiles, self, opts)
                retval = miscutils.convertBool(results)
                #print "after interpolation stagefiles (%s)" % retval
            else:
                envkey = 'DESDM_%s' % pfwdefs.STAGE_FILES.upper()
                if envkey in os.environ and not miscutils.convertBool(os.environ[envkey]):
                    retval = False

        #print "stagefiles retval = %s" % retval
        return retval


    ###########################################################################
    # Determine whether should save files or not
    def savefiles(self, opts=None):
        """ Return whether to save files from job """
        retval = True

        savefiles_exists, savefiles = self.search(pfwdefs.SAVE_FILE_ARCHIVE, opts)
        if savefiles_exists:
            if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
                miscutils.fwdebug_print("checking savefiles (%s)" % savefiles)
            results = replfuncs.replace_vars_single(savefiles, self, opts)
            retval = miscutils.convertBool(results)
            if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
                miscutils.fwdebug_print("after interpolation savefiles (%s)" % retval)
        else:
            envkey = 'DESDM_%s' % pfwdefs.SAVE_FILE_ARCHIVE.upper()
            if envkey in os.environ and not miscutils.convertBool(os.environ[envkey]):
                retval = False

        if miscutils.fwdebug_check(3, 'PFWCONFIG_DEBUG'):
            miscutils.fwdebug_print("savefiles retval = %s" % retval)
        return retval

    def get_param_info(self, keys, opts=None):
        """ returns values for given list of keys """
        info = {}
        for key, stat in keys.items():
            (found, value) = self.search(key, opts)
            if found:
                info[key] = value
            else:
                if stat.lower() == 'req':
                    miscutils.fwdie("Error:  Config does not contain value for %s" % key,
                                    pfwdefs.PF_EXIT_FAILURE, 2)

        return info


if __name__ == '__main__':
    if len(sys.argv) == 2:
        pfw = PfwConfig({'wclfile': sys.argv[1]})
        #pfw.write(sys.argv[2])
        print pfwdefs.SW_BLOCKLIST in pfw
        print 'not_there' in pfw
        pfw.set_block_info()
        print pfw[pfwdefs.PF_BLKNUM]
        pfw.inc_blknum()
        print pfw[pfwdefs.PF_BLKNUM]
        pfw.reset_blknum()
        pfw.set_block_info()
        print pfw[pfwdefs.PF_BLKNUM]
