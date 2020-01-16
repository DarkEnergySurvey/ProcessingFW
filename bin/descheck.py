#!/usr/bin/env python3
# $Id: descheck.py 38174 2015-05-11 22:07:19Z mgower $
# $Rev:: 38174                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-05-11 17:07:19 #$:  # Date of last commit.

""" Execute WCL checks """

import argparse

import despymisc.miscutils as miscutils
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwcheck as pfwcheck
import processingfw.pfwdefs as pfwdefs

def main():
    """ Entry point when called as an executable """

    parser = argparse.ArgumentParser(description='Run check on given submit wcl')
    parser.add_argument('--verbose', action='store', default=True)
    parser.add_argument('--des_db_section', action='store')
    parser.add_argument('--des_services', action='store')
    parser.add_argument('--expandwcl', action='store', default=True,
                        help='set to False if running on an uberctrl/config.des')
    parser.add_argument('wclfile', action='store')

    args = vars(parser.parse_args())   # convert dict

    args['verbose'] = miscutils.convertBool(args['verbose'])
    args['usePFWconfig'] = miscutils.convertBool(args['expandwcl'])
    args['get_db_config'] = miscutils.convertBool(args['expandwcl'])

    # usePFWconfig and get_db_config set to True because dessubmit does
    #   (work only done at submit time)
    #   use_db_in=False in submit wcl overrides get_db_config
    print("Gathering wcl...")
    config = pfwconfig.PfwConfig(args)

    config[pfwdefs.ATTNUM] = '0'   # must be string as if read from wcl file
    testcnts = pfwcheck.check(config, '')

    print("\nTest Summary")
    print(f"\tErrors: {testcnts[0]:d}")
    print(f"\tWarnings: {testcnts[1]:d}")
    print(f"\tItems fixed: {testcnts[2]:d}")

if __name__ == '__main__':
    main()
