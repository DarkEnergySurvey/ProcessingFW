#!/bin/csh
# $Id: submitmassjob.sh 15873 2013-10-29 16:47:24Z mgower $
# $Rev:: 15873                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2013-10-29 11:47:24 #$:  # Date of last commit.

# example of a shell-script which does mass production of DES jobs
# this may need lots of tweaks depending on what needs to be done.

if ( $#argv < 4 ) then
  echo "Usage: submitmassjob.sh desfile tilelist maxjobs site";
  exit 1;
endif
set default = $argv[1]
set inp = $argv[2]
set maxjobs = $argv[3]
set site = $argv[4]
echo $inp
foreach tile (`cat $inp`)
	check_run:
	#check for number jobs to see if it is less than maxjobs
#	set runs = `condor_q -l | grep "des_run =" | sort -u | wc -l`
	set runs = `desstat|grep $USER| grep $site | wc -l `
    set dbase = `basename $default`
	set tilefile = `echo $dbase | sed -e "s/xxxx/$tile/g"` 	
	if (($runs < $maxjobs) && !(-f $tilefile))  then
		#create submit file
		sed -e "s/xxxx/$tile/g" $default > $tilefile  
		#now submit the file
		echo "Now submitting" $tilefile
		dessubmit $tilefile
		sleep 120
		@ runs = $runs + 1 
	else
		sleep 5
	endif
 # check again if tile file exists and if not repeat for this tile
	if (! (-f $tilefile)) then
		goto check_run
	endif  
end	
