#!/bin/bash
#
# A script to test if the basic functions of the files 
# in CloudFS. Has to be run from the ./src/scripts/ 
# directory.
# 

TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $TEST_DIR/../../../scripts/paths.sh
source $SCRIPTS_DIR/functions.sh

THRESHOLD="64"
AVGSEGSIZE="4"
LOG_DIR="/tmp/testrun-`date +"%Y-%m-%d-%H%M%S"`"
TESTDIR=$FUSE_MNT_
TEMPDIR="/tmp/cloudfstest"

#
# Execute battery of test cases.
# expects that the test files are in $TESTDIR
# and the reference files are in $TEMPDIR
# Creates the intermediate results in $LOGDIR
#
function execute_part3_tests()
{

   echo "Executing test_3_3"
   reinit_env

   # Copy largefile and smallfile_1
   echo "Copying files"
   cp $TEST_DIR/largefile $TESTDIR
   cp $TEST_DIR/largefile $TEMPDIR

   cp $TEST_DIR/smallfile_1 $TESTDIR
   cp $TEST_DIR/smallfile_1 $TEMPDIR

   sleep 1
   
   # create snapshot 1
   echo -ne "Checking for snapshot creation                   "
   snapshot_num=$($SCRIPTS_DIR/snapshot $FUSE_MNT/.snapshot s)
   if [ $? -ne 0 ]; then
      print_result 1 
      exit
   else
      print_result 0
   fi

   echo "Deleting files"
   rm $TESTDIR/largefile
   rm $TESTDIR/smallfile_1
   sleep 1

   # Copy largefile and smallfile_2
   cp $TEST_DIR/bigfile $TESTDIR
   cp $TEST_DIR/smallfile_2 $TESTDIR
   sleep 1

   # restore a snapshot
   echo -ne "Checking for snapshot restore                     "
   $SCRIPTS_DIR/snapshot $FUSE_MNT/.snapshot r $snapshot_num
   if [ $? -ne 0 ]; then
      print_result 1 
      exit
   else
      print_result 0
   fi
   sleep 1

   # Check for data integrity
   echo -ne "Checking for data integrity(largefile)            "
   TEST_FILE="largefile"
   cd $TESTDIR && find $TEST_FILE  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 | awk '{print $1}' > $LOG_DIR/md5sum.out.master
   cd $TEMPDIR && find $TEST_FILE  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 | awk '{print $1}' > $LOG_DIR/md5sum.out
   diff $LOG_DIR/md5sum.out.master $LOG_DIR/md5sum.out
   print_result $?

   echo -ne "Checking for data integrity(smallfile_1)          "
   TEST_FILE="smallfile_1"
   cd $TESTDIR && find $TEST_FILE  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 | awk '{print $1}' > $LOG_DIR/md5sum.out.master
   cd $TEMPDIR && find $TEST_FILE  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 | awk '{print $1}' > $LOG_DIR/md5sum.out
   diff $LOG_DIR/md5sum.out.master $LOG_DIR/md5sum.out
   print_result $?

   # Files that must not exist.
   echo -ne "Checking for data integrity(smallfile_2)          "
   if [ ! -f $TESTDIR/smallfile_2 ]; then
      print_result 0
   else
      print_result 1
   fi

   echo -ne "Checking for data integrity(bigfile)              "
   if [ ! -f $TESTDIR/bigfile ]; then
       print_result 0
   else
       print_result 1
   fi
}

#
# Main
#
process_args cloudfs --ssd-path $SSD_MNT_ --fuse-path $FUSE_MNT_ --threshold $THRESHOLD --avg-seg-size $AVGSEGSIZE

#----
# test setup
rm -rf $TEMPDIR
mkdir -p $TEMPDIR
mkdir -p $LOG_DIR

#----
# tests
#run the actual tests
execute_part3_tests
#----

rm -rf $TEMPDIR
rm -rf $LOG_DIR

exit 0
