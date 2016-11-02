#!/bin/bash
#
# A script to test if the basic functions of the files
# in CloudFS. Has to be run from the src directory.
#
TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $TEST_DIR/../../../scripts/paths.sh

LOG_DIR="/tmp/testrun-`date +"%Y-%m-%d-%H%M%S"`"
THRESHOLD="1024"
TEST_FILE="$TEST_DIR/largefile"

source $SCRIPTS_DIR/functions.sh

#
# Execute battery of test cases.
# expects that the test files are in $FUSE_MNT
# and the reference files are in $REFERENCE_DIR
# Creates the intermediate results in $LOG_DIR
#
process_args cloudfs --ssd-path $SSD_MNT_ --fuse-path $FUSE_MNT_ --threshold $THRESHOLD

# test setup
mkdir -p $LOG_DIR

reinit_env

# create the test data in FUSE dir
cp $TEST_FILE $FUSE_MNT

# get rid of disk cache
#$SCRIPTS_DIR/cloudfs_controller.sh x $CLOUDFSOPTS

sleep 2

#----
# Testcases
# assumes out test data does not have any hidden files(.* files)
# students should have all their metadata in hidden files/dirs
echo ""
echo "Executing test_1_4"

echo -ne "Checking for big files on SSD     "
find $SSD_MNT -type f -size +${THRESHOLD}k > $LOG_DIR/find-above-treshold.out
nfiles=`wc -l $LOG_DIR/find-above-treshold.out|cut -d" " -f1`
print_result $nfiles

echo -ne "Checking if the big file is in cloud    "
find $S3_DIR \( ! -regex '.*/\..*' \) -type f > $LOG_DIR/find-s3-before.out
nfiles=`wc -l $LOG_DIR/find-s3-before.out|cut -d" " -f1`
if [ $nfiles -eq 1 ]
then
  print_result 0
else 
  print_result 1
fi

truncate -s0 $FUSE_MNT/largefile

echo -ne "Checking if the small file is not in cloud    "
# wait for s3 to delete the file ...
sleep 1
find $S3_DIR \( ! -regex '.*/\..*' \) -type f > $LOG_DIR/find-s3-after.out
nfiles=`wc -l $LOG_DIR/find-s3-after.out|cut -d" " -f1`
print_result $nfiles

echo -ne "Checking if the small file is on SSD    "
find $FUSE_MNT \( ! -regex '.*/\..*' \) -type f > $LOG_DIR/find-fuse.out
nfiles=`wc -l $LOG_DIR/find-fuse.out|cut -d" " -f1`
if [ $nfiles -eq 1 ]
then
  print_result 0
else 
  print_result 1
fi

#----
#destructive test : always do this test at the end!!
echo -ne "File removal test (rm -rf)        "
rm -rf $FUSE_MNT/*
LF="$LOG_DIR/files-remaining-after-rm-rf.out"

ls $FUSE_MNT > $LF
find $SSD_MNT \( ! -regex '.*/\..*' \) -type f >> $LF
find $S3_DIR \( ! -regex '.*/\..*' \) -type f >> $LF
nfiles=`wc -l $LF|cut -d" " -f1`
print_result $nfiles

# test cleanup
rm -rf $LOG_DIR
exit 0
