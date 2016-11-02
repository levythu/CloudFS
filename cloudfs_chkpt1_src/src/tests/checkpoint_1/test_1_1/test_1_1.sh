#!/bin/bash
#
# A script to test if the basic functions of the files
# in CloudFS. Has to be run from the src directory.
#
TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $TEST_DIR/../../../scripts/paths.sh

REFERENCE_DIR="/tmp/cloudfstest"
LOG_DIR="/tmp/testrun-`date +"%Y-%m-%d-%H%M%S"`"
STAT_FILE="$LOG_DIR/stats"
THRESHOLD="16"
TAR_FILE="$TEST_DIR/small_test.tar.gz"

source $SCRIPTS_DIR/functions.sh

#
# Execute battery of test cases.
# expects that the test files are in $FUSE_MNT
# and the reference files are in $REFERENCE_DIR
# Creates the intermediate results in $LOG_DIR
#
process_args cloudfs --ssd-path $SSD_MNT_ --fuse-path $FUSE_MNT_ --threshold $THRESHOLD

# test setup
rm -rf $REFERENCE_DIR
mkdir -p $REFERENCE_DIR
mkdir -p $LOG_DIR

reinit_env

# create the test data in FUSE dir
untar $TAR_FILE $FUSE_MNT
# create a reference copy
untar $TAR_FILE $REFERENCE_DIR

# get rid of disk cache
#$SCRIPTS_DIR/cloudfs_controller.sh x $CLOUDFSOPTS

#----
# Testcases
# assumes out test data does not have any hidden files(.* files)
# students should have all their metadata in hidden files/dirs
echo ""
echo "Executing test_1_1"
echo -ne "Basic file and attribute test(ls -lR) "
cd $REFERENCE_DIR && ls -lR|grep -v '^total' > $LOG_DIR/ls-lR.out.master
cd $FUSE_MNT && ls -lR|grep -v '^total' > $LOG_DIR/ls-lR.out

collect_stats > $STAT_FILE

diff $LOG_DIR/ls-lR.out.master $LOG_DIR/ls-lR.out
print_result $?

echo -ne "Basic file content test(md5sum)   "
cd $REFERENCE_DIR && find .  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 > $LOG_DIR/md5sum.out.master
cd $FUSE_MNT && find .  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 > $LOG_DIR/md5sum.out

diff $LOG_DIR/md5sum.out.master $LOG_DIR/md5sum.out
print_result $?

echo -ne "Checking for big files on SSD     "
find $SSD_MNT -type f -size +${THRESHOLD}k > $LOG_DIR/find-above-treshold.out
nfiles=`wc -l $LOG_DIR/find-above-treshold.out|cut -d" " -f1`
print_result $nfiles

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
rm -rf $REFERENCE_DIR
rm -rf $LOG_DIR
exit 0
