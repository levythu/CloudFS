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
AVGSEGSIZE="4"
TEST_FILE="largefile"
MODIFIED_FILE="largefile.modified"
FILE_SIZE=$(wc -c < $TEST_DIR/$TEST_FILE)
CLOUD_USAGE="cloud_usage"

source $SCRIPTS_DIR/functions.sh
NODEDUP=0
#
# Execute battery of test cases.
# expects that the test files are in $FUSE_MNT_
# and the reference files are in $REFERENCE_DIR
# Creates the intermediate results in $LOG_DIR
#

process_args cloudfs --ssd-path $SSD_MNT_ --fuse-path $FUSE_MNT_ --threshold $THRESHOLD --avg-seg-size $AVGSEGSIZE

# test setup
rm -rf $REFERENCE_DIR
mkdir -p $REFERENCE_DIR
mkdir -p $LOG_DIR

reinit_env

#----
# Testcases assumes that test data does not have any hidden files(.* files)
# Students should have all their metadata in hidden files/dirs
echo ""
echo "Executing test_2_4"
echo -e "Running cloudfs in dedup mode\n"

#Copy the test file into the fuse folder and the reference folder
echo -e "Copying test file into the fuse folder..."
cp  $TEST_DIR/$TEST_FILE $FUSE_MNT/$TEST_FILE
cp  $TEST_DIR/$TEST_FILE $REFERENCE_DIR/$TEST_FILE

sleep 2

collect_stats > $STAT_FILE
echo -e "\nCloud statistics -->"
echo "Capacity usage in cloud : $(get_cloud_max_usage $STAT_FILE)"

#Compare the number of bytes written to the cloud with the file size
echo "$(get_cloud_current_usage $STAT_FILE)" > $LOG_DIR/$CLOUD_USAGE
nbytes1=$(<$LOG_DIR/$CLOUD_USAGE)
#echo -ne "\nCheck if cloud usage is less than file size   " 
#test $nbytes1 -lt $FILE_SIZE 
#print_result $?

echo -ne "Original File: Basic file content test(md5sum)   "
PWDSAVE=$PWD
cd $REFERENCE_DIR && find $TEST_FILE  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 > $LOG_DIR/md5sum.out.master
cd $FUSE_MNT_ && find $TEST_FILE  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 > $LOG_DIR/md5sum.out
cd $PWDSAVE

diff $LOG_DIR/md5sum.out.master $LOG_DIR/md5sum.out
print_result $?

#Replace the first 6 bytes
echo -e "\nReplacing 6 bytes at offset 0...\n"
sed 's/^\(.\{0\}\)111111/\1555555/' $FUSE_MNT/$TEST_FILE > $FUSE_MNT/$MODIFIED_FILE
sed 's/^\(.\{0\}\)111111/\1555555/' $REFERENCE_DIR/$TEST_FILE > $REFERENCE_DIR/$MODIFIED_FILE

#Run md5sum on the modified file to check if the replace operation was successful
echo -ne "After modification: Basic file content test(md5sum)   "
PWDSAVE=$PWD
cd $REFERENCE_DIR && find $MODIFIED_FILE  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 > $LOG_DIR/md5sum.out.master
cd $FUSE_MNT_ && find $MODIFIED_FILE  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 > $LOG_DIR/md5sum.out
cd $PWDSAVE

diff $LOG_DIR/md5sum.out.master $LOG_DIR/md5sum.out
print_result $?

collect_stats > $STAT_FILE
echo -e "\nCloud statistics -->"
echo "Capacity usage in cloud : $(get_cloud_max_usage $STAT_FILE)"

#Compare the new cloud usage with the old one and original file size
echo "$(get_cloud_current_usage $STAT_FILE)" > $LOG_DIR/$CLOUD_USAGE.2
nbytes2=$(<$LOG_DIR/$CLOUD_USAGE.2)
echo -ne "Check if cloud usage has increased   "
test $nbytes2 -lt $(($nbytes1*2))
print_result $?
echo -ne "\nCheck if cloud usage is less than file size   "   
test $nbytes2 -lt $FILE_SIZE
print_result $? 

#----
#destructive test : always do this test at the end!!
echo -ne "\nFile removal test (rm -rf)        "
rm -rf $FUSE_MNT/*
LF="$LOG_DIR/files-remaining-after-rm-rf.out"

ls $FUSE_MNT_ > $LF
find $SSD_MNT_ \( ! -regex '.*/\..*' \) -type f >> $LF
find $S3_DIR \( ! -regex '.*/\..*' \) -type f >> $LF
nfiles=`wc -l $LF|cut -d" " -f1`
print_result $nfiles

# test cleanup
rm -rf $REFERENCE_DIR
rm -rf $LOG_DIR
exit 0
