#!/bin/bash
#
# A script to test if the basic functions of the files 
# in CloudFS. Has to be run from the ./src/scripts/ 
# directory.
# 
TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $TEST_DIR/../../../scripts/paths.sh

CLOUDFS=$cloudfs_bin
FUSE=$fuse_mnt
SSD=$ssd_mnt
CLOUD="/tmp/s3"
CLOUDFSOPTS=""
SSDSIZE=""
THRESHOLD="64"
AVGSEGSIZE="4096"
RABINWINDOWSIZE=""
CACHESIZE=""
NODEDUP="0"
NOCACHE="0"
LOG_DIR="/tmp/testrun-`date +"%Y-%m-%d-%H%M%S"`"
TESTDIR="$FUSE_MNT"
TEMPDIR="/tmp/cloudfstest"
STATFILE="$LOG_DIR/stats"

CACHEDIR=$CACHE_DIR

source $SCRIPTS_DIR/functions.sh

#
# Execute battery of test cases.
# expects that the test files are in $TESTDIR
# and the reference files are in $TEMPDIR
# Creates the intermediate results in $LOG_DIR
#
function execute_part2_tests()
{

    #----
    # Testcases
    # assumes out test data does not have any hiddenfiles(.* files)
    # students should have all their metadata in hidden files/dirs
    echo ""
    echo "Executing test_2_3"
    rm -rf $CACHEDIR
    reinit_env

    dd if=/dev/urandom of=$TEMPDIR/file1 bs=128 count=$(($THRESHOLD+$AVGSEGSIZE)) > /dev/null 2>&1
    dd if=/dev/urandom of=$TEMPDIR/file2 bs=128 count=$(($THRESHOLD+$AVGSEGSIZE)) > /dev/null 2>&1
    dd if=/dev/urandom of=$TEMPDIR/file3 bs=128 count=$(($THRESHOLD+$AVGSEGSIZE)) > /dev/null 2>&1

    cat $TEMPDIR/file1 $TEMPDIR/file1 > $TEMPDIR/bigfile1
    cat $TEMPDIR/file2 $TEMPDIR/file2 > $TEMPDIR/bigfile2
    cat $TEMPDIR/file3 $TEMPDIR/file3 > $TEMPDIR/bigfile3

    echo "Running cloudfs with --no-dedup"
    $SCRIPTS_DIR/cloudfs_controller.sh x $CLOUDFSOPTS --no-dedup
    collect_stats > $STATFILE.nodedup
    cp  $TEMPDIR/bigfile1 $TESTDIR/bigfile1
    cp  $TEMPDIR/bigfile2 $TESTDIR/bigfile2
    cp  $TEMPDIR/bigfile3 $TESTDIR/bigfile3

    collect_stats >> $STATFILE.nodedup
    max_cloud_storage_nodedup=`get_cloud_max_usage $STATFILE.nodedup`

    rm -rf $CACHEDIR
    AVGSEGSIZE="16"
    reinit_env

    echo "Running cloudfs with dedup enabled(using default avg-seg-size)"
    collect_stats > $STATFILE.dedup
    cp $TEMPDIR/bigfile1 $TESTDIR/bigfile1
    cp $TEMPDIR/bigfile2 $TESTDIR/bigfile2
    cp $TEMPDIR/bigfile3 $TESTDIR/bigfile3
    collect_stats >> $STATFILE.dedup
    max_cloud_storage_dedup=`get_cloud_max_usage $STATFILE.dedup`

    echo "after running cloudfs with dedup enabled "

    echo "Cloud capacity usage with dedup    : $max_cloud_storage_dedup bytes"
    echo "Cloud capacity usage without dedup : $max_cloud_storage_nodedup bytes"

    echo -ne "Checking for --no-dedup option : "
    test $max_cloud_storage_dedup -lt $max_cloud_storage_nodedup
    print_result $? 

    #----
}


#
# Main
#
process_args cloudfs --ssd-path $SSD_MNT_ --fuse-path $FUSE_MNT_ --threshold $THRESHOLD


#----
# test setup
rm -rf $TEMPDIR
mkdir -p $TEMPDIR
mkdir -p $LOG_DIR

#----
# tests
#run the actual tests
execute_part2_tests
#----
# test cleanup

echo -ne "File removal test (rm -rf)        "
rm -rf $FUSE_MNT/*
LF="$LOG_DIR/files-remaining-after-rm-rf.out"

ls $FUSE_MNT_ > $LF
find $SSD_MNT_ \( ! -regex '.*/\..*' \) -type f >> $LF
find $S3_DIR \( ! -regex '.*/\..*' \) -type f >> $LF
nfiles=`wc -l $LF|cut -d" " -f1`
print_result $nfiles

rm -rf $TEMPDIR
rm -rf $LOG_DIR

exit 0

