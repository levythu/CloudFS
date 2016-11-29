#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <getopt.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <openssl/md5.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include "cloudapi.h"
#include "cloudfs.h"
#include "dedup.h"
#include "hashtable.h"
#include "fsfunc.h"
#include "chunks.h"
#include "snapshot-api.h"

#define UNUSED __attribute__((unused))

static long snapshotBox[CLOUDFS_MAX_NUM_SNAPSHOTS+2];
static int isInstalled[CLOUDFS_MAX_NUM_SNAPSHOTS+2];
static int sizeSnapshotBox;
static int installedSnapshot=0;

const char SNAPSHOT_TAR_PREFIX[]=".snapshot.";
void qsortSnapshotBox(int begg, int endd) {
    int i=begg, j=endd;
    long k=snapshotBox[(i+j)>>1];
    while (i<=j) {
        while (snapshotBox[i]<k) i++;
        while (snapshotBox[j]>k) j--;
        if (i<=j) {
            long t=snapshotBox[i];
            snapshotBox[i]=snapshotBox[j];
            snapshotBox[j]=t;
            i++;
            j--;
        }
    }
    if (begg<j) qsortSnapshotBox(begg, j);
    if (i<endd) qsortSnapshotBox(i, endd);
}
static int list_bucket(const char *key, UNUSED time_t modified_time, UNUSED uint64_t size) {
    int i, N=strlen(SNAPSHOT_TAR_PREFIX);
    for (i=0; i<N; i++) {
        if (key[i]!=SNAPSHOT_TAR_PREFIX[i]) return 0;
    }
    long theTime;
    sscanf(key+N, "%ld", &theTime);
    fprintf(logFile, "[pullSnapshot]\tIngest %ld...\n", theTime);
    fflush(logFile);
    snapshotBox[sizeSnapshotBox]=theTime;
    isInstalled[sizeSnapshotBox]=false;
    sizeSnapshotBox++;
    return 0;
}
void pullSnapshot() {
    char* targetPath=getSSDPosition("/.snapshotbox");
    FILE* f=fopen(targetPath, "r");
    free(targetPath);
    if (!f) {
        fprintf(logFile, "[pullSnapshot]\tNo snapshot box file. Trying to get one from cloud...\n");
        fflush(logFile);
        sizeSnapshotBox=0;
        cloud_list_bucket(CONTAINER_NAME, list_bucket);
        if (sizeSnapshotBox>1) qsortSnapshotBox(0, sizeSnapshotBox-1);
    } else {
        int useless=fscanf(f, "%d", &sizeSnapshotBox);
        (void)useless;
        int i;
        for (i=0; i<sizeSnapshotBox; i++) {
            useless=fscanf(f, "%ld %d", snapshotBox+i, isInstalled+i);
            (void)useless;
        }
        fclose(f);
    }
}

bool pushSnapshot() {
    char* targetPath=getSSDPosition("/.snapshotbox");
    FILE* f=fopen(targetPath, "w");
    free(targetPath);
    if (!f) {
        return false;
    } else {
        fprintf(f, "%d\n", sizeSnapshotBox);
        int i;
        for (i=0; i<sizeSnapshotBox; i++) {
            fprintf(f, "%ld %d\n", snapshotBox[i], isInstalled[i]);
        }
        fclose(f);
        return true;
    }
}

void initSnapshot() {
    pullSnapshot();
    pushSnapshot();
}

long unixMilli() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long)(tv.tv_sec)*1000+(long)(tv.tv_usec)/1000;
}

char* getSnapshotNameFromTimestamp(char *buf, long timestamp) {
    sprintf(buf, ".snapshot.%ld.tar.gz", timestamp);
    return buf;
}
char* getSnapshotInstallPositionFromTimestamp(char *buf, long timestamp) {
    sprintf(buf, "/snapshot_%ld/", timestamp);
    char* absPath=getSSDPosition(buf);
    sprintf(buf, "%s", absPath);
    free(absPath);
    return buf;
}

void* generateTarCmd(const char* tarName) {
    char* rootDir=getSSDPosition("/");
    char* tarFilename=getSSDPositionSlash(tarName);
    char* newStr=malloc(sizeof(char)*MAX_PATH_LEN);
    sprintf(newStr,
        "tar -zcvf %s -C %s . --xattrs --exclude lost+found --exclude .blankplaceholder --exclude .snapshotbox --exclude .cache --exclude .cachemeta",
        tarFilename, rootDir
    );
    free(rootDir);
    free(tarFilename);
    return newStr;
}

void* generateChmodCmd(const char* targetPath, const char* mode) {
    char* newStr=malloc(sizeof(char)*MAX_PATH_LEN);
    sprintf(newStr,
        "chmod -R %s %s",
        mode, targetPath
    );
    return newStr;
}

static FILE *infile;
static int put_buffer(char *buffer, int bufferLength) {
    return fread(buffer, 1, bufferLength, infile);
}

long createSnapshot() {
    if (sizeSnapshotBox>=CLOUDFS_MAX_NUM_SNAPSHOTS) return -1;
    if (installedSnapshot>0) return -2;
    long currentTS=unixMilli();
    fprintf(logFile, "[createSnapshot]\tCreating snapshot at %ld\n", currentTS);
    fflush(logFile);

    char* cmdLine=generateChmodCmd(fsConfig->ssd_path, "+w");
    int status=system(cmdLine);
    free(cmdLine);

    char tarName[MAX_PATH_LEN];
    cmdLine=generateTarCmd(getSnapshotNameFromTimestamp(tarName, currentTS));
    status=system(cmdLine);
    free(cmdLine);
    if (status<0 || !WIFEXITED(status)) {
        return -1;
    }

    char* tarFilepath=getSSDPositionSlash(tarName);
    struct stat tstat;
    if (stat(tarFilepath, &tstat)<0) {
        free(tarFilepath);
        return -3;
    }
    infile=fopen(tarFilepath, "rb");
    if (infile == NULL) {
        free(tarFilepath);
        return -4;
    }
    S3Status s=cloud_put_object(CONTAINER_NAME, tarName, tstat.st_size, put_buffer);
    fclose(infile);
    if (s!=S3StatusOK) {
        free(tarFilepath);
        return -1;
    }
    snapshotBox[sizeSnapshotBox]=currentTS;
    isInstalled[sizeSnapshotBox]=false;
    sizeSnapshotBox++;
    unlink(tarFilepath);
    free(tarFilepath);
    incALLReference();
    pushChunkTable();

    return currentTS;
}

int listSnapshot(long* retSpace) {
    int i;
    for (i=0; i<sizeSnapshotBox; i++) retSpace[i]=snapshotBox[i];
    retSpace[i]=0;
    return 0;
}

int searchForSnapshot(long timestamp) {
    int i;
    for (i=0; i<sizeSnapshotBox; i++)
        if (snapshotBox[i]==timestamp) return i;
    return -1;
}

void* generateUntarCmd(const char* tarABSPath, const char* targetPath) {
    char* newStr=malloc(sizeof(char)*MAX_PATH_LEN);
    sprintf(newStr,
        "tar -zxvf %s -C %s --xattrs",
        tarABSPath, targetPath
    );
    return newStr;
}


static FILE *outfile;
static int get_buffer(const char *buffer, int bufferLength) {
    return fwrite(buffer, 1, bufferLength, outfile);
}
int installSnapshot(long timestamp) {
    int target;
    if ((target=searchForSnapshot(timestamp))<0) return -1;
    if (isInstalled[target]) return -2;
    char iFolderName[MAX_PATH_LEN];
    char tarName[MAX_PATH_LEN];
    char absTarName[MAX_PATH_LEN];
    if (mkdir(getSnapshotInstallPositionFromTimestamp(iFolderName, timestamp), 0777)<0) return -3;
    sprintf(absTarName, "%s%s", iFolderName, getSnapshotNameFromTimestamp(tarName, timestamp));

    outfile=fopen(absTarName, "wb");
    S3Status s=cloud_get_object(CONTAINER_NAME, tarName, get_buffer);
    fclose(outfile);
    if (s!=S3StatusOK) return -4;

    char* cmdLine=generateUntarCmd(absTarName, iFolderName);
    int status=system(cmdLine);
    free(cmdLine);
    if (status<0 || !WIFEXITED(status)) {
        return -5;
    }

    unlink(absTarName);

    cmdLine=generateChmodCmd(iFolderName, "555");
    status=system(cmdLine);
    free(cmdLine);
    if (status<0 || !WIFEXITED(status)) {
        return -5;
    }

    isInstalled[target]=true;
    installedSnapshot++;
    return 0;
}

void* generateCleanCmd(const char* targetPath) {
    char* newStr=malloc(sizeof(char)*MAX_PATH_LEN);
    sprintf(newStr,
        "rm -rf %s",
        targetPath
    );
    return newStr;
}
int uninstallSnapshot(long timestamp) {
    int target;
    if ((target=searchForSnapshot(timestamp))<0) return -1;
    if (!isInstalled[target]) return -2;
    char iFolderName[MAX_PATH_LEN];
    getSnapshotInstallPositionFromTimestamp(iFolderName, timestamp);

    char* cmdLine=generateChmodCmd(iFolderName, "777");
    int status=system(cmdLine);
    free(cmdLine);
    if (status<0 || !WIFEXITED(status)) {
        return -5;
    }

    cmdLine=generateCleanCmd(iFolderName);
    status=system(cmdLine);
    free(cmdLine);
    if (status<0 || !WIFEXITED(status)) {
        return -5;
    }

    isInstalled[target]=false;
    installedSnapshot--;
    return 0;
}

int removeSnapshot(long timestamp) {
    int target;
    fprintf(logFile2, "[removeSnapshot]\t%ld\n", timestamp);
    fflush(logFile2);
    if ((target=searchForSnapshot(timestamp))<0) return -1;
    if (isInstalled[target]) return -2;
    fprintf(logFile2, "[removeSnapshot]\tInstalling....\n");
    fflush(logFile2);
    if (installSnapshot(timestamp)<0) return -3;

    fprintf(logFile2, "[removeSnapshot]\tCheck old chunkdir to do substract.\n");
    fflush(logFile2);

    char iFolderName[MAX_PATH_LEN];
    char chunkDirPath[MAX_PATH_LEN];
    sprintf(chunkDirPath, "%s.chunkdir", getSnapshotInstallPositionFromTimestamp(iFolderName, timestamp));

    FILE* f=fopen(chunkDirPath, "r");
    fprintf(logFile2, "[removeSnapshot]\tStart substracting.\n");
    fflush(logFile2);
    if (f) {
        int N, i, useless;
        char chunkName[200];
        useless=fscanf(f, "%d", &N);
        fprintf(logFile2, "[removeSnapshot]\tSubstracting %d chunks\n", N);
        fflush(logFile2);
        (void)useless;
        for (i=0; i<N; i++) {
            int count;
            long chunkSize;
            useless=fscanf(f, "%d %ld %s", &count, &chunkSize, chunkName);
            fprintf(logFile2, "[removeSnapshot]\tSubstracting %s\n", chunkName);
            fflush(logFile2);
            (void)useless;
            (void)count;
            (void)chunkSize;
            decChunkReference(chunkName);
        }
        fprintf(logFile2, "[removeSnapshot]\tFinish substracting\n");
        fflush(logFile2);
        pushChunkTable();
        fclose(f);
    }

    fprintf(logFile2, "[removeSnapshot]\tUninstall & remove.\n");
    fflush(logFile2);

    uninstallSnapshot(timestamp);
    cloud_delete_object(CONTAINER_NAME, getSnapshotNameFromTimestamp(chunkDirPath, timestamp));

    fprintf(logFile2, "[removeSnapshot]\tRearrange snapshot box.\n");
    fflush(logFile2);

    int i;
    for (i=target+1; i<sizeSnapshotBox; i++) {
        snapshotBox[i-1]=snapshotBox[i];
        isInstalled[i-1]=isInstalled[i];
    }
    sizeSnapshotBox--;
    return 0;
}

int restoreSnapshot(long timestamp) {
    int target;
    if ((target=searchForSnapshot(timestamp))<0) return -1;
    if (installedSnapshot>0) return -2;
    int i;
    char tarName[MAX_PATH_LEN];
    for (i=sizeSnapshotBox-1; i>target; i--) {
        cloud_delete_object(CONTAINER_NAME, getSnapshotNameFromTimestamp(tarName, snapshotBox[i]));
    }
    sizeSnapshotBox=target+1;

    // clean all old files

    char* cleanTarget=getSSDPosition("*");
    char* cmdLine=generateCleanCmd(cleanTarget);
    free(cleanTarget);
    int status=system(cmdLine);
    free(cmdLine);
    if (status<0 || !WIFEXITED(status)) {
        return -5;
    }

    char absTarName[MAX_PATH_LEN];
    char* iFolderName=getSSDPosition("/");
    sprintf(absTarName, "%s%s", iFolderName, getSnapshotNameFromTimestamp(tarName, timestamp));

    outfile=fopen(absTarName, "wb");
    S3Status s=cloud_get_object(CONTAINER_NAME, tarName, get_buffer);
    fclose(outfile);
    if (s!=S3StatusOK) {
        free(iFolderName);
        return -4;
    }

    cmdLine=generateUntarCmd(absTarName, iFolderName);
    status=system(cmdLine);
    free(cmdLine);
    free(iFolderName);
    if (status<0 || !WIFEXITED(status)) {
        return -6;
    }

    rebaseChunkTable();
    incALLReference();
    pushChunkTable();

    unlink(absTarName);

    return 0;
}
