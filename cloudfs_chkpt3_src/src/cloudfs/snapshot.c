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

static long snapshotBox[CLOUDFS_MAX_NUM_SNAPSHOTS+2];
static int sizeSnapshotBox;

void pullSnapshot() {
    char* targetPath=getSSDPosition("/.snapshotbox");
    FILE* f=fopen(targetPath, "r");
    free(targetPath);
    if (!f) {
        fprintf(logFile, "[pullSnapshot]\tNo snapshot box file. Create zero.");
        fflush(logFile);
        sizeSnapshotBox=0;
    } else {
        int useless=fscanf(f, "%d", &sizeSnapshotBox);
        (void)useless;
        int i;
        for (i=0; i<sizeSnapshotBox; i++) {
            useless=fscanf(f, "%ld", snapshotBox+i);
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
            fprintf(f, "%ld\n", snapshotBox[i]);
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

void* generateTarCmd(const char* tarName) {
    char* rootDir=getSSDPosition("/");
    char* tarFilename=getSSDPositionSlash(tarName);
    char* newStr=malloc(sizeof(char)*MAX_PATH_LEN);
    sprintf(newStr,
        "tar -zcvf %s -C %s . --xattrs --exclude lost+found --exclude .blankplaceholder --exclude .chunkdir --exclude .snapshotbox",
        tarFilename, rootDir
    );
    free(rootDir);
    free(tarFilename);
    return newStr;
}

static FILE *infile;
static int put_buffer(char *buffer, int bufferLength) {
    return fread(buffer, 1, bufferLength, infile);
}

long createSnapshot() {
    if (sizeSnapshotBox>=CLOUDFS_MAX_NUM_SNAPSHOTS) return -1;
    long currentTS=unixMilli();
    fprintf(logFile, "[createSnapshot]\tCreating snapshot at %ld\n", currentTS);
    fflush(logFile);
    char tarName[MAX_PATH_LEN];
    char* cmdLine=generateTarCmd(getSnapshotNameFromTimestamp(tarName, currentTS));
    int status=system(cmdLine);
    free(cmdLine);
    if (status<0 || !WIFEXITED(status)) {
        return -1;
    }

    char* tarFilepath=getSSDPositionSlash(tarName);
    struct stat tstat;
    if (stat(tarFilepath, &tstat)<0) {
        free(tarFilepath);
        return -1;
    }
    infile=fopen(tarFilepath, "rb");
    if (infile == NULL) {
        free(tarFilepath);
        return -1;
    }
    S3Status s=cloud_put_object(CONTAINER_NAME, tarName, tstat.st_size, put_buffer);
    fclose(infile);
    if (s!=S3StatusOK) {
        free(tarFilepath);
        return -1;
    }
    snapshotBox[sizeSnapshotBox++]=currentTS;
    unlink(tarFilepath);
    free(tarFilepath);
    incALLReference();
    pushChunkTable();

    return currentTS;
}
