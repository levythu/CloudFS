#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/xattr.h>
#include <sys/stat.h>
#include <fuse.h>
#include <dirent.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/statvfs.h>
#include <stdbool.h>
#include <sys/time.h>
#include <fcntl.h>
#include <openssl/md5.h>
#include "dedup.h"

#include "cloudfs.h"
#include "hashtable.h"
#include "chunks.h"
#include "cloudapi.h"
#include "libs3.h"
#include "fsfunc.h"

static Hashtable chunkTable;

#define CHUNKDIR_MAX_SIZE 10*1024*1024      // 10M

static char* tReadBuffer;
static int dup_read_buffer(const char *buffer, int bufferLength) {
    memcpy(tReadBuffer, buffer, bufferLength);
    tReadBuffer+=bufferLength;
    return bufferLength;
}
void pullChunkTable_FromS3() {
    char* buf=tReadBuffer=(char*)malloc(sizeof(char)*CHUNKDIR_MAX_SIZE);
    S3Status s=cloud_get_object(CONTAINER_NAME, CHUNK_DIRECTORY_NAME, dup_read_buffer);
    if (s!=S3StatusOK) {
        fprintf(logFile, "[pullChunkTable]\tError in fetching chunktable. Create new instead.");
        fflush(logFile);
    } else {
        int N, i;
        int offset=0, readlen;
        char chunkName[200];
        sscanf(buf, "%d%n", &N, &offset);
        for (i=0; i<N; i++) {
            int* count=(int*)malloc(sizeof(int));
            sscanf(buf+offset, "%d %s%n", count, chunkName, &readlen);
            offset+=readlen;
            HPutIfAbsent(chunkTable, chunkName, count);
        }
    }
    free(buf);
}
void pullChunkTable() {
    char* targetPath=getSSDPosition("/.chunkdir");
    FILE* f=fopen(targetPath, "r");
    free(targetPath);
    if (!f) {
        fprintf(logFile, "[pullChunkTable]\tError in fetching chunktable. Create new instead.");
        fflush(logFile);
    } else {
        int N, i, useless;
        char chunkName[200];
        useless=fscanf(f, "%d", &N);
        (void)useless;
        for (i=0; i<N; i++) {
            int* count=(int*)malloc(sizeof(int));
            useless=fscanf(f, "%d %s", count, chunkName);
            (void)useless;
            HPutIfAbsent(chunkTable, chunkName, count);
        }
        fclose(f);
    }
}

void* getChunkRaw(const char *chunkName, long *len) {
    fprintf(logFile, "[getChunkRaw]\t%s\n", chunkName);
    fflush(logFile);
    char* buf=tReadBuffer=(char*)malloc(sizeof(char)*(fsConfig->max_seg_size+10));
    S3Status s=cloud_get_object(CONTAINER_NAME, chunkName, dup_read_buffer);
    if (s!=S3StatusOK) {
        fprintf(logFile, "[getChunkRaw]\tError.");
        fflush(logFile);
        free(buf);
        return NULL;
    }
    *len=tReadBuffer-buf;
    fprintf(logFile, "[getChunkRaw].SUCC\n");
    fflush(logFile);
    return buf;
}
long getChunkLen(const char *chunkName) {
    fprintf(logFile, "[getChunkLen].\n");
    fflush(logFile);
    long ret=0;
    void* p=getChunkRaw(chunkName, &ret);
    if (p) free(p);
    fprintf(logFile, "[getChunkLen].SUCC\n");
    fflush(logFile);
    return ret;
}

void initChunkTable() {
    chunkTable=NewHashTable();
    pullChunkTable();

    pushChunkTable();
}

int getChunkCount() {
    int i;
    int ans=0;
    for (i=0; i<BIG_PRIME; i++) {
        hashNode* p=chunkTable.table[i];
        while (p) {
            ans++;
            p=p->next;
        }
    }
    return ans;
}


static char* tWriteBuffer;
static long writeBufferLen;
static int dup_write_buffer(char *buffer, int bufferLength) {
    if (bufferLength>writeBufferLen) bufferLength=writeBufferLen;
    memcpy(buffer, tWriteBuffer, bufferLength);
    writeBufferLen-=bufferLength;
    tWriteBuffer+=bufferLength;
    return bufferLength;
}
bool pushChunkTable_ToS3() {
    char* buf=tWriteBuffer=(char*)malloc(sizeof(char)*CHUNKDIR_MAX_SIZE);
    int offset, i;
    sprintf(tWriteBuffer, "%d\n%n", getChunkCount(), &offset);
    for (i=0; i<BIG_PRIME; i++) {
        hashNode* p=chunkTable.table[i];
        while (p) {
            int len;
            sprintf(tWriteBuffer+offset, "%d %s\n%n", *((int*)p->v), p->k, &len);
            offset+=len;
            p=p->next;
        }
    }
    writeBufferLen=offset;

    S3Status s=cloud_put_object(CONTAINER_NAME, CHUNK_DIRECTORY_NAME, writeBufferLen, dup_write_buffer);
    free(buf);
    return s==S3StatusOK;
}
bool pushChunkTable() {
    char* targetPath=getSSDPosition("/.chunkdir");
    FILE* f=fopen(targetPath, "w");
    free(targetPath);
    if (!f) return false;

    int i;
    fprintf(f, "%d\n", getChunkCount());
    for (i=0; i<BIG_PRIME; i++) {
        hashNode* p=chunkTable.table[i];
        while (p) {
            fprintf(f, "%d %s\n", *((int*)p->v), p->k);
            p=p->next;
        }
    }
    fclose(f);
    return true;
}

bool putChunkRaw(const char *chunkname, long len, char *content) {
    tWriteBuffer=content;
    writeBufferLen=len;

    S3Status s=cloud_put_object(CONTAINER_NAME, chunkname, writeBufferLen, dup_write_buffer);
    return s==S3StatusOK;
}

int deleteChunkRaw(const char *chunkname) {
    if (cloud_delete_object(CONTAINER_NAME, chunkname)!=S3StatusOK) {
        fprintf(logFile, "[deleteChunkRaw] removing error\t\n");
        fflush(logFile);
        return -1;
    }
    return 0;
}

void incChunkReference(const char* chunkname, long len, char* content) {
    fprintf(logFile, "[incChunkReference]\t%s\n", chunkname);
    fflush(logFile);
    int* tmp=(int*)malloc(sizeof(int));
    *tmp=0;
    if (!HPutIfAbsent(chunkTable, chunkname, tmp)) free(tmp);
    else {
        // upload the chunk content
        putChunkRaw(chunkname, len, content);
    }
    (*(int*)HGet(chunkTable, chunkname))++;
}

// [return] Whether a deletion has happened. Exception on non-exist chunck
bool decChunkReference(const char* chunkname) {
    fprintf(logFile, "[decChunkReference]\t%s\n", chunkname);
    fflush(logFile);
    int* t=(int*)HGet(chunkTable, chunkname);
    (*t)--;
    if (*t>0) return false;
    deleteChunkRaw(chunkname);
    free(HRemove(chunkTable, chunkname));
    return true;
}
