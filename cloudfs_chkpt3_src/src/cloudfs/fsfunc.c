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

#include "hashtable.h"
#include "cloudapi.h"
#include "libs3.h"
#include "cloudfs.h"
#include "fsfunc.h"
#include "chunks.h"
#include "snapshot.h"
#include "snapshot-api.h"

#define UNUSED __attribute__((unused))

#define XATTR_ON_CLOUD "user.cloudfs.on.cloud"
#define XATTR_ON_OLDCLOUD "user.cloudfs.on.old.cloud"
#define XATTR_ON_CLOUD_NOV "//notInTheCloud"
#define XATTR_ON_CLOUD_CHUNK "//chunked"
#define XATTR_M_TIME "user.cloudfs.mtime"
#define XATTR_A_TIME "user.cloudfs.atime"
#define XATTR_SIZE "user.cloudfs.size"
#define XATTR_BNUM "user.cloudfs.blocknum"
#define SNAPSHOT_FD "/.snapshot"

#define MAX_CHUNK_NUM

FILE *logFile=NULL;
FILE *logFile2=NULL;
struct cloudfs_state* fsConfig=NULL;
Hashtable openfileTable;
rabinpoly_t *rp=NULL;

/*
** openedFile is the core data structure of the inmemory representation of a file
** it is single instanced, and managed by global hashtable. Once there's no
** open file handler for a file, its openedFile structure will get destructed.
**
** if the mode is 0, it is a normal file (either a small file on disk or a big file
** on cloud); otherwise it is a chunked file on the cloud. When the file size
** exceeds limitation and nodedup==false, a file will be converted to mode 1.
**
** For a simple small file, it is stored on disk with XATTR_ON_CLOUD==XATTR_ON_CLOUD_NOV,
** For a nodedup big file, it is stored on cloud, with XATTR_ON_CLOUD==its cloud name
** For a chunked file, it is chunked on cloud, and store its chunklist containing
**      its chunks and startPosition on disk, with XATTR_ON_CLOUD==XATTR_ON_CLOUD_CHUNK
*/

typedef struct _openedFile {
    int refCount;
    int mode;   // 0 for normal, 1 for dedup

    // the following members are valid only mode==1; both chunkName and chunkPosition
    // are array[0..chunkCount-1], and chunkPosition is an increasing array.
    char **chunkName;
    long* chunkPosition;
    int chunkCount;
    long totalLen;
    bool dirty;

    // capacity is the actual size of chunkName and chunkPosition. It should always
    // be >=chunkCount. When smaller, double it.
    int capacity;
} openedFile;

// get the length of a file. Cache the length when needed (now deprecated since)
// the chunk size is localized.
long getLen(openedFile* ofr) {
    if (ofr->totalLen>=0) return ofr->totalLen;

    if (ofr->chunkCount==0) {
        ofr->totalLen=0;
        return ofr->totalLen;
    }
    ofr->totalLen=ofr->chunkPosition[ofr->chunkCount-1]+getChunkLen(ofr->chunkName[ofr->chunkCount-1]);
    return ofr->totalLen;
}

// print the of just for debugging.
void printOF(openedFile* ofr) {
    fprintf(logFile, "[printOF]================Size: %ld\n[", getLen(ofr));
    fflush(logFile);
    int i;
    for (i=0; i<ofr->chunkCount; i++)
        fprintf(logFile, "(%s, %ld), ", ofr->chunkName[i], ofr->chunkPosition[i]);

    fprintf(logFile, "NULL]\n");
    fflush(logFile);
}

// duplicate an OF. It performs a very deep copy of the data stucture.
openedFile* makeDupOF(openedFile* ofr) {
    openedFile* ret=(openedFile*)malloc(sizeof(openedFile));
    ret->refCount=ofr->refCount;
    ret->mode=ofr->refCount;
    ret->capacity=ofr->capacity;
    ret->chunkCount=ofr->chunkCount;
    ret->totalLen=ofr->totalLen;
    ret->dirty=ofr->dirty;
    ret->chunkName=NULL;
    ret->chunkPosition=NULL;

    if (ret->capacity>0) {
        ret->chunkName=(char **)malloc(sizeof(char *)*ret->capacity);
        ret->chunkPosition=(long*)malloc(sizeof(long)*ret->capacity);
    }
    int i=0;
    for (i=0; i<ofr->chunkCount; i++) {
        ret->chunkName[i]=dupString(ofr->chunkName[i]);
        ret->chunkPosition[i]=ofr->chunkPosition[i];
    }
    return ret;
}

// dispose an OF. Release all the space
void disposeOF(openedFile* ofr) {
    fprintf(logFile, "[disposeOF]\t\n");
    fflush(logFile);
    if (ofr->mode==1 && ofr->chunkName) {
        int i;
        for (i=0; i<ofr->chunkCount; i++) {
            free(ofr->chunkName[i]);
        }
        free(ofr->chunkName);
        free(ofr->chunkPosition);
    }
    free(ofr);
}

// quick sort on OF.chunkPosition, and modify chunkName as required.
void qksortOnChunklist(openedFile* ofr, int begg, int endd) {
    int i=begg, j=endd;
    long k=ofr->chunkPosition[(i+j)>>1];
    while (i<=j) {
        while (ofr->chunkPosition[i]<k) i++;
        while (ofr->chunkPosition[j]>k) j--;
        if (i<=j) {
            long s1=ofr->chunkPosition[i];
            ofr->chunkPosition[i]=ofr->chunkPosition[j];
            ofr->chunkPosition[j]=s1;

            char* s2=ofr->chunkName[i];
            ofr->chunkName[i]=ofr->chunkName[j];
            ofr->chunkName[j]=s2;

            i++; j--;
        }
    }
    if (begg<j) qksortOnChunklist(ofr, begg, j);
    if (i<endd) qksortOnChunklist(ofr, i, endd);
}

// after truncate, or write, some chunks may be invalid (with chunkPosition==0x7fffffff)
// and some new are appended. This function remove those invalid and sort the rest
// so that the chunkPosition array is monotonously increasing
void rearrangeChunkList(openedFile* ofr) {
    fprintf(logFile, "[rearrangeChunkList]\t\n");
    fflush(logFile);
    ofr->totalLen=-1;
    ofr->dirty=true;
    qksortOnChunklist(ofr, 0, ofr->chunkCount-1);
    while (ofr->chunkCount>0 && ofr->chunkPosition[ofr->chunkCount-1]==0x7fffffff) {
        decChunkReference(ofr->chunkName[ofr->chunkCount-1]);
        free(ofr->chunkName[ofr->chunkCount-1]);
        ofr->chunkCount--;
    }
    fprintf(logFile, "[rearrangeChunkList]\tDONE. rest: %d\n", ofr->chunkCount);
    fflush(logFile);
}

// This function find the chunk startPos should belong to. If it exceeds the size
// of the file, return -1; attention: it will not work when the chunks are not
// contigent.
int findRightChunk(openedFile* ofr, long startPos) {
    fprintf(logFile, "[findRightChunk]\t%ld\n", startPos);
    fflush(logFile);
    int i=0;
    for (i=0; i<ofr->chunkCount; i++)
        if (startPos<ofr->chunkPosition[i]) break;
    if (i!=ofr->chunkCount) return i-1;
    if (startPos>=getLen(ofr)) return -1;
    return ofr->chunkCount-1;
}

// if chunkIndex <ofr->chunkCount, replace it.
// if chunkIndex>=ofr->chunkCount, append it.
// chunkName will be duplicated. And all the references to the chunks will be
// maintained.
void putChunk(openedFile* ofr, int chunkIndex, const char* chunkName, long startPos, long len, char* chunkContent) {
    ofr->totalLen=-1;
    ofr->dirty=true;
    if (chunkIndex<ofr->chunkCount) {
        char* originalName=ofr->chunkName[chunkIndex];
        ofr->chunkName[chunkIndex]=dupString(chunkName);
        ofr->chunkPosition[chunkIndex]=startPos;
        decChunkReference(originalName);
        incChunkReference(ofr->chunkName[chunkIndex], len ,chunkContent);
        free(originalName);
    } else {
        // append
        if (ofr->chunkCount==ofr->capacity) {
            ofr->capacity=2*ofr->chunkCount+1;
            ofr->chunkName=realloc(ofr->chunkName, ofr->capacity*sizeof(char*));
            ofr->chunkPosition=realloc(ofr->chunkPosition, ofr->capacity*sizeof(long));
        }
        ofr->chunkName[ofr->chunkCount]=dupString(chunkName);
        ofr->chunkPosition[ofr->chunkCount]=startPos;
        ofr->chunkCount++;
        incChunkReference(chunkName, len, chunkContent);
    }
}

// ============================================================================


static int cloudfs_error(char *error_str)
{
    if (errno==0) return -1;
    int retval = -errno;

    fprintf(logFile, "[error]\t CloudFS Error: %s(%d)\n", error_str, errno);
    fflush(logFile);

    /* FUSE always returns -errno to caller (yes, it is negative errno!) */
    return retval;
}

// the returned string needs to be freed
char* getSSDPosition(const char *pathname) {
    char* newStr=malloc(sizeof(char)*MAX_PATH_LEN);
    strcpy(newStr, fsConfig->ssd_path);
    strcat(newStr, pathname);
    return newStr;
}
char* getFusePosition(const char *pathname) {
    char* newStr=malloc(sizeof(char)*MAX_PATH_LEN);
    strcpy(newStr, fsConfig->fuse_path);
    strcat(newStr, pathname);
    return newStr;
}
char* getSSDPositionSlash(const char *pathname) {
    char* newStr=malloc(sizeof(char)*MAX_PATH_LEN);
    sprintf(newStr, "%s/%s", fsConfig->ssd_path, pathname);
    return newStr;
}

int cloudfsMkdir(const char *pathname, mode_t mode) {
    fprintf(logFile, "[mkdir]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int res=mkdir(target, mode);
    free(target);
    if (res>=0) return res;
    return cloudfs_error("mkdir error");
}

// 1 for on (big cloud file), 0 for off, -1 for error, 2 for chunked
int onCloud(const char *filename) {
    char attrBuf[4096];
    int len=getxattr(filename, XATTR_ON_CLOUD, attrBuf, 4096);
    if (len==-1) {
        if (errno==ENODATA) return 0;
        cloudfs_error("onCloud error");
        return -1;
    }
    attrBuf[len]=0;
    if (strcmp(attrBuf, XATTR_ON_CLOUD_NOV)==0) {
        return 0;
    }
    if (strcmp(attrBuf, XATTR_ON_CLOUD_CHUNK)==0) {
        return 2;
    }
    return 1;
}
void cloudfsInitPlaceholder() {
    char* targetPath=getSSDPosition("/.blankplaceholder");
    FILE* f=fopen(targetPath, "w");
    free(targetPath);
    fclose(f);
}
int cloudfsGetAttr(const char *pathname, struct stat *tstat) {
    fprintf(logFile, "[getattr]\t%s\n", pathname);
    fflush(logFile);
    if (strcmp(pathname, SNAPSHOT_FD)==0) {
        char* target=getSSDPosition("/.blankplaceholder");
        int ret=stat(target, tstat);
        free(target);
        if (ret<0) {
            return cloudfs_error("getattr error");
        }
        tstat->st_mode=0100444;
        return 0;
    }
    char* target=getSSDPosition(pathname);
    int ret=stat(target, tstat);
    if (ret<0) {
        free(target);
        return cloudfs_error("getattr error");
    }

    int oc=onCloud(target);
    if (oc<0) {
        free(target);
        return cloudfs_error("getattr error");
    }
    if (oc==0) return 0;

    char attrBuf[4096];
    int len;

    long ss1, ns1;
    long ss2, ns2;
    if (oc==1) {
        len=getxattr(target, XATTR_A_TIME, attrBuf, 4096);
        if (len<0) {
            free(target);
            return cloudfs_error("getattr error");
        }
        attrBuf[len]=0;
        sscanf(attrBuf, "%ld %ld", &ss1, &ns1);

        len=getxattr(target, XATTR_M_TIME, attrBuf, 4096);
        if (len<0) {
            free(target);
            return cloudfs_error("getattr error");
        }
        attrBuf[len]=0;
        sscanf(attrBuf, "%ld %ld", &ss2, &ns2);
    }

    len=getxattr(target, XATTR_SIZE, attrBuf, 4096);
    if (len<0) {
        free(target);
        return cloudfs_error("getattr error");
    }
    attrBuf[len]=0;
    long sz;
    sscanf(attrBuf, "%ld", &sz);

    len=getxattr(target, XATTR_BNUM, attrBuf, 4096);
    if (len<0) {
        free(target);
        return cloudfs_error("getattr error");
    }
    attrBuf[len]=0;
    long bn;
    sscanf(attrBuf, "%ld", &bn);

    free(target);

    tstat->st_size=sz;
    if (oc==1) {
        tstat->st_atim.tv_sec=ss1;
        tstat->st_atim.tv_nsec=ss1;
        tstat->st_mtim.tv_sec=ss2;
        tstat->st_mtim.tv_nsec=ss2;
    }

    tstat->st_blocks=(blkcnt_t)bn;

    fprintf(logFile, "[getattr]\tfile mde: %d\n", tstat->st_mode);
    fflush(logFile);

    return 0;
}

#define IGNORE_PATHNAME "/"
#define IGNORE_FILENAME "lost+found"
#define IGNORE_FILENAME2 ".blankplaceholder"
#define IGNORE_FILENAME3 ".snapshotbox"
#define IGNORE_FILENAME4 ".chunkdir"
#define IGNORE_FILENAME5 ".cache"
#define IGNORE_FILENAME6 ".cachemeta"
int cloudfsReadDir(const char *pathname, void *buf, fuse_fill_dir_t filler, UNUSED off_t offset, UNUSED struct fuse_file_info *fi) {
    fprintf(logFile, "[readdir]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    DIR* dir=opendir(target);
    free(target);

    struct dirent *ep;
    if (dir) {
        while ((ep=readdir(dir))) {
            if (strcmp(ep->d_name, IGNORE_FILENAME)==0 && strcmp(pathname, IGNORE_PATHNAME)==0) continue;
            if (strcmp(ep->d_name, IGNORE_FILENAME2)==0 && strcmp(pathname, IGNORE_PATHNAME)==0) continue;
            if (strcmp(ep->d_name, IGNORE_FILENAME3)==0 && strcmp(pathname, IGNORE_PATHNAME)==0) continue;
            if (strcmp(ep->d_name, IGNORE_FILENAME4)==0 && strcmp(pathname, IGNORE_PATHNAME)==0) continue;
            if (strcmp(ep->d_name, IGNORE_FILENAME5)==0 && strcmp(pathname, IGNORE_PATHNAME)==0) continue;
            if (strcmp(ep->d_name, IGNORE_FILENAME6)==0 && strcmp(pathname, IGNORE_PATHNAME)==0) continue;
            filler(buf, ep->d_name, NULL, 0);
        }
        if (strcmp(pathname, IGNORE_PATHNAME)==0) {
            filler(buf, SNAPSHOT_FD+1, NULL, 0);
        }
    } else {
        closedir(dir);
        return cloudfs_error("readdir error");
    }
    closedir(dir);

    return 0;
}

int cloudfsRmDir(const char *pathname) {
    fprintf(logFile, "[rmdir]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int ret=rmdir(target);
    free(target);
    if (ret>=0) return ret;
    return cloudfs_error("rmdir error");
}

int cloudfsTruncate(const char *pathname, off_t length) {
    fprintf(logFile, "[truncate]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);

    int oc=onCloud(target);
    if (oc<0) {
        free(target);
        return cloudfs_error("truncate error");
    }
    if (oc==0) {
        int ret=truncate(target, length);
        free(target);
        if (ret>=0) return ret;
        return cloudfs_error("truncate error");
    }
    if (oc==2) {
        // the file is chunked. So we should modify the trailing chunk and consider
        // whether the file is truncated below threshold
        struct fuse_file_info fi;
        fi.flags=O_WRONLY;
        if (cloudfsOpen(pathname, &fi)<0) {
            free(target);
            return cloudfs_error("truncate error");
        }
        openedFile* ofr=(openedFile*)HGet(openfileTable, target);
        if (length<=fsConfig->threshold) {
            // the file is truncated below threshold, put the file back onto disk
            char* smallFileContent=(char*)malloc(sizeof(char)*(length+10));
            char* sfcHead=smallFileContent;
            long rest=(long)length;
            int i;
            for (i=0; i<ofr->chunkCount; i++) {
                long chunkLen;
                char* tmp=getChunkRaw(ofr->chunkName[i], &chunkLen);
                if (chunkLen>rest) chunkLen=rest;
                memcpy(sfcHead, tmp, chunkLen);
                free(tmp);
                rest-=chunkLen;
                sfcHead+=chunkLen;
                if (rest==0) break;
            }
            // cut off all the chunks and perform a rearrangeChunkList. So that
            // the references are maintained.
            for (i=0; i<ofr->chunkCount; i++) ofr->chunkPosition[i]=0x7fffffff;
            rearrangeChunkList(ofr);
            cloudfsRelease(pathname, &fi);
            // write the content to the file
            FILE* outfile=fopen(target, "wb");
            if (!outfile) {
                free(target);
                free(smallFileContent);
                return cloudfs_error("truncate error");
            }
            long written=0;
            while (written<(long)length) {
                written+=fwrite(smallFileContent+written, 1, ((long)length-written), outfile);
            }
            fclose(outfile);
            // TODO: bug: when truncating and write it above threshold. Crash.
            free(smallFileContent);
            if (setxattr(target, XATTR_ON_OLDCLOUD, XATTR_ON_CLOUD_NOV, strlen(XATTR_ON_CLOUD_NOV), 0)<0)
                cloudfs_error("disposeFile: put xattr error");
            if (setxattr(target, XATTR_ON_CLOUD, XATTR_ON_CLOUD_NOV, strlen(XATTR_ON_CLOUD_NOV), 0)<0)
                cloudfs_error("disposeFile: put xattr error");
        } else {
            // the file is still chunked. so modify the chunkList and consider alter
            // the trailing chunk.
            int endPoint=findRightChunk(ofr, length-1);
            if (endPoint>=0) {
                int i;
                long startPos=ofr->chunkPosition[endPoint];
                long cr;
                for (i=endPoint; i<ofr->chunkCount; i++)
                    ofr->chunkPosition[i]=0x7fffffff;
                char* tmp=getChunkRaw(ofr->chunkName[endPoint], &cr);

                long thisLen=length-startPos;
                // after truncated, the trailing chunk can be at most one chunk.
                MD5_CTX ctx;
                MD5_Init(&ctx);
                MD5_Update(&ctx, tmp, thisLen);
                unsigned char md5[MD5_DIGEST_LENGTH];
                char chunkname[2*MD5_DIGEST_LENGTH+1];
                MD5_Final(md5, &ctx);
                for(i=0; i<MD5_DIGEST_LENGTH; i++)
                    sprintf(chunkname+2*i, "%02x", md5[i]);
                chunkname[2*MD5_DIGEST_LENGTH]=0;
                putChunk(ofr, 0x7fffffff, chunkname, startPos, thisLen, tmp);
                rearrangeChunkList(ofr);
                free(tmp);
                cloudfsRelease(pathname, &fi);
            }
        }
        ofr=(openedFile*)HGet(openfileTable, target);
        if (ofr) ofr->mode=0;
        free(target);
        pushChunkTable();
        return 0;
    }

    char attrBuf[4096];
    sprintf(attrBuf, "%ld", (long)length);

    if (setxattr(target, XATTR_SIZE, attrBuf, strlen(attrBuf), 0)<0) {
        free(target);
        return cloudfs_error("disposeFile: put xattr error");
    }
    free(target);
    return 0;
}

int cloudfsStatfs(const char *pathname, struct statvfs *buf) {
    fprintf(logFile, "[statfs]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int ret=statvfs(target, buf);
    free(target);
    if (ret>=0) return ret;
    return cloudfs_error("statfs error");
}

int cloudfsMknod(const char *pathname, mode_t mode, dev_t dev) {
    fprintf(logFile, "[mknod]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int ret=mknod(target, mode, dev);
    free(target);
    if (ret>=0) return ret;
    return cloudfs_error("mknod error");
}

int cloudfsUTimens(const char *pathname, const struct timespec tv[2]) {
    fprintf(logFile, "[utimens]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);

    if (utimensat(0, target, tv, 0)<0) {
        free(target);
        return cloudfs_error("timens: utime error");
    }
    char attrBuf[4096];
    sprintf(attrBuf, "%ld %ld", (long)tv[0].tv_sec, (long)tv[0].tv_nsec);
    if (setxattr(target, XATTR_A_TIME, attrBuf, strlen(attrBuf), 0)<0) {
        free(target);
        return cloudfs_error("timens: put xattr error");
    }
    sprintf(attrBuf, "%ld %ld", (long)tv[1].tv_sec, (long)tv[1].tv_nsec);
    if (setxattr(target, XATTR_M_TIME, attrBuf, strlen(attrBuf), 0)<0) {
        free(target);
        return cloudfs_error("timens: put xattr error");
    }

    free(target);
    return 0;
}

int cloudfsChmod(const char * pathname, mode_t mode) {
    fprintf(logFile, "[chmod]\t%s: %d\n", pathname, (int)mode);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int ret=chmod(target, mode);
    free(target);
    if (ret>=0) return ret;
    return cloudfs_error("chmod error");
}

// if the file is chunked, it is treated as no name on the cloud.
int getCloudName(const char *filename, char* attrBuf, int size) {
    int len=getxattr(filename, XATTR_ON_CLOUD, attrBuf, size);
    if (len==-1) {
        if (errno==ENODATA) return 0;
        cloudfs_error("getCloudName error");
        return -1;
    }
    attrBuf[len]=0;
    if (strcmp(attrBuf, XATTR_ON_CLOUD_NOV)==0) {
        return 0;
    }
    if (strcmp(attrBuf, XATTR_ON_CLOUD_CHUNK)==0) {
        return 2;
    }
    return 1;
}

int cloudfsUnlink(const char *pathname) {
    fprintf(logFile, "[unlink]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);

    char attrBuf[4096];
    int oc=getCloudName(target, attrBuf, 4096);
    if (oc<0) {
        free(target);
        return -1;
    }
    if (oc==1) {
        if (cloud_delete_object(CONTAINER_NAME, attrBuf)!=S3StatusOK) {
            cloudfs_error("removing error");
        }
    }
    if (oc==2) {
        // when the file is chunked, we truncate it to zero to free all the chunks
        // before really remove it
        if (cloudfsTruncate(pathname, 0)<0) {
            free(target);
            return cloudfs_error("removing error");
        }
    }
    int ret=unlink(target);
    free(target);
    if (ret<0) return cloudfs_error("unlink error");

    return ret;
}

static FILE *outfile;
int get_buffer(const char *buffer, int bufferLength) {
    return fwrite(buffer, 1, bufferLength, outfile);
}
// only work when the file is not chunked. (Old function for nodedup mode)
// when error, return <0; when succ, return 0; when the file is chunked, return 1;
int ensureFileExist(const char *filename) {
    char attrBuf[4096];
    int len=getxattr(filename, XATTR_ON_CLOUD, attrBuf, 4096);
    if (len<0) {
        if (errno==ENODATA) return 0;
        cloudfs_error("ensureFileExist error");
        return -1;
    }
    attrBuf[len]=0;
    if (strcmp(attrBuf, XATTR_ON_CLOUD_NOV)==0) {
        return 0;
    }
    if (strcmp(attrBuf, XATTR_ON_CLOUD_CHUNK)==0) {
        return 1;
    }

    // modify the perm
    struct stat tst;
    if (stat(filename, &tst)<0) return cloudfs_error("ensureFileExist error: stat");
    if (chmod(filename, 0100666)<0) return cloudfs_error("ensureFileExist error: chmod");

    // fetch the file from cloud
    outfile=fopen(filename, "wb");
    S3Status s=cloud_get_object(CONTAINER_NAME, attrBuf, get_buffer);
    fclose(outfile);
    if (s!=S3StatusOK) return -1;
    // restore time
    len=getxattr(filename, XATTR_SIZE, attrBuf, 4096);
    if (len<0) return -1;
    attrBuf[len]=0;
    long sz;
    sscanf(attrBuf, "%ld", &sz);
    if (truncate(filename, sz)<0) return -1;

    len=getxattr(filename, XATTR_A_TIME, attrBuf, 4096);
    if (len<0) return -1;
    attrBuf[len]=0;
    long ss1, ns1;
    sscanf(attrBuf, "%ld %ld", &ss1, &ns1);

    len=getxattr(filename, XATTR_M_TIME, attrBuf, 4096);
    if (len<0) return -1;
    attrBuf[len]=0;
    long ss2, ns2;
    sscanf(attrBuf, "%ld %ld", &ss2, &ns2);

    struct timespec _times[2];
    _times[0].tv_sec=ss1;
    _times[0].tv_nsec=ns1;
    _times[1].tv_sec=ss2;
    _times[1].tv_nsec=ns2;
    utimensat(0, filename, _times, 0);

    if (setxattr(filename, XATTR_ON_CLOUD, XATTR_ON_CLOUD_NOV, strlen(XATTR_ON_CLOUD_NOV), 0)<0) return -1;

    if (chmod(filename, tst.st_mode)<0) return cloudfs_error("ensureFileExist error: chmod");

    return 0;
}
void generateObjname(char *buf, const char *filename) {
    sprintf(buf, "%s-%d", filename, (int)time(NULL));
    while (*buf) {
        if (*buf=='/') *buf='-';
        buf++;
    }
}

int saveChunkedFile(const char *filename, openedFile* ofr) {
    if (!ofr->dirty) return 0;
    FILE* f=fopen(filename, "w");
    if (!f) return -1;
    fprintf(f, "%d\n", ofr->chunkCount);
    int i;
    for (i=0; i<ofr->chunkCount; i++) {
        fprintf(f, "%ld %s\n", ofr->chunkPosition[i], ofr->chunkName[i]);
    }
    fclose(f);

    char attrBuf[4096];
    sprintf(attrBuf, "%ld", getLen(ofr));
    if (setxattr(filename, XATTR_SIZE, attrBuf, strlen(attrBuf), 0)<0) return cloudfs_error("writeChunkfile: put xattr error");
    sprintf(attrBuf, "%ld", (long)233);
    if (setxattr(filename, XATTR_BNUM, attrBuf, strlen(attrBuf), 0)<0) return cloudfs_error("writeChunkfile: put xattr error");

    if (setxattr(filename, XATTR_ON_CLOUD, XATTR_ON_CLOUD_CHUNK, strlen(XATTR_ON_CLOUD_CHUNK), 0)<0) return cloudfs_error("writeChunkfile: put xattr error");
    return 0;
}

int getOldCloudName(const char *filename, char* attrBuf, int size) {
    int len=getxattr(filename, XATTR_ON_OLDCLOUD, attrBuf, size);
    if (len==-1) {
        if (errno==ENODATA) return 0;
        cloudfs_error("getCloudName error");
        return -1;
    }
    attrBuf[len]=0;
    if (strcmp(attrBuf, XATTR_ON_CLOUD_NOV)==0) {
        return 0;
    }
    if (strcmp(attrBuf, XATTR_ON_CLOUD_CHUNK)==0) {
        return 0;
    }
    return 1;
}
static FILE *infile;
int put_buffer(char *buffer, int bufferLength) {
    return fread(buffer, 1, bufferLength, infile);
}
// only work when the file is not chunked. (Old function for nodedup mode)
// when error, return <0; when succ, return 0 (for chunked file nothing will be done);
int disposeFile(const char *filename) {
    struct stat tstat;
    int ret=stat(filename, &tstat);
    if (ret<0) return cloudfs_error("disposeFile: get stat error");
    char attrBuf[4096];

    if (tstat.st_size<=fsConfig->threshold || !fsConfig->no_dedup) {
        int isCN=getOldCloudName(filename, attrBuf, 4096);
        if (isCN<0) return -1;
        if (isCN==1) {
            if (setxattr(filename, XATTR_ON_OLDCLOUD, XATTR_ON_CLOUD_NOV, strlen(XATTR_ON_CLOUD_NOV), 0)<0)
                return cloudfs_error("disposeFile: put xattr error");
            if (cloud_delete_object(CONTAINER_NAME, attrBuf)!=S3StatusOK) {
                cloudfs_error("removing error");
            }
        }
        return 0;
    }

    // modify the perm
    struct stat tst;
    if (stat(filename, &tst)<0) return cloudfs_error("ensureFileExist error: stat");
    if (chmod(filename, 0100666)<0) return cloudfs_error("ensureFileExist error: chmod");

    sprintf(attrBuf, "%ld", (long)tstat.st_size);
    if (setxattr(filename, XATTR_SIZE, attrBuf, strlen(attrBuf), 0)<0) return cloudfs_error("disposeFile: put xattr error");
    sprintf(attrBuf, "%ld", (long)tstat.st_blocks);
    if (setxattr(filename, XATTR_BNUM, attrBuf, strlen(attrBuf), 0)<0) return cloudfs_error("disposeFile: put xattr error");
    sprintf(attrBuf, "%ld %ld", (long)tstat.st_atim.tv_sec, (long)tstat.st_atim.tv_nsec);
    if (setxattr(filename, XATTR_A_TIME, attrBuf, strlen(attrBuf), 0)<0) return cloudfs_error("disposeFile: put xattr error");
    sprintf(attrBuf, "%ld %ld", (long)tstat.st_mtim.tv_sec, (long)tstat.st_mtim.tv_nsec);
    if (setxattr(filename, XATTR_M_TIME, attrBuf, strlen(attrBuf), 0)<0) return cloudfs_error("disposeFile: put xattr error");

    int isCN=getOldCloudName(filename, attrBuf, 4096);
    if (isCN<0) return -1;
    if (isCN==0) generateObjname(attrBuf, filename);
    infile=fopen(filename, "rb");
    if (infile == NULL) {
        cloudfs_error("disposeFile: file does not exist");
        return -1;
    }
    S3Status s=cloud_put_object(CONTAINER_NAME, attrBuf, tstat.st_size, put_buffer);
    fclose(infile);
    if (s!=S3StatusOK) return -1;

    if (truncate(filename, 0)<0) return cloudfs_error("disposeFile: truncate error");
    if (setxattr(filename, XATTR_ON_OLDCLOUD, attrBuf, strlen(attrBuf), 0)<0) return cloudfs_error("disposeFile: put xattr error");
    if (setxattr(filename, XATTR_ON_CLOUD, attrBuf, strlen(attrBuf), 0)<0) return cloudfs_error("disposeFile: put xattr error");

    if (chmod(filename, tst.st_mode)<0) return cloudfs_error("ensureFileExist error: chmod");

    return 0;
}

int loadInChunkedFile(const char *filename, openedFile* ofr) {
    FILE* f=fopen(filename, "r");
    if (!f) return -1;
    ofr->chunkCount=0;
    int t=fscanf(f, "%d", &ofr->chunkCount);
    (void)t;
    ofr->capacity=ofr->chunkCount;
    ofr->chunkName=NULL;
    ofr->chunkPosition=NULL;
    ofr->dirty=false;
    if (ofr->chunkCount==0) return 0;
    ofr->chunkName=(char**)malloc(sizeof(char*)*ofr->chunkCount);
    ofr->chunkPosition=(long*)malloc(sizeof(long)*ofr->chunkCount);
    int i;
    for (i=0; i<ofr->chunkCount; i++) {
        ofr->chunkName[i]=(char*)malloc(sizeof(char)*200);
        t=(fscanf(f, "%ld %s", &ofr->chunkPosition[i], ofr->chunkName[i]));
        (void)t;
    }
    fclose(f);
    return 0;
}

int cloudfsOpen(const char *pathname, struct fuse_file_info *fi) {
    fprintf(logFile, "[open]\t%s\n", pathname);
    fflush(logFile);
    if (strcmp(pathname, SNAPSHOT_FD)==0) {
        fprintf(logFile, "[open]\tSpecial File! %d\n", fi->flags);
        fflush(logFile);
        if ((fi->flags & O_WRONLY)==0) {
            return 0;
        }
    }
    char* target=getSSDPosition(pathname);
    openedFile* ofr=(openedFile*)malloc(sizeof(openedFile));
    ofr->refCount=0;
    ofr->totalLen=-1;
    ofr->dirty=false;
    if (!HPutIfAbsent(openfileTable, target, ofr)) {
        free(ofr);
        ofr=(openedFile*)HGet(openfileTable, target);
    }
    if (ofr->refCount==0) {
        int res=ensureFileExist(target);
        if (res<0) {
            free(target);
            cloudfs_error("open error: error in fetch");
            return -1;
        } else if (res==1) {
            // chunked file, load the file content into memory
            ofr->mode=1;
            fi->fh=0;
            if (loadInChunkedFile(target, ofr)<0) {
                // may have mem leak
                free(target);
                return cloudfs_error("open error");
            }
        } else {
            ofr->mode=0;
        }
    }
    int ret=open(target, fi->flags);
    ofr->refCount++;
    if (ret<0) {
        fi->fh=0;
        cloudfsRelease(pathname, fi);
        return cloudfs_error("open error");
    }
    if (ofr->mode==0) {
        fi->fh=ret;
    } else {
        close(ret);
    }

    free(target);
    return 0;
}

int cloudfsRelease(const char *pathname, struct fuse_file_info *fi) {
    fprintf(logFile, "[release]\t%s\n", pathname);
    fflush(logFile);
    if (strcmp(pathname, SNAPSHOT_FD)==0) {
        return 0;
    }
    char* target=getSSDPosition(pathname);
    openedFile* ofr=(openedFile*)HGet(openfileTable, target);
    ofr->refCount--;
    if (ofr->mode==0) {
        close(fi->fh);
    }
    if (ofr->refCount==0) {
        if (ofr->mode==0) {
            if (disposeFile(target)<0) {
                cloudfs_error("open error: error in dispose");
                return -1;
            }
        } else {
            if (saveChunkedFile(target, ofr)<0) {
                cloudfs_error("open error: error in dispose");
                return -1;
            }
        }

        HRemove(openfileTable, target);
        disposeOF(ofr);
    }
    free(target);

    return 0;
}

int cloudfsFsync(UNUSED const char* pathname, int isdatasync, struct fuse_file_info* fi) {
    fprintf(logFile, "[fsync]\t%s\n", pathname);
    fflush(logFile);
    int ret=0;
    if (fi->fh>0) {
        if (isdatasync) ret=(fdatasync(fi->fh));
        else ret=(fsync(fi->fh));
    }

    if (ret>=0) return ret;
    return cloudfs_error("fsync error");
}

int cloudfsRead(const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(logFile, "[read]\t%s\n", pathname);
    fflush(logFile);
    if (strcmp(pathname, SNAPSHOT_FD)==0) {
        return 0;
    }

    char* target=getSSDPosition(pathname);
    openedFile* ofr=(openedFile*)HGet(openfileTable, target);
    free(target);
    if (ofr->mode==0) {
        if (lseek(fi->fh, offset, SEEK_SET)==-1) return cloudfs_error("read error");
        int transferred=0;
        while (size>0) {
            int bt=read(fi->fh, buf, size);
            if (bt<0) return cloudfs_error("read error");
            transferred+=bt;
            if (bt==0) break;
            size-=bt;
            buf+=bt;
        }
        return transferred;
    }

    // oops! the file is chunked.
    int transferred=0;
    while (size>0) {
        int chunkNumber=findRightChunk(ofr, offset);
        if (chunkNumber>=ofr->chunkCount || chunkNumber==-1) {
            // eof!
            break;
        }
        long bt;
        long inc=offset-ofr->chunkPosition[chunkNumber];
        char* tmp=(char*)getChunkRaw(ofr->chunkName[chunkNumber], &bt);
        bt-=inc;
        if (bt>(long)size) bt=(long)size;
        memcpy(buf, tmp+inc, bt*sizeof(char));
        offset+=bt;
        buf+=bt;
        size-=bt;
        transferred+=bt;
        free(tmp);
    }
    return transferred;
}

void convertFileToChunks(const char* filename, openedFile* ofr) {
    fprintf(logFile, "[convertFileToChunks]\t%s\n", filename);
    fflush(logFile);
    ofr->mode=1;
    ofr->capacity=ofr->chunkCount=0;
    ofr->chunkName=NULL;
    ofr->chunkPosition=NULL;
    int fd=open(filename, O_RDONLY);
    MD5_CTX ctx;
    rabin_reset(rp);
    unsigned char md5[MD5_DIGEST_LENGTH];
    char chunkname[MD5_DIGEST_LENGTH*2+1];
    char* vs=(char*)malloc(sizeof(char)*(fsConfig->max_seg_size+1));
    char* nvs=vs;
	int new_segment = 0;
    long segment_start=0;
	int len, segment_len = 0, b;
	char buf[1024];
	int bytes;

	MD5_Init(&ctx);
	while( (bytes = read(fd, buf, sizeof buf)) > 0 ) {
		char *buftoread = (char *)&buf[0];
		while ((len = rabin_segment_next(rp, buftoread, bytes, &new_segment)) > 0) {
			MD5_Update(&ctx, buftoread, len);
			segment_len += len;
            memcpy(nvs, buftoread, len);
            nvs+=len;

			if (new_segment) {
				MD5_Final(md5, &ctx);
				for(b = 0; b < MD5_DIGEST_LENGTH; b++) {
                    sprintf(chunkname+(2*b), "%02x", md5[b]);
                }
                chunkname[2*MD5_DIGEST_LENGTH]=0;
                putChunk(ofr, 0x7fffffff, chunkname, segment_start, nvs-vs, vs);
				MD5_Init(&ctx);

                segment_start+=nvs-vs;
				segment_len = 0;
                nvs=vs;
			}

			buftoread += len;
			bytes -= len;

			if (!bytes) {
				break;
			}
		}
	}
    if (nvs!=vs) {
        // trailing chunk found. write it regardless of its length.
        MD5_Final(md5, &ctx);
        for(b = 0; b < MD5_DIGEST_LENGTH; b++) {
            sprintf(chunkname+(2*b), "%02x", md5[b]);
        }
        chunkname[2*MD5_DIGEST_LENGTH]=0;
        putChunk(ofr, 0x7fffffff, chunkname, segment_start, nvs-vs, vs);
    }
    free(vs);
    pushChunkTable();
    printOF(ofr);
    close(fd);
}

int cloudfsWrite(const char* pathname, const char *buf, size_t size, off_t offset, struct fuse_file_info* fi) {
    fprintf(logFile, "[write]\t%s\n", pathname);
    fflush(logFile);
    if (size==0) return 0;

    char* target=getSSDPosition(pathname);
    openedFile* ofr=(openedFile*)HGet(openfileTable, target);

    if (ofr->mode==0) {
        if (lseek(fi->fh, offset, SEEK_SET)==-1) return cloudfs_error("write error");
        int transferred=0;
        while (size>0) {
            int bt=write(fi->fh, buf, size);
            if (bt<0) return cloudfs_error("write error");
            transferred+=bt;
            size-=bt;
            buf+=bt;
        }
        if (!fsConfig->no_dedup && (long)(size+offset)>fsConfig->threshold) {
            close(fi->fh);
            convertFileToChunks(target, ofr);
            fi->fh=0;
        }
        free(target);
        return transferred;
    }
    // oops! chunked file. We should firstly fetch the chunk where the startPosition
    // is located, and then invalidating all the overlapped chunks, and put new chunks
    // in. Keep track of the trailing chunk and concatenate it with subsequent chubks,
    // until there's no chunk left.

    fprintf(logFile, "[write]\tchunk mode find\n");
    fflush(logFile);

    size_t oriSize=size;
    int startChunkNumber=findRightChunk(ofr, offset);
    int startPosition;
    openedFile* newofr=makeDupOF(ofr);

    char* tSpace=(char*)malloc(sizeof(char)*(size+2*fsConfig->max_seg_size));

    fprintf(logFile, "[write]\talgining head\n");
    fflush(logFile);

    if (startChunkNumber<0 && ofr->chunkCount>0) startChunkNumber=ofr->chunkCount-1;
    if (startChunkNumber>=0) {
        long chunkLen;
        fprintf(logFile, "[write]\tgetting chunk %s\n", ofr->chunkName[startChunkNumber]);
        fflush(logFile);
        char* tmp=getChunkRaw(ofr->chunkName[startChunkNumber], &chunkLen);
        memcpy(tSpace, tmp, chunkLen);
        free(tmp);
        memcpy(tSpace+(offset-ofr->chunkPosition[startChunkNumber]), buf, size);
        size+=offset-ofr->chunkPosition[startChunkNumber];
        startPosition=ofr->chunkPosition[startChunkNumber];
    } else {
        memcpy(tSpace, buf, size);
        startPosition=getLen(ofr);
    }

    // now, tSpace hold the content to be written and startPosition is the absolute
    // start position; size is the size of tSpace;

    rabin_reset(rp);
    MD5_CTX ctx;
    MD5_Init(&ctx);

    char* tSpaceHead=tSpace;
    char* segHead=tSpaceHead;

    while (true) {
        // always, startPosition is the absolute position of segHead,
        // tSpaceHead is the remaining data needs to be processed, and
        // size is the total left size of tSpaceHead
        int new_segment;
        int segLen;
        fprintf(logFile, "[write]\tpulling old chunks\n");
        fflush(logFile);
        while ((segLen=rabin_segment_next(rp, tSpaceHead, size, &new_segment))>0) {
            MD5_Update(&ctx, tSpaceHead, segLen);
            if (new_segment) {
                // seg: [segHead, tSpaceHead+segLen)
                // firstly, disable the old seg hold the segment
                fprintf(logFile, "[write]\tnew chunk at [%d, %ld)\n", startPosition, startPosition+(tSpaceHead+segLen-segHead));
                fflush(logFile);
                int oldSegNumber=findRightChunk(ofr, startPosition);
                int oldSegNumberEnd=findRightChunk(ofr, startPosition+(tSpaceHead+segLen-segHead)-1);
                if (oldSegNumber>=0) {
                    if (oldSegNumberEnd<0) oldSegNumberEnd=ofr->chunkCount-1;
                    int j;
                    for (j=oldSegNumber; j<=oldSegNumberEnd; j++)
                        newofr->chunkPosition[j]=0x7fffffff;
                }
                // then, save the chunk
                unsigned char md5[MD5_DIGEST_LENGTH];
                char chunkname[2*MD5_DIGEST_LENGTH+1];
                MD5_Final(md5, &ctx);
                int i;
                for(i=0; i<MD5_DIGEST_LENGTH; i++)
                    sprintf(chunkname+2*i, "%02x", md5[i]);
                chunkname[2*MD5_DIGEST_LENGTH]=0;
                putChunk(newofr, 0x7fffffff, chunkname, startPosition, tSpaceHead+segLen-segHead, segHead);
                // finally, update the variables
                startPosition+=tSpaceHead+segLen-segHead;
                segHead=tSpaceHead+segLen;
                MD5_Init(&ctx);
            }
            tSpaceHead+=segLen;
            size-=segLen;
            if (size==0) break;
        }
        fprintf(logFile, "[write]\tapplying cascading modification\n");
        fflush(logFile);
        // attention: may have bugs. what if the subsequent chunk has already been
        // disabledd but the previous chunk end here?
        if (tSpaceHead!=segHead) {
            // still sth left, try to initiate new chunk in
            int replementChunkNumber=findRightChunk(ofr, startPosition+(tSpaceHead-segHead));
            if (replementChunkNumber<0) {
                // no more, just add the tail and jump out
                // [SegHead, tSpaceHead)
                fprintf(logFile, "[write]\tnew chunk at [%d, %ld)\n", startPosition, startPosition+(tSpaceHead-segHead));
                fflush(logFile);
                int oldSegNumber=findRightChunk(ofr, startPosition);
                int oldSegNumberEnd=findRightChunk(ofr, startPosition+(tSpaceHead-segHead)-1);
                if (oldSegNumber>=0) {
                    if (oldSegNumberEnd<0) oldSegNumberEnd=ofr->chunkCount-1;
                    int j;
                    for (j=oldSegNumber; j<=oldSegNumberEnd; j++)
                        newofr->chunkPosition[j]=0x7fffffff;
                }

                unsigned char md5[MD5_DIGEST_LENGTH];
                char chunkname[2*MD5_DIGEST_LENGTH+1];
                MD5_Final(md5, &ctx);
                int i;
                for(i=0; i<MD5_DIGEST_LENGTH; i++)
                    sprintf(chunkname+2*i, "%02x", md5[i]);
                chunkname[2*MD5_DIGEST_LENGTH]=0;
                putChunk(newofr, 0x7fffffff, chunkname, startPosition, tSpaceHead-segHead, segHead);
                break;
            }
            long chunkLen;
            char* tmp=getChunkRaw(ofr->chunkName[replementChunkNumber], &chunkLen);
            char* newtSpace=(char*)malloc(sizeof(char)*(chunkLen+fsConfig->max_seg_size));
            memcpy(newtSpace, segHead, tSpaceHead-segHead);
            long p=tSpaceHead-segHead;
            segHead=newtSpace;
            tSpaceHead=segHead+p;
            free(tSpace);
            tSpace=newtSpace;
            size=chunkLen-(startPosition+p-ofr->chunkPosition[replementChunkNumber]);

            memcpy(tSpaceHead, tmp+(startPosition+p-ofr->chunkPosition[replementChunkNumber]),
                chunkLen-(startPosition+p-ofr->chunkPosition[replementChunkNumber]));
            free(tmp);
        } else {
            break;
        }
    }
    fprintf(logFile, "[write]\tpush modification\n");
    fflush(logFile);

    free(tSpace);

    printOF(newofr);
    rearrangeChunkList(newofr);
    disposeOF(ofr);
    HRemove(openfileTable, target);
    HPutIfAbsent(openfileTable, target, newofr);
    free(target);
    pushChunkTable();
    return oriSize;
}

int cloudfsGetXAttr(const char *pathname, const char *name, char *value, size_t size) {
    fprintf(logFile, "[getxaddr]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int ret=getxattr(target, name, value, size);
    free(target);
    if (ret>=0) return ret;
    return cloudfs_error("getxaddr error");
}

int cloudfsSetXAttr(const char *pathname, const char *name, const char *value, size_t size, int flags) {
    fprintf(logFile, "[setxaddr]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int ret=setxattr(target, name, value, size, flags);
    free(target);
    if (ret>=0) return ret;
    return cloudfs_error("setxaddr error");
}

int cloudfsAccess(const char *pathname, int mask) {
    fprintf(logFile, "[access]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int ret=access(target, mask);
    free(target);
    return ret;
}

int cloudfsIOctl(const char *pathname, int cmd, UNUSED void *arg, UNUSED struct fuse_file_info *fi, UNUSED unsigned int flags, void *data) {
    if (strcmp(pathname, SNAPSHOT_FD)!=0) return -1;
    if (cmd==CLOUDFS_SNAPSHOT) {
        long ext=createSnapshot();
        fprintf(logFile2, "[snapshot]\tcreate: %ld\n", ext);
        fflush(logFile2);
        if (ext<0) return -1;
        pushSnapshot();
        *(unsigned long*)data=(unsigned long)ext;
        return 0;
    } else if (cmd==CLOUDFS_SNAPSHOT_LIST) {
        listSnapshot((long*)data);
        return 0;
    } else if (cmd==CLOUDFS_INSTALL_SNAPSHOT) {
        int ret=installSnapshot(*(long*)data);
        fprintf(logFile2, "[snapshot]\tinstall: %d\n", ret);
        fflush(logFile2);
        pushSnapshot();
        return ret;
    } else if (cmd==CLOUDFS_UNINSTALL_SNAPSHOT) {
        int ret=uninstallSnapshot(*(long*)data);
        fprintf(logFile2, "[snapshot]\tuninstall: %d\n", ret);
        fflush(logFile2);
        pushSnapshot();
        return ret;
    } else if (cmd==CLOUDFS_DELETE) {
        int ret=removeSnapshot(*(long*)data);
        fprintf(logFile2, "[snapshot]\tremove: %d\n", ret);
        fflush(logFile2);
        pushSnapshot();
        return ret;
    } else if (cmd==CLOUDFS_RESTORE) {
        int ret=restoreSnapshot(*(long*)data);
        fprintf(logFile2, "[snapshot]\trestore: %d\n", ret);
        fflush(logFile2);
        pushSnapshot();
        return ret;
    }
    return -1;
}
