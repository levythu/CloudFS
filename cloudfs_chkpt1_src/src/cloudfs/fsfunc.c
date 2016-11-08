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

#include "hashtable.h"
#include "cloudapi.h"
#include "libs3.h"
#include "cloudfs.h"
#include "fsfunc.h"

#define UNUSED __attribute__((unused))

#define XATTR_ON_CLOUD "user.cloudfs.on.cloud"
#define XATTR_ON_OLDCLOUD "user.cloudfs.on.old.cloud"
#define XATTR_ON_CLOUD_NOV "//notInTheCloud"
#define XATTR_M_TIME "user.cloudfs.mtime"
#define XATTR_A_TIME "user.cloudfs.atime"
#define XATTR_SIZE "user.cloudfs.size"
#define XATTR_BNUM "user.cloudfs.blocknum"

FILE *logFile=NULL;
struct cloudfs_state* fsConfig=NULL;
Hashtable openfileTable;

typedef struct _openedFile {
    int refCount;
} openedFile;

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

int cloudfsMkdir(const char *pathname, mode_t mode) {
    fprintf(logFile, "[mkdir]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int res=mkdir(target, mode);
    free(target);
    if (res>=0) return res;
    return cloudfs_error("mkdir error");
}

// 1 for on, 0 for off, -1 for error
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
    return 1;
}
int cloudfsGetAttr(const char *pathname, struct stat *tstat) {
    fprintf(logFile, "[getattr]\t%s\n", pathname);
    fflush(logFile);
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
    len=getxattr(target, XATTR_A_TIME, attrBuf, 4096);
    if (len<0) {
        free(target);
        return cloudfs_error("getattr error");
    }
    attrBuf[len]=0;
    long ss1, ns1;
    sscanf(attrBuf, "%ld %ld", &ss1, &ns1);

    len=getxattr(target, XATTR_M_TIME, attrBuf, 4096);
    if (len<0) {
        free(target);
        return cloudfs_error("getattr error");
    }
    attrBuf[len]=0;
    long ss2, ns2;
    sscanf(attrBuf, "%ld %ld", &ss2, &ns2);

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
    tstat->st_atim.tv_sec=ss1;
    tstat->st_atim.tv_nsec=ss1;
    tstat->st_mtim.tv_sec=ss2;
    tstat->st_mtim.tv_nsec=ss2;
    tstat->st_blocks=(blkcnt_t)bn;

    fprintf(logFile, "[getattr]\tfile mde: %d\n", tstat->st_mode);
    fflush(logFile);

    return 0;
}

#define IGNORE_PATHNAME "/"
#define IGNORE_FILENAME "lost+found"
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
            filler(buf, ep->d_name, NULL, 0);
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
    int ret=unlink(target);
    free(target);
    if (ret<0) return cloudfs_error("unlink error");
    if (oc==1) {
        if (cloud_delete_object(CONTAINER_NAME, attrBuf)!=S3StatusOK) {
            cloudfs_error("removing error");
        }
    }

    return ret;
}

static FILE *outfile;
int get_buffer(const char *buffer, int bufferLength) {
    return fwrite(buffer, 1, bufferLength, outfile);
}
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
    return 1;
}
static FILE *infile;
int put_buffer(char *buffer, int bufferLength) {
    return fread(buffer, 1, bufferLength, infile);
}
int disposeFile(const char *filename) {
    struct stat tstat;
    int ret=stat(filename, &tstat);
    if (ret<0) return cloudfs_error("disposeFile: get stat error");
    char attrBuf[4096];

    if (tstat.st_size<=fsConfig->threshold) {
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

int cloudfsOpen(const char *pathname, struct fuse_file_info *fi) {
    fprintf(logFile, "[open]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    openedFile* ofr=(openedFile*)malloc(sizeof(openedFile));
    ofr->refCount=0;
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
        }
    }
    int ret=open(target, fi->flags);
    free(target);
    ofr->refCount++;
    if (ret<0) {
        fi->fh=0;
        cloudfsRelease(pathname, fi);
        return cloudfs_error("open error");
    }
    fi->fh=ret;
    return 0;
}

int cloudfsRelease(const char *pathname, struct fuse_file_info *fi) {
    fprintf(logFile, "[release]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    if (fi->fh>0) close(fi->fh);
    openedFile* ofr=(openedFile*)HGet(openfileTable, target);
    ofr->refCount--;
    if (ofr->refCount==0) {
        if (disposeFile(target)<0) {
            cloudfs_error("open error: error in dispose");
            return -1;
        }
        free(HRemove(openfileTable, target));
    }
    free(target);

    return 0;
}

int cloudfsFsync(UNUSED const char* pathname, int isdatasync, struct fuse_file_info* fi) {
    fprintf(logFile, "[fsync]\t%s\n", pathname);
    fflush(logFile);
    int ret;
    if (isdatasync) ret=(fdatasync(fi->fh));
    else ret=(fsync(fi->fh));

    if (ret>=0) return ret;
    return cloudfs_error("fsync error");
}

int cloudfsRead(UNUSED const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(logFile, "[read]\t%s\n", pathname);
    fflush(logFile);
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

int cloudfsWrite(UNUSED const char* pathname, const char *buf, size_t size, off_t offset, struct fuse_file_info* fi) {
    fprintf(logFile, "[write]\t%s\n", pathname);
    fflush(logFile);
    if (lseek(fi->fh, offset, SEEK_SET)==-1) return cloudfs_error("write error");
    int transferred=0;
    while (size>0) {
        int bt=write(fi->fh, buf, size);
        if (bt<0) return cloudfs_error("write error");
        transferred+=bt;
        size-=bt;
        buf+=bt;
    }
    return transferred;
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
