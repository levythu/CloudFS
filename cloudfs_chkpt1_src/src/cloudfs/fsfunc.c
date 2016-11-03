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

#include "cloudfs.h"
#include "fsfunc.h"

#define UNUSED __attribute__((unused))

FILE *logFile=NULL;
struct cloudfs_state* fsConfig=NULL;

static int cloudfs_error(char *error_str)
{
    int retval = -errno;

    // TODO:
    //
    // You may want to add your own logging/debugging functions for printing
    // error messages. For example:
    //
    // debug_msg("ERROR happened. %s\n", error_str, strerror(errno));
    //

    fprintf(logFile, "[error]\t CloudFS Error: %s\n", error_str);

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
    return 0;
    int res;
    if ((res=mkdir(pathname, mode))>=0) return res;
    return cloudfs_error("mkdir error");
}

int cloudfsGetAttr(const char *pathname, struct stat *tstat) {
    fprintf(logFile, "[getattr]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    int ret=stat(target, tstat);
    free(target);
    if (ret>=0) return ret;
    return cloudfs_error("getattr error");
}

int cloudfsReadDir(const char *pathname, void *buf, fuse_fill_dir_t filler, UNUSED off_t offset, UNUSED struct fuse_file_info *fi) {
    fprintf(logFile, "[readdir]\t%s\n", pathname);
    fflush(logFile);
    char* target=getSSDPosition(pathname);
    DIR* dir=opendir(target);
    free(target);

    struct dirent *ep;
    if (dir) {
        while ((ep=readdir(dir))) filler(buf, ep->d_name, NULL, 0);
    } else {
        closedir(dir);
        return cloudfs_error("readdir error");
    }
    closedir(dir);

    return 0;
}
