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
#include "cloudapi.h"
#include "cloudfs.h"
#include "dedup.h"
#include "hashtable.h"
#include "fsfunc.h"
#include "chunks.h"


#define UNUSED __attribute__((unused))

static struct cloudfs_state state_;

static int UNUSED cloudfs_error(char *error_str)
{
    int retval = -errno;

    fprintf(stderr, "CloudFS Error: %s\n", error_str);

    /* FUSE always returns -errno to caller (yes, it is negative errno!) */
    return retval;
}
/*
 * Initializes the FUSE file system (cloudfs) by checking if the mount points
 * are valid, and if all is well, it mounts the file system ready for usage.
 *
 */
void *cloudfs_init(struct fuse_conn_info *conn UNUSED)
{
    cloud_init(state_.hostname);
    cloud_create_bucket(CONTAINER_NAME);
    fprintf(logFile, "Created container\n");
    fflush(logFile);


    rp=rabin_init(fsConfig->rabin_window_size, fsConfig->avg_seg_size, fsConfig->min_seg_size, fsConfig->max_seg_size);
    initChunkTable();

    return NULL;
}

void cloudfs_destroy(void *data UNUSED) {
  cloud_destroy();
  fclose(logFile);
}

/*
 * Functions supported by cloudfs
 */
static
struct fuse_operations cloudfs_operations = {
    .init           = cloudfs_init,
    //
    //
    // This is where you add the VFS functions that your implementation of
    // MelangsFS will support, i.e. replace 'NULL' with 'melange_operation'
    // --- melange_getattr() and melange_init() show you what to do ...
    //
    // Different operations take different types of parameters. This list can
    // be found at the following URL:
    // --- http://fuse.sourceforge.net/doxygen/structfuse__operations.html
    //
    //
    .getattr        = cloudfsGetAttr,
    .mkdir          = cloudfsMkdir,
    .readdir        = cloudfsReadDir,
    .rmdir          = cloudfsRmDir,
    .truncate       = cloudfsTruncate,
    .statfs         = cloudfsStatfs,
    .mknod          = cloudfsMknod,
    .utimens        = cloudfsUTimens,
    .unlink         = cloudfsUnlink,
    .open           = cloudfsOpen,
    .release        = cloudfsRelease,
    .fsync          = cloudfsFsync,
    .read           = cloudfsRead,
    .write          = cloudfsWrite,
    .chmod          = cloudfsChmod,
    .getxattr       = cloudfsGetXAttr,
    .setxattr       = cloudfsSetXAttr,
    .destroy        = cloudfs_destroy,
    .access         = cloudfsAccess
};

int cloudfs_start(struct cloudfs_state *state,
                  const char* fuse_runtime_name) {

  int argc = 0;
  char* argv[10];
  argv[argc] = (char *) malloc(128 * sizeof(char));
  strcpy(argv[argc++], fuse_runtime_name);
  argv[argc] = (char *) malloc(1024 * sizeof(char));
  strcpy(argv[argc++], state->fuse_path);
  argv[argc++] = "-s"; // set the fuse mode to single thread
  //argv[argc++] = "-f"; // run fuse in foreground

  state_  = *state;
  fsConfig=&state_;
  openfileTable=NewHashTable();

  //logFile=fopen("/tmp/cloudfs.log", "w");
  logFile=fopen("/dev/null", "w+");

  int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);

  return fuse_stat;
}
