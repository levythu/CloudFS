#ifndef FSFUNC_H
#define FSFUNC_H

extern int cloudfsMkdir(const char *pathname, mode_t mode);
extern int cloudfsGetAttr(const char *pathname, struct stat *stat);
extern int cloudfsReadDir(const char *pathname, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
extern int cloudfsRmDir(const char *pathname);
extern int cloudfsTruncate(const char *pathname, off_t length);
extern int cloudfsStatfs(const char *pathname, struct statvfs *buf);
extern int cloudfsMknod(const char *pathname, mode_t mode, dev_t dev);
extern int cloudfsUTimens(const char *pathname, const struct timespec tv[2]);
extern int cloudfsUnlink(const char *pathname);
extern int cloudfsOpen(const char *pathname, struct fuse_file_info *fi);
extern int cloudfsRelease(const char *pathname, struct fuse_file_info *fi);
extern int cloudfsFsync(const char* pathname, int isdatasync, struct fuse_file_info* fi);
extern int cloudfsRead(const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
extern int cloudfsWrite(const char* path, const char *buf, size_t size, off_t offset, struct fuse_file_info* fi);
extern int cloudfsChmod(const char * pathname, mode_t mode);
extern int cloudfsGetXAttr(const char *path, const char *name, char *value, size_t size);
extern int cloudfsSetXAttr(const char *path, const char *name, const char *value, size_t size, int flags);
extern int cloudfsAccess(const char *pathname, int mask);
extern int cloudfsIOctl(const char *pathname, int cmd, void *arg, struct fuse_file_info *fi, unsigned int flags, void *data);
extern void cloudfsInitPlaceholder();

extern FILE *logFile;
extern struct cloudfs_state* fsConfig;
extern Hashtable openfileTable;
extern rabinpoly_t *rp;

char* getSSDPosition(const char *pathname);
char* getSSDPositionSlash(const char *pathname);

#define CONTAINER_NAME "test1"
#define CHUNK_DIRECTORY_NAME ".chunkdir"

#endif
