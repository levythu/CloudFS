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
extern int cloudfsWrite(const char* path, char *buf, size_t size, off_t offset, struct fuse_file_info* fi);

extern FILE *logFile;
extern struct cloudfs_state* fsConfig;
extern Hashtable openfileTable;

#define CONTAINER_NAME "test1"

#endif
