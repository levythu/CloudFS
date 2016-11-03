#ifndef FSFUNC_H
#define FSFUNC_H

extern int cloudfsMkdir(const char *pathname, mode_t mode);
extern int cloudfsGetAttr(const char *pathname, struct stat *stat);
extern int cloudfsReadDir(const char *pathname, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
extern int cloudfsRmDir(const char *pathname);
extern int cloudfsTruncate(const char *pathname, off_t length);
extern int cloudfsStatfs(const char *pathname, struct statvfs *buf);
extern int cloudfsUnlink(const char *pathname);

extern FILE *logFile;
extern struct cloudfs_state* fsConfig;

#endif
