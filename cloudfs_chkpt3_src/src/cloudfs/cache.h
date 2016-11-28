#ifndef CACHE_H

#define CACHE_H

typedef struct _cacheBlock {
    char id[2*MD5_DIGEST_LENGTH+5];
    int inCloud;

    struct _cacheBlock* prev;
    struct _cacheBlock* next;
} CacheBlock;

extern void initCache();
extern void pullCache();
extern int pushCache();
extern void syncCache();

extern void* c__getChunkRaw(const char *chunkname, long *len);
extern bool c__putChunkRaw(const char *chunkname, long len, char *content);
extern int c__deleteChunkRaw(const char *chunkname);


#endif
