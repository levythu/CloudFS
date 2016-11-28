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
#include "cache.h"

CacheBlock cacheLinkHead;
long totalCacheSize;

// must happend after initChunk;
void initCache() {
    char* target=getSSDPosition("/.cache/");
    mkdir(target, 0666);
    free(target);
    cacheLinkHead.prev=cacheLinkHead.next=&cacheLinkHead;
    cacheLinkHead.id[0]=0;
    totalCacheSize=0;
    pullCache();
    pushCache();
}
int countLink() {
    int ret=0;
    CacheBlock* p=cacheLinkHead.next;
    while (p!=&cacheLinkHead) {
        ret++;
        p=p->next;
    }
    return ret;
}
void pullCache() {
    char* targetPath=getSSDPosition("/.cachemeta");
    FILE* f=fopen(targetPath, "r");
    free(targetPath);
    if (!f) {
        fprintf(logFile, "[pullCache]\tNo cache meta find. Assume that cache is empty.\n");
        fflush(logFile);
    } else {
        int N;
        int useless=fscanf(f, "%d", &N);
        (void)useless;
        int i;
        for (i=0; i<N; i++) {
            CacheBlock* newCacheBlock=(CacheBlock*)malloc(sizeof(CacheBlock));
            useless=fscanf(f, "%d %s", &newCacheBlock->inCloud, newCacheBlock->id);
            totalCacheSize+=getChunkLen(newCacheBlock->id);
            newCacheBlock->prev=cacheLinkHead.prev;
            newCacheBlock->next=&cacheLinkHead;
            newCacheBlock->prev->next=newCacheBlock;
            newCacheBlock->next->prev=newCacheBlock;
            (void)useless;
        }
        fclose(f);
    }
}
int pushCache() {
    char* targetPath=getSSDPosition("/.cachemeta");
    FILE* f=fopen(targetPath, "w");
    free(targetPath);
    if (!f) return -1;
    CacheBlock* p=cacheLinkHead.next;
    fprintf(f, "%d\n", countLink());
    while (p!=&cacheLinkHead) {
        fprintf(f, "%d %s\n", p->inCloud, p->id);
        p=p->next;
    }
    fclose(f);
    return 0;
}

CacheBlock* findCacheBlock(const char* id) {
    CacheBlock* p=cacheLinkHead.next;
    while (p!=&cacheLinkHead) {
        if (strcmp(p->id, id)==0) return p;
        p=p->next;
    }
    return NULL;
}

void accessCacheBlock(CacheBlock* cb) {
    cb->next->prev=cb->prev;
    cb->prev->next=cb->next;
    cb->next=cacheLinkHead.next;
    cb->prev=&cacheLinkHead;
    cb->next->prev=cb;
    cb->prev->next=cb;
}

char* getLocalCacheName(char* buf, const char* id) {
    char* cacheFolder=getSSDPosition("/.cache/");
    sprintf(buf, "%s%s", cacheFolder, id);
    free(cacheFolder);
    return buf;
}

char* loadinCachedChunk(const char* id) {
    char cacheFile[MAX_PATH_LEN];
    getLocalCacheName(cacheFile, id);

    long size=getChunkLen(id);

    FILE* infile=fopen(cacheFile, "rb");
    if (infile == NULL) {
        return NULL;
    }
    char* buffer=(char*)malloc(sizeof(char)*size);
    long p=0;
    while (p<size) {
        long t=fread(buffer+p, 1, size-p, infile);
        p+=t;
        if (t==0) break;
    }
    fclose(infile);

    return buffer;
}

void saveCachedChunk(const char* id, long size, const char* content) {
    char cacheFile[MAX_PATH_LEN];
    getLocalCacheName(cacheFile, id);

    FILE* outfile=fopen(cacheFile, "wb");
    if (outfile == NULL) {
        // error
        return;
    }
    long p=0;
    while (p<size) {
        long t=fwrite(content+p, 1, size-p, outfile);
        p+=t;
        if (t==0) break;
    }
    fclose(outfile);
}

bool evictOneCacheBlock() {
    CacheBlock* victim=cacheLinkHead.prev;
    if (victim==&cacheLinkHead) {
        // nothing to evict;
        return false;
    }
    long clen=getChunkLen(victim->id);

    if (!victim->inCloud) {
        // dirty, write it back!
        char* res=loadinCachedChunk(victim->id);
        nc__putChunkRaw(victim->id, clen, res);
        victim->inCloud=true;
        free(res);
    }

    char cacheFile[MAX_PATH_LEN];
    unlink(getLocalCacheName(cacheFile, victim->id));

    victim->next->prev=victim->prev;
    victim->prev->next=victim->next;
    free(victim);
    totalCacheSize-=clen;

    return true;
}

void* c__getChunkRaw(const char *chunkname, long *len) {
    CacheBlock* targetCB=findCacheBlock(chunkname);
    *len=getChunkLen(chunkname);
    if (targetCB) {
        accessCacheBlock(targetCB);
        return loadinCachedChunk(chunkname);
    }
    // need to fetch from cloud
    if (*len>fsConfig->cache_size) {
        // the chunk is too big to fit in the cache. Fetch it directly
        return nc__getChunkRaw(chunkname, len);
    }
    while (totalCacheSize+*len>fsConfig->cache_size) {
        evictOneCacheBlock();
    }
    char* content=nc__getChunkRaw(chunkname, len);
    saveCachedChunk(chunkname, *len, content);

    CacheBlock* cb=(CacheBlock*)malloc(sizeof(CacheBlock));
    sprintf(cb->id, "%s", chunkname);
    cb->inCloud=true;
    cb->next=cacheLinkHead.next;
    cb->prev=&cacheLinkHead;
    cb->next->prev=cb;
    cb->prev->next=cb;
    totalCacheSize+=*len;

    pushChunkTable();

    return content;
}

bool c__putChunkRaw(const char *chunkname, long len, char *content) {
    if (len>fsConfig->cache_size) {
        // the chunk is too big to fit in the cache. Fetch it directly
        return nc__putChunkRaw(chunkname, len, content);
    }
    while (totalCacheSize+len>fsConfig->cache_size) {
        evictOneCacheBlock();
    }
    saveCachedChunk(chunkname, len, content);

    CacheBlock* cb=(CacheBlock*)malloc(sizeof(CacheBlock));
    sprintf(cb->id, "%s", chunkname);
    cb->inCloud=false;
    cb->next=cacheLinkHead.next;
    cb->prev=&cacheLinkHead;
    cb->next->prev=cb;
    cb->prev->next=cb;
    totalCacheSize+=len;

    pushChunkTable();
    return true;
}

int c__deleteChunkRaw(const char *chunkname) {
    CacheBlock* targetCB=findCacheBlock(chunkname);
    if (!targetCB || targetCB->inCloud) {
        if (nc__deleteChunkRaw(chunkname)<0) return -1;
    }
    if (targetCB) {
        long len=getChunkLen(chunkname);
        char cacheFile[MAX_PATH_LEN];
        unlink(getLocalCacheName(cacheFile, chunkname));

        targetCB->next->prev=targetCB->prev;
        targetCB->prev->next=targetCB->next;
        free(targetCB);
        totalCacheSize-=len;

        pushChunkTable();
    }
    return 0;
}

void syncCache() {
    CacheBlock* p=cacheLinkHead.next;
    while (p!=&cacheLinkHead) {
        if (!p->inCloud) {
            long clen=getChunkLen(p->id);
            char* res=loadinCachedChunk(p->id);
            nc__putChunkRaw(p->id, clen, res);
            p->inCloud=true;
            free(res);
        }
        p=p->next;
    }
    pushChunkTable();
    return;
}
