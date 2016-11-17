#ifndef CHUNKS_H

#define CHUNKS_H

extern void initChunkTable();
extern bool pushChunkTable();
extern void pullChunkTable();

// Both inc and dec function will not push chunk table;

extern void incChunkReference(const char* chunkname, long len, char* content);

// [return] Whether a deletion has happened. Exception on non-exist chunck
extern bool decChunkReference(const char* chunkname);

// get the raw data of a trunk and return its length in len
// if error, return NULL and len will not be modified.
// invocator should free the space returned.
// never care internal implementation. (Now it pulls it from the cloud everytime,
// in the future it may cache the content.)
extern void* getChunkRaw(const char *chunkname, long *len);
extern long getChunkLen(const char *chunkname);
extern bool putChunkRaw(const char *chunkname, long len, char *content);

#endif
