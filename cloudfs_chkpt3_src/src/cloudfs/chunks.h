/*
** Functions to manage chunks on the cloud.
*/

#ifndef CHUNKS_H

#define CHUNKS_H

// initChunkTable will try to init the table from chunkDir file. If the file is
// broken, initiate a blank one.
extern void initChunkTable();
extern bool pushChunkTable();
extern void pullChunkTable();
extern void rebaseChunkTable();

// Both inc and dec function will not push chunk table; The invoker can manage a
// batch submit in order to reduce disk IO overheads

extern void incChunkReference(const char* chunkname, long len, char* content);
extern void incALLReference();

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
extern int deleteChunkRaw(const char *chunkname);

// invoked by cache
extern void* nc__getChunkRaw(const char *chunkname, long *len);
extern bool nc__putChunkRaw(const char *chunkname, long len, char *content);
extern int nc__deleteChunkRaw(const char *chunkname);

#endif
