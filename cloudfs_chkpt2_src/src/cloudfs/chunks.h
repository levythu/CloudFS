#ifndef CHUNKS_H

#define CHUNKS_H

extern void initChunkTable();
extern bool pushChunkTable();
extern void pullChunkTable();

// Both inc and dec function will not push chunk table;

extern void incChunkReference(const char* chunkname);

// [return] Whether a deletion has happened. Exception on non-exist chunck
extern bool decChunkReference(const char* chunkname);

#endif
