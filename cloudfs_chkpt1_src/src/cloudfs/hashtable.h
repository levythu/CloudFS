#ifndef HASHTABLE_H

#define HASHTABLE_H

typedef struct _hashNode {
    void* v;
    const char* k;
    struct _hashNode *next;
} hashNode;

typedef struct _hashtable {
    hashNode** table;
} Hashtable;

extern int HCalc(const char* key);
extern void* HRemove(Hashtable obj, const char* key);
extern void* HGet(Hashtable obj, const char* key);
extern bool HPutIfAbsent(Hashtable obj, const char* key, void* v);
extern void HRelease(Hashtable obj);

extern Hashtable NewHashTable();

#endif
