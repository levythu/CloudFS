#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "hashtable.h"

#define BIG_PRIME 65447

Hashtable NewHashTable() {
    Hashtable ret;
    ret.table=(hashNode**)calloc(BIG_PRIME, sizeof(hashNode*));
    return ret;
}

void HRelease(Hashtable obj) {
    for (int i=0; i<BIG_PRIME; i++) {
        hashNode* p=obj.table[i];
        while (p) {
            hashNode* q=p->next;
            free(p);
            p=q;
        }
    }
    free(obj.table);
}

hashNode* searchInList(hashNode* listHead, const char* key) {
    while (listHead) {
        if (strcmp(listHead->k, key)==0) return listHead;
        listHead=listHead->next;
    }
    return NULL;
}

bool HPutIfAbsent(Hashtable obj, const char* key, void* v) {
    int bucket=HCalc(key)%BIG_PRIME;
    if (searchInList(obj.table[bucket], key)==NULL) {
        hashNode* nt=(hashNode*)malloc(sizeof(hashNode));
        nt->k=key;
        nt->v=v;
        nt->next=obj.table[bucket];
        obj.table[bucket]=nt;
        return true;
    }
    return false;
}

void* HGet(Hashtable obj, const char* key) {
    int bucket=HCalc(key)%BIG_PRIME;
    hashNode* target=searchInList(obj.table[bucket];
    return target?target->v:NULL;
}

void* HRemove(Hashtable obj, const char* key) {
    int bucket=HCalc(key)%BIG_PRIME;
    hashNode* p=obj.table[bucket];
    hashNode** pp=&obj.table[bucket];
    while (p) {
        if (strcmp(listHead->k, key)==0) {
            *pp=p->next;
            void* t=p->v;
            free(p);
            return t;
        }
        pp=&p->next;
        p=p->next;
    }
    return NULL;
}

int HCalc(const char* key) {
    int result=0;
    while (key) {
        result=(result*256+*key)%BIG_PRIME;
        key++;
    }

    return result;
}
