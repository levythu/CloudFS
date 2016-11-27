#ifndef SNAPSHOT_H
#define SNAPSHOT_H

extern void initSnapshot();
extern bool pushSnapshot();
extern void pullSnapshot();

extern long createSnapshot();
extern int listSnapshot(long* retSpace);
extern int installSnapshot(long timestamp);
extern int uninstallSnapshot(long timestamp);
extern int removeSnapshot(long timestamp);

#endif
