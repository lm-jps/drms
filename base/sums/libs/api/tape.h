/* tape.h
 *
*/

#ifndef TAPE_INCL
#define TAPE_INCL      1

#include <SUM.h>

#ifdef SUMDC
#define MAX_SLOTS 30
#define NUM_IMP_EXP_SLOTS 5  /* assumes these slots are the highest slot#s */
#define MAX_SLOTS_LIVE (MAX_SLOTS - NUM_IMP_EXP_SLOTS)
#define MAX_DRIVES 1
#define MAX_GROUPS 1024
#define MAX_AVAIL_BLOCKS 800000000      /* max (512) blocks on a t50 tape */
#define TAR_FILE_SZ 500000000   /* try to make a tar file this many bytes */
#define TAR_FILE_SZ_BLKS 976563 /* TAR_FILE_SZ/512 */
#define LIBDEV "/dev/t50"
/* If /dev/ name in this file then override the LIBDEV above */
#define LIBDEVFILE "/home/jim/cvs/JSOC/base/sums/apps/data/libdevfile_50.cfg"
#define SUMDR0 "/dev/nst0"
#define SUMDR  "/dev/nst"
#define GTARBLOCK 256   /* the gtar default is this */
#define UNLOADDUMP "/tmp/t50_unload_dump.out"
#define UNLOADCMD "/usr/sbin/mtx -f /dev/t50 unload"
#define POSITIONDUMP "/tmp/t50_position_dump.out"
#else
#define MAX_SLOTS 128
#define NUM_IMP_EXP_SLOTS 8  /* assumes these slots are the highest slot#s */
#define MAX_SLOTS_LIVE (MAX_SLOTS - NUM_IMP_EXP_SLOTS)
#define MAX_DRIVES 4
#define MAX_GROUPS 1024
#define MAX_AVAIL_BLOCKS 1000000000	/* max (512) blocks on a t120 tape */
#define TAR_FILE_SZ 500000000   /* try to make a tar file this many bytes */
#define TAR_FILE_SZ_BLKS 976563 /* TAR_FILE_SZ/512 */
#define LIBDEV "/dev/sg12"
/* If /dev/ name in this file then override the LIBDEV above */
#define LIBDEVFILE "/home/production/cvs/JSOC/base/sums/apps/data/libdevfile.cfg"
#define SUMDR0 "/dev/nst0"
#define SUMDR1 "/dev/nst1"
#define SUMDR  "/dev/nst"
#define GTARBLOCK 256	/* the gtar default is this */
#define UNLOADDUMP "/tmp/t120_unload_dump.out"
#define UNLOADCMD "/usr/sbin/mtx -f /dev/sg12 unload"
#define POSITIONDUMP "/tmp/t120_position_dump.out"
#endif

/* tapemode values in DRIVE struct */
#define TAPE_NOT_LOADED 0 /* no tape in drive */
#define TAPE_RD_INIT 1	/* tape just loaded for a read */
#define TAPE_RD_CONT 2	/* subsequent tape read. filenum now valid */

/* closed values in TAPE struct for writting status */
/* NOTE: if change here check use in libSUM.d sql */
#define TAPEUNINIT -1	/* tape uninitialized (no file 0 label) */
#define TAPEACTIVE 1	/* tape is open for writting */
#define TAPECLOSED 2	/* tape closed. no further writting */
#define TAPECLOSEDREJECT 3 /* special status sent to tapearc when next q */
			   /* entry is for an already closed tape */

typedef struct {
  int slotnum;		/* slot number from 1 to MAX_SLOTS */
  char *tapeid;		/* tape id in this slot */
} SLOT;

typedef struct {
  uint64_t sumid;	/* identifies who is using the drive */
  int busy;
  int tapemode;		/* TAPE_NOT_LOADED, TAPE_RD_INIT, TAPE_RD_CONT */
  int slotnum;		/* slot# where the cartridge in the drive belongs */
  int filenum;		/* file # starting at 0 (rewound) */
  int blocknum;		/* tape block# from mt tell command */
  int offline;		/* =1 when drive set offline by driveonoff utility */
  char *tapeid;		/* tape id in this drive */
} DRIVE;

typedef struct {
  /* NOTE: file numbers start at 0 (tape rewound or just loaded) */
  /* File # 0 is always the tape label */
  char *tapeid;         /* tape id of this tape */
  int nxtwrtfn;		/* next file# to write */
  int spare;		/* reserved for future use */
  int group_id;		/* tape belongs to this group# */
  uint64_t availblocks;	/* avail 512 byte blocks left on tape */
  int closed;		/* -1=uninitialized, 1=active, 2=tape closed */
  char *last_access;	/* date of last write(?) to tape */
} TAPE;

#endif	


