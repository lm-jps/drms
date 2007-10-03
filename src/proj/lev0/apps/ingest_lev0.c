/*-----------------------------------------------------------------------------
 * cvs/jsoc/src/lev0/ingest_lev0.c
 * NOTE: This originally came from hmi_lev0.c on hmi0
 *-----------------------------------------------------------------------------
 *
 * This is a module that runs DRMS and continuously extracts images and HK
 * data from .tlm files that appear in the given input dir. It outputs images
 * to the DRMS dataset hmi.lev0 and hk data to hmi.!!!!TBD!!!.

 * DESCRIPTION:

*!!!!TBD UPDATE THIS DESCRIP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

 * The ingest_lev0 program is started by the DDS_SOC program when it is first
 * run. This program has the SUMS API interface to get and put SUM storage. 
 * The ingest_tlm is given the input dir to monitor as an argument when
 * it is invoked. This dir is normally the $DIRSOC2SOC directory that the 
 * DDS files are moved into after they are validated. Ingest_tlm is also
 * given the dir to copy files to after it is done ingesting them into SUMS.
 * This is normally the $DIRSOC2PIPE directory that the files are staged to
 * for pickup by the pipeline processing backend system.
 * When ingest_tlm sees a 
 * new .qac file, it will confirm the .tlm file and allocate disk storage via 
 * SUM_Allocate() and copy the current files into this storage and do a 
 * SUM_Put() to make the storage archivable by SUMS. When the files have been 
 * ingested into SUMS the program then moves them from $DIRSOC2SOC to 
 * $DIRSOC2PIPE for the backend pipeline system to begin processing.
 * 
 * Ingest_tlm is also passed the pipeline to soc dir that the pipeline backend
 * system writes .parc file to, to indicate which storage units have been 
 * archived in the backend. We will read these files and update the sum_main
 * table for the given storage unit with the safe_tape info provided.
 *
 * Called:
 *ingest_tlm [-v] -pVC02 /dir_to_ingest /dir_to_pipeline /dir_from_pipeline [log]
 *
 */

#include <jsoc_main.h>
#include <cmdparams.h>
#include <drms.h>
#include <drms_names.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <signal.h>
#include <strings.h>
#include <sum_rpc.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h> /* for umask(2) */
#include <dirent.h>
#include <unistd.h> /* for alarm(2) among other things... */
#include <printk.h>
#include "imgstruct.h"
/************************************
#include "hmi_compression.h"
#include "decompress.h"
#include "load_hk_config_files.h"
*************************************/

#define H0LOGFILE "/usr/local/logs/lev0/ingest_lev0.%s.%s.log"
#define DIRDDS "/egse/ssim2soc"
#define IMAGEDIR "/tmp/jim"	/* dir to put the last IMAGEDIRCNT images */
#define IMAGEDIRCNT 20		/* # of images to save */
#define SEC1970TO2004 1072828800 /* approx #of secs from 1970 to 2004 */
#define PKTSZ 1788		/* size of VCDU pkt */
#define DEFAULTDB "jsoc"	/* the default db to connect to */
#define MAXFILES 512		/* max # of file can handle in tlmdir */
#define NUMTIMERS 10		/* number of seperate timers avail */
#define TESTAPPID 0x199		/* appid of test pattern packet */
#define TESTVALUE 0xc0b		/* first value in test pattern packet */

#define NOTSPECIFIED "***NOTSPECIFIED***"
/* List of default parameter values. */
ModuleArgs_t module_args[] = { 
  {ARG_STRING, "vc", NOTSPECIFIED, "Primary virt channel to listen to"},
  {ARG_STRING, "indir", NOTSPECIFIED, "directory containing the files to ingest"},
  {ARG_STRING, "outdir", NOTSPECIFIED, "directory to move the files to after the ingest"}, 
  {ARG_STRING, "logfile", NOTSPECIFIED, "optional log file name. Will create one if not given"},
  {ARG_FLAG, "v", "0", "verbose flag"},
  {ARG_FLAG, "h", "0", "help flag"},
  {ARG_END}
};

CmdParams_t cmdparams;

/* Module name presented to DRMS. */
char *module_name = "ingest_lev0";


FILE *h0logfp;                  /* fp for h0 ouput log for this run */
IMG Image;

static char datestr[32];
static struct timeval first[NUMTIMERS], second[NUMTIMERS];
static float tsum[NUMTIMERS];

/* Declarations for static functions */
static void open_sum(void);
/* static double du_dir(void);*/
static time_t call_time(void);
static void now_do_alrm_sig();

extern int numcontexts;

unsigned int fsn = 0;
unsigned int fsn_prev = 0;
unsigned int fid = 0;
SUM_t *sum;
SUMID_t uid = 0;
char **cptr;
uint64_t *dsixpt;
uint64_t alloc_index;
char alloc_wd[64];

long long vcdu_seq_num;
long long vcdu_seq_num_next;
long long total_missing_im_pdu;
unsigned int vcdu_24_cnt, vcdu_24_cnt_next;
int verbose;			/* set by get_cmd() */
double reqbytes;		/* # of bytes requested */
double dsize;			/* # of bytes used */
double bytes_used;		/* total #of bytes for all cataloged output */
int total_tlm_vcdu;
int total_missing_vcdu;
int dsds_tid;			/* originally the tid of dsds_svc */
				/* now the tid of pe_rpc that gets us to dsds*/
int abort_active;		/* set while doing an abort */
int sigalrmflg = 0;		/* set on signal so prog will know */
int sigtermflg = 0;		/* set on signal so prog will know */
int tlmactive = 0;              /* set when tlm lev0 processing is active */
int pflg = 0;
int imagedircnt = 0;            /* inc each time write an image to IMAGEDIR */
int ALRMSEC = 60;               /* seconds for alarm signal */
int dbxflg;			/* user defined while running dbx, else N/A */
int debugflg;			/* run all pvm servers in the debug mode */
				/* also do keyiterate. Don't use w/tae */ 
char database[MAX_STR];
char pdshost[MAX_STR];
char pchan[8];			/* primary channel to listen to e.g. VC02 */
char rchan[8];			/* redundant channel to listen to e.g. VC10 */
char *dbname = DEFAULTDB;	/* !!TBD pass this in as an arg */
char *username;			/* from getenv("USER") */
char *indir;			/* tlm dir name passed in */
char *outdir;			/* output dir for .tlm file (can be /dev/null)*/
char *logfile;			/* optional log name passed in */
char *vc;			/* virtual channel to process, e.g. VC02 */
struct p_r_chans {
  char *pchan;
  char *rchan;
};
typedef struct p_r_chans P_R_CHANS;

P_R_CHANS p_r_chan_pairs[] = {
{"VC01", "VC09"},		/* AIA */
{"VC04", "VC12"},		/* AIA */
{"VC02", "VC10"},		/* HMI */
{"VC05", "VC13"},		/* HMI */
{"n/a", "n/a"}
};

struct namesort {		/* sorted file names in tlmdir */
  char *name;
};
typedef struct namesort NAMESORT;

/* linked list of open images */
struct openimg {
  struct openimg *next;
  time_t sec;
  unsigned int fsn;
};
typedef struct openimg OPENIMG;
OPENIMG *openimg_hdr = NULL;	/* linked list of open images */
OPENIMG *openimg_ptr;		/* current enty */

int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\ningest_lev0 [-vh] "
	"vc=<virt chan> indir=</dir> outdir=</dir> [logfile=<file>]\n"
	"  -h: help - show this message then exit\n"
	"  -v: verbose\n"
	"vc= primary virt channel to listen to e.g. VC02\n"
	"indir= directory containing the files to ingest\n"
	"outdir= directory to move the files to after the ingest\n"
	"logfile= optional log file name. Will create one if not given\n");
    return(1);
    }
  return (0);
  }

/* Returns a time tag like  yyyy.mm.dd.hhmmss */
char *gettimetag()
{
  char timetag[32];
  struct timeval tvalr;
  struct tm *t_ptr;

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr);
  sprintf(timetag, "%04d.%02d.%02d.%02d%02d%02d",
        (t_ptr->tm_year+1900), (t_ptr->tm_mon+1), t_ptr->tm_mday, t_ptr->tm_hour, t_ptr->tm_min, t_ptr->tm_sec);
  return(timetag);
}


/* Module main function. */
int DoIt(void)
{
  char logname[128];

  if (nice_intro())
    return (0);
  vc = cmdparams_get_str(&cmdparams, "vc", NULL);
  indir = cmdparams_get_str(&cmdparams, "indir", NULL);
  outdir = cmdparams_get_str(&cmdparams, "outdir", NULL);
  logfile = cmdparams_get_str(&cmdparams, "logfile", NULL);
  if (strcmp(vc, NOTSPECIFIED) == 0) {
    fprintf(stderr, "'vc' virt channel must be specified.  Abort\n");
    return(1);
  }
  if (strcmp(indir, NOTSPECIFIED) == 0) {
    fprintf(stderr, "'indir' must be specified.  Abort\n");
    return(1);
  }
  if (strcmp(outdir, NOTSPECIFIED) == 0) {
    fprintf(stderr, "'outdir' must be specified.  Abort\n");
    return(1);
  }
  if (strcmp(logfile, NOTSPECIFIED) == 0) {
    sprintf(logname, H0LOGFILE, username, gettimetag());
  }
  else {
    sprintf(logname, "%s", logfile);
  }
  if((h0logfp=fopen(logname, "w")) == NULL)
    fprintf(stderr, "**Can't open the log file %s\n", logname);

}


/* NOTE: This my be vestigial. Now we can only have one open image per process. !!CHECK */
/* Add an entry with the given values to the OPENIMG linked list */
void setopenimg(OPENIMG **list, time_t sec, unsigned int fsn)
{
  OPENIMG *newone;

  newone = (OPENIMG *)malloc(sizeof(OPENIMG));
  newone->next = *list;
  newone->sec = sec;
  newone->fsn = fsn;
  *list = newone;
}

/* remove the OPENIMG list entry with the given fsn */
void remopenimg(OPENIMG **list, unsigned int fsn)
{
  OPENIMG *walk = *list;
  OPENIMG *trail = NULL;

  while(walk) {
    if(walk->fsn != fsn) {
      trail = walk;
      walk = walk->next;
    }
    else {
      if(trail)
        trail->next = walk->next;
      else
        *list = walk->next;
      free(walk);
      walk = NULL;
    }
  }
}

/* get the OPENIMG list entry with the given fsn. return null if none. */
OPENIMG *getopenimg(OPENIMG *list, unsigned int fsn)
{
  OPENIMG *walk = list;

  while(walk) {
    if(walk->fsn != fsn)
      walk = walk->next;
    else
      return walk;
  }
  return walk;
}


void temp_drms_test_stuff() 
{
  DRMS_Record_t *rs;
  DRMS_Segment_t *segment;
  DRMS_Array_t *segArray;
  char seriesname[80];
  int status;
  unsigned short *rdat = Image.data;

  sprintf(seriesname, "su_jim.lev0");
  rs = drms_create_record(drms_env, seriesname, DRMS_PERMANENT, &status);
  if(status) {
    printk("Can't create record for %s\n", seriesname);
  }
  segment = drms_segment_lookup(rs, "image");
  status = drms_setkey_int(rs, "FrameSequenceNumber", 1);
  segArray = drms_array_create(DRMS_TYPE_SHORT,
                                             segment->info->naxis, 
                                             segment->axis, 
                                             rdat,
                                             &status);
  status = drms_segment_write(segment, segArray, 0);
}

void StartTimer(int n)
{
  gettimeofday (&first[n], NULL);
}

float StopTimer(int n)
{
  gettimeofday (&second[n], NULL);
  if (first[n].tv_usec > second[n].tv_usec) {
    second[n].tv_usec += 1000000;
    second[n].tv_sec--;
  }
  return (float) (second[n].tv_sec-first[n].tv_sec) +
    (float) (second[n].tv_usec-first[n].tv_usec)/1000000.0;
}

/* Outputs the variable format message (re: printf) to the log file.
 */
int h0log(const char *fmt, ...)
{
  va_list args;
  char string[32768];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  if(h0logfp) {
    fprintf(h0logfp, string);
    fflush(h0logfp);
  } else {			/* couldn't open log */
    printf(string);
    fflush(stdout);
  }
  va_end(args);
  return(0);
}

/* Got a fatal error sometime after registering with SUMS. 
 * Degregister and close with SUMS as approriate.
 */
void abortit(int stat)
{
  printk("***Abort in progress ...\n");
  if(uid ) {			/* we've registered with SUMS */
    SUM_close(sum, h0log);
  }
  printk("**Exit ingest_tlm w/ status = %d\n", stat);
  /*msg("Exit ingest_tlm w/ status = %d\n\n", stat);*/
  if (h0logfp) fclose(h0logfp);
  exit(stat);
}

/* Called every 60 sec to check if timeout on opened images.
 * Sets flag only as the libhmicomp lib is not reentrant.
 * The main program will check for the flag when it is safe to do the 
 * processing and call now_do_alrm_sig() if set.
 */
void alrm_sig(int sig)
{
  signal(SIGALRM, alrm_sig);
  if(!tlmactive)
    now_do_alrm_sig();
  else
    sigalrmflg = 1;
  return;
}

void now_do_alrm_sig()
{
  sigalrmflg = 0;
  alarm(ALRMSEC);
  return;
}

void sighandler(sig)
     int sig;
{
  sigtermflg = 1;
  return;
}

void now_do_term_sig()
{
  abortit(2);
}


time_t call_time() 
{
  time_t tsec;

  tsec = time(NULL);
  return(tsec - (time_t)SEC1970TO2004); /* make the epoch 2004 */
}

/* Open with the sum_svc.
 * Sets the global handle, sum, to the value returned by SUM_open().
 */
void open_sum()
{
  if((sum = SUM_open(NULL, NULL, h0log)) == 0) {
    printk("***Failed on SUM_open()\n");
    abortit(3);
  }
  uid=sum->uid;			/* uid assigned to this open */
  printk("*Opened with sum_svc as uid=%ld\n", uid); 
}

int compare_names(const void *a, const void *b)
{
  NAMESORT *x=(NAMESORT *)a, *y=(NAMESORT *)b;
  return(strcmp(x->name, y->name));
}

unsigned short MDI_getshort (unsigned char *c)    /*  machine independent  */
{
  unsigned short s = 0;

  s = (unsigned short) *c++ << 8;
  s |= (unsigned short) *c;
  return s;
}

/* Process the tlm file to validate and optionally to extract the 
 * lev0 image.
*/
int get_tlm(char *file)
{
  FILE *fpin;
/***************************
  Image_t *images, *sav_images, *im, *partimg;
  CCSDS_Packet_t *hk_packets;
*******************************/
  unsigned char cbuf[PKTSZ];
  char errtxt[128], imgfile[128], cmd[128];
  long long gap_42_cnt;
  int status, rstatus, fpkt_cnt, i, j, sync_bad_cnt, nx;
  int imagecnt, appid, datval, eflg, first;
  unsigned int cnt1, cnt2, cnt3, fsnx, fidx, gap_24_cnt;
  int zero_pn;
  unsigned short pksync1, pksync2;
  float ftmp;

  StartTimer(1);			/* time tlm file processing */
  if(!(fpin = fopen(file, "r"))) {	/* open the tlm input */
    h0log("*Can't open tlm file %s\n", file);
    return(1);
  }
  h0log("*Processing tlm file %s\n", file);
  fpkt_cnt = sync_bad_cnt = imagecnt = eflg = 0;
  zero_pn = gap_24_cnt = gap_42_cnt = 0;
  first = 1; 
  /*StartTimer(2);			/* time each image */
  /* read a VCDU packet */
  while((status = fread(cbuf,sizeof(char),PKTSZ,fpin) ) == PKTSZ) {
    pksync1 = MDI_getshort(cbuf);
    pksync2 = MDI_getshort(cbuf+2);
    if((pksync1 == 0) && (pksync2 == 0)) { /* skip 0 pn code */
      if(!zero_pn) {			/* give msg for 1st one only */
        h0log("*0 PN code at pkt# %d\n", fpkt_cnt);
        h0log("*Subsequent ones will be ignored until non-0 again\n");
        zero_pn = 1;
      }
      fpkt_cnt++;			/* count # of pkts found */
      continue;
    }
    if((pksync1 != 0x1acf) || (pksync2 != 0xfc1d)) {
      h0log("*Lost sync at VCDU pkt# %d. pksync1=%x pksync2=%x\n", 
		fpkt_cnt, pksync1, pksync2);
      fpkt_cnt++;			/* count # of pkts found */
      eflg++;
      if(sync_bad_cnt++ > 4) {
        h0log("**Too many out of sync packets.\n");
        return(1);
      }
      h0log("  Will attempt to press on...\n");
      zero_pn = 0;
      continue;
    }
    if(first) {			/* print first good sync found */
      h0log("*VCDU pkt# %d sync = %x %x\n", fpkt_cnt, pksync1, pksync2);
    }
    fpkt_cnt++;			/* count # of pkts found */
    /* get 24 bit VCDU counter */
    cnt1 = MDI_getshort(cbuf+6);
    cnt2 = MDI_getshort(cbuf+8);
    cnt2 = (cnt2 >> 8)& 0xFF;
    cnt2 = ((cnt1 << 8)& 0xFF00) + cnt2;
    cnt1 = (cnt1 >> 8)& 0xFF;
    vcdu_24_cnt = (cnt1*65536) + cnt2;
    if(vcdu_24_cnt_next != vcdu_24_cnt) {
      h0log("*VCDU 24bit seq num out of sequence. exp: %u  rec: %u\n", 
	    vcdu_24_cnt_next, vcdu_24_cnt);
      if(vcdu_24_cnt_next > vcdu_24_cnt) {
        h0log("*NOTE: VCDU 24 bit counter retarded\n"); /*cntr does go thru 0*/
        h0log("*NOTE: gap report will be inaccurate (tbd)\n");
      }
      if(!first) {		/* don't count gap across .tlm files */
        gap_24_cnt += vcdu_24_cnt - vcdu_24_cnt_next;
      }
    }
    vcdu_24_cnt_next = vcdu_24_cnt + 1;
    /* now get the 42bit IM_PDU counter */
    cnt1 = MDI_getshort(cbuf+10);
    cnt2 = MDI_getshort(cbuf+12);
    cnt3 = MDI_getshort(cbuf+14);
    cnt1 = cnt1 & 0x03ff;
    vcdu_seq_num = (cnt1*4294967296) + (cnt2*65536) + cnt3;
    /* h0log("vcdu_seq_num = %lld\n", vcdu_seq_num); */
    if(vcdu_seq_num_next != vcdu_seq_num) {
      h0log("*IM_PDU seq num out of sequence. exp: %lld  rec: %lld\n", 
	    vcdu_seq_num_next, vcdu_seq_num);
      if(vcdu_seq_num_next > vcdu_seq_num) {
        h0log("*NOTE: IM_PDU 42 bit counter retarded\n");
        h0log("*NOTE: gap report will be inaccurate\n");
      }
      if(!first) {		/* don't count gap across .tlm files */
        gap_42_cnt += vcdu_seq_num - vcdu_seq_num_next;
      }
      eflg++;
    }
    first = 0;
    vcdu_seq_num_next = vcdu_seq_num + 1;
    /* get the App ID. Low 11 bit of short at buf+18 */
    appid = MDI_getshort(cbuf+18);
    appid = appid & 0x07ff;
    if(appid == TESTAPPID) {	/* appid of test pattern */
      /*h0log("*Test ApID of %0x found for IM_PDU Cntr = %lld\n", 
			TESTAPPID, vcdu_seq_num);*/
      for(i=0, j=TESTVALUE; i < 877; i=i+2, j++) {
        datval = MDI_getshort(cbuf+32+i);	/* next data value */
        if(datval != j) {
          h0log("*Test data value=%0x, expected=%0x for IM_PDU Cntr=%lld\n", 
		datval, j, vcdu_seq_num);
          eflg++;
          break;		/* skip the rest of this packet */
        }
      }
      continue; 		/* go on to next packet */
    }

    /* Parse tlm packet headers. */
/** !!!TEMP noop processing for Jul 2006 test w/ DDS **************
    rstatus = decompress_next_vcdu((unsigned short *)(cbuf+10), 
			&images, &hk_packets);
*******************************************************************/
/*rstatus = SUCCESS;*/
/*goto BYPASS;*/

    /*h0log("decompress_next_vcdu() rstatus = %d\n", rstatus); /* !!TEMP */
    switch(rstatus) {
    case SUCCESS:
      /* 0: A science data VCDU was successfully decoded
      /* The coresponding image is not yet complete.
      /* On return, *image and *hk_packets are untouched. 
      */
      cnt1 = MDI_getshort(cbuf+32);
      cnt2 = MDI_getshort(cbuf+34);
      fsnx = (unsigned int)(cnt1<<16)+(unsigned int)(cnt2);
      if(fsnx != fsn_prev) {            /* the fsn has changed */
        h0log("*FSN has changed from %d to %d\n", fsn_prev, fsnx);
        /* close the image of the prev fsn if not 0 */
        if(fsn_prev != 0) {
          for(nx=0; nx < numcontexts; nx++) {
            /*fsn = ID2FSN(Context[nx]->ID);*/
            if(fsn == fsn_prev) {
               /* !!!!STUFF taken out!!!!!*/
            }
          }
        }
        fsn_prev = fsnx;
      }
      break;
   /* !!!!STUFF taken out!!!!!*/
    default:
      /* < 0: An error occured. Consult hmi_compression.h for macro
      /* definitions of negative error codes.
      h0log("*decompress_next_vcdu() returns err status = %d:\n", rstatus);
      decompress_next_vcdu_err_str(rstatus, errtxt);
      h0log("%s\n\n", errtxt);
      **********/
      break;
    }
    if(rstatus > 1) {		/* !!TBD just free hk pkts for now */
      /*decompress_free_hk(hk_packets);*/
    }
BYPASS:
    if(sigtermflg) { now_do_term_sig(); }
    if(sigalrmflg) { now_do_alrm_sig(); }
  }
  if(!eflg) {
    h0log("*No errors in tlm file\n");
  }
  /* !!TBD ck for incomplete pkt */
  fclose(fpin);
  ftmp = StopTimer(1);
  h0log("**Processed %s with\n**complete images %d and %d VCDUs in %f sec\n\n",
	file, imagecnt, fpkt_cnt, ftmp);
  if(fpkt_cnt != total_tlm_vcdu) {
    h0log("**WARNING: Found #vcdu=%d; expected=%d\n", fpkt_cnt, total_tlm_vcdu);
  }
  if(gap_24_cnt != total_missing_vcdu) {
    h0log("**WARNING: VCDU 24bit cntr gaps=%d; expected=%d\n",
	 gap_24_cnt, total_missing_vcdu);
  }
  if(gap_42_cnt != total_missing_im_pdu) {
    h0log("**WARNING: IM_PDU 42bit cntr gaps=%lld; expected=%lld\n",
	 gap_42_cnt, total_missing_im_pdu);
  }
/***********************************************************************
  tsum[1] += ftmp;
  h0log("Time for IMAGECOMPLETE summary = %fsec\n", tsum[2]);
  h0log("Time for fitz write summary = %fsec\n", tsum[3]);
  h0log("Time for catalog_image summary = %fsec\n", tsum[4]+tsum[5]+tsum[6]);
  h0log("Time for tlm file image summary = %fsec\n \n", tsum[1]);
*************************************************************************/
  return(0);
}

/* This is called from the main loop to check if any .parc files are in
 * the pipeline to soc dir ($DIRPIPE2SOC).
 * The .parc file is sent to /dds/pipe2soc/aia or /dds/pipe2soc/hmi
 * every time the pipeline back end system does a tapearc. (during development
 * the .parc files are sent by the cron job /home/jim/cvs/jsoc/scripts/pipefe_rm
 * on d00.)
 * The .parc file
 * has info on storage units that were archived successfully by the backend,
 * and so can be marked as such in the data capture sum_main table.
 * The sum_main table is updated for its safe_tape info from the .parc.
 * A .parc file looks like:
 * dcs0.jsoc:/dds/pipe2soc/aia> t AIA_2007_131_11_56.parc 
 * VC01_2007_131_11_51_39_0123456789A_FFFFF_00 000000S1 666 2007-04-12 17:15:45
 * VC04_2007_131_11_52_09_0123456789A_FFFFF_00 000000S1 666 2007-04-12 17:15:45
 *
 * storage_unit_name pipeline_tape_id_archived_on tape_fn date
*/
void do_pipe2soc() {
  DIR *dfd;
  struct dirent *dp;
  FILE *fp;
  int ptape_fn, complete;
  char line[128], fname[128], cmd[128];
  char *su_name, *ptape_id, *ptape_date;

  char *frompipedir; /* !!!TEMP to compile */

  /* only run this on the primary channel ingest_tlm process */
  if(strcmp(pchan, "VC01") && strcmp(pchan, "VC02")) { return; }

    if(DS_ConnectDB_Q(dbname)) {
      printk("**Can't connect to DB %s\n", dbname);
      abortit(3);
    }

  if((dfd=opendir(frompipedir)) == NULL) {
    printk("**Can't opendir(%s) to find files\n", frompipedir);
    abortit(3);
  }
  while((dp=readdir(dfd)) != NULL) {
    if(strstr(dp->d_name, ".parc")) {
      sprintf(fname, "%s/%s", frompipedir, dp->d_name);
      if(!(fp=fopen(fname, "r"))) {
        printk("***Can't open %s\n", fname);
        continue;
      }
      printk("Found parc file: %s\n", fname);
      complete = 1;
      while(fgets(line, 128, fp)) {       /* get .parc file lines */
        if(line[0] == '#' || line[0] == '\n') continue;
        printk("%s", line);
        su_name = (char *)strtok(line, " ");
        ptape_id = (char *)strtok(NULL, " ");
        ptape_fn = atoi((char *)strtok(NULL, " "));
        ptape_date = (char *)strtok(NULL, "\n");
        if(SUMLIB_SafeTapeUpdate(su_name,ptape_id,ptape_fn,ptape_date)) {
          printk("**ERROR in SUMLIB_SafeTapeUpdate(%s...)\n", su_name);
          complete = 0;
        }
      }
      fclose(fp);
      if(complete) {
        sprintf(cmd, "/bin/rm -f %s", fname);
        printk("%s\n", cmd);
        system(cmd);
      }
    }
  }
  closedir(dfd);
  DS_DisConnectDB_Q();
}

/* This is the main loop that gets the .qac and .tlm files and 
 * ingests them into SUMS.
 */
void do_ingest()
{
  FILE *fp;
  DIR *dfd;
  NAMESORT *nameptr;
  struct dirent *dp;
  float ttmp;
  int found, i, j, status;
  char name[128], line[128], mvname[128], tlmfile[128], tlmname[96];
  char cmd[128], xxname[128], tlmsize[80];
  char *token;

  char *tlmdir; /* !!!TEMP to compile */
  char *pipedir; /* !!!TEMP to compile */

  /* init summary timers */
  for(i=0; i < NUMTIMERS; i++) {
    tsum[i] = 0.0;
  }
  if((dfd=opendir(tlmdir)) == NULL) {
    printk("**Can't opendir(%s) to find files\n", tlmdir);
    abortit(3);
  }
  found = 0; i = 0;
  if((nameptr = (NAMESORT *)malloc(MAXFILES * sizeof(NAMESORT))) == NULL) {
    printk("***Can't alloc memory for file name sort\n");
    abortit(3);
  }

  while((dp=readdir(dfd)) != NULL) {
    /* printk("%s\n", dp->d_name) ; continue;*/ /* !!TEMP */
    /* Only accept our files. */
    if(strstr(dp->d_name, pchan) || strstr(dp->d_name, rchan) || strstr(dp->d_name, ".dsf")) {
      nameptr[i++].name = strdup(dp->d_name);
      if(i >= MAXFILES) {
        printk("***Fatal error. Too many (%d) files in %s\n", MAXFILES, tlmdir);
        abortit(3);
      }
    }
  }
  closedir(dfd);
  qsort(nameptr, i, sizeof(NAMESORT), &compare_names);

  for(j=0; j < i; j++) {
    /*printk("####QSORT FILES: %s\n", nameptr[j].name); /* !!TEMP */
    /* OLD. the .dsf file is now moved by dds_soc program to pipedir */
    /********************
    if(strstr(nameptr[j].name, ".dsf")) {
      sprintf(cmd, "/bin/mv %s/%s %s", tlmdir, nameptr[j].name, pipedir);
      printk("*mv dsf file to %s\n", pipedir);
      printk("%s\n", cmd);
      if(system(cmd)) {
        printk("***Error on: %s\n", cmd);
      }
    }
    *********************/
    if(!strstr(nameptr[j].name, ".qac")) {	/* can be .qac or .qacx */
      free(nameptr[j].name);
      continue;
    }
    StartTimer(NUMTIMERS-1);
    sprintf(name, "%s/%s", tlmdir, nameptr[j].name);
    printk("\n*Found qac file:\n* %s\n", name);
    if(!(fp=fopen(name, "r"))) {
      printk("***Can't open %s\n", name);
      free(nameptr[j].name);
      continue;
    }
    found = 1;
    /* NOTE: the qac file is already verified by the caller of ingest_tlm */
    while(fgets(line, 256, fp)) {	/* get qac file lines */
      if(line[0] == '#' || line[0] == '\n') continue;
      if(strstr(line, "TLM_FILE_NAME=")) {
        token = (char *)strtok(line, "=");
        token = (char *)strtok(NULL, "\n");
        printk("tlm file is %s\n", token);
        sprintf(tlmfile, "%s/%s", tlmdir, token);
        sprintf(tlmname, "%s", token);
      }
      else if(strstr(line, "TLM_FILE_SIZE=")) {
        token = (char *)strtok(line, "=");
        token = (char *)strtok(NULL, "=");
        printk("*tlm file size is %s", token);
        sprintf(tlmsize, "%s", token);
        tlmsize[strlen(token)-1] = 0;
        reqbytes = (double)atol(token);
        /*reqbytes += (double)1000000;*/	/* add some overhead */
      }
      else if(strstr(line, "TOTAL_TLM_VCDU=")) {
        token = (char *)strtok(line, "=");
        token = (char *)strtok(NULL, "\n");
        total_tlm_vcdu = atoi(token);
      }
      else if(strstr(line, "TOTAL_MISSING_VCDU=")) {
        token = (char *)strtok(line, "=");
        token = (char *)strtok(NULL, "\n");
        total_missing_vcdu = atoi(token);
      }
      else if(strstr(line, "TOTAL_MISSING_IM_PDU=")) {
        token = (char *)strtok(line, "=");
        token = (char *)strtok(NULL, "\n");
        total_missing_im_pdu = atol(token);
        break;
      }
    }
    fclose(fp);

    /* get storage to ingest the files */
      sum->bytes = reqbytes;
      sum->reqcnt = 1;
      if(status = SUM_alloc(sum, h0log)) { /* allocate a data segment */
	printk("***Can't allocate %g bytes in SUM_alloc. Error code = %d\n", 
		reqbytes, status);
	abortit(3);
      }
      cptr = sum->wd;
      dsixpt = sum->dsix_ptr;
      alloc_index = *dsixpt;
      strcpy(alloc_wd, *cptr);
      printk("*Alloc %g bytes at %s dsindex=%ld\n",
                        sum->bytes, *cptr, alloc_index);

    /* cp the files to the ingest dir */
    sprintf(mvname, "%s/%s", alloc_wd, nameptr[j].name);
    free(nameptr[j].name);

            /*StartTimer(7);*/
	    /*sprintf(cmd, "cp -p %s %s", name, mvname);*/
	    sprintf(cmd, "cp -p %s %s", name, alloc_wd);
            printk("*cp qac to %s\n", alloc_wd);
	    printk("%s\n", cmd);
	    if(status = system(cmd)) {
	    printk("***Error %d on: %s\n", status, cmd);
	    abortit(1);
	    }
	    sprintf(cmd, "cp -p %s %s", tlmfile, alloc_wd);
            printk("*cp tlm to %s\n", alloc_wd);
	    printk("%s\n", cmd);
	    if(system(cmd)) {
	    printk("***Error on: %s\n", cmd);
	    abortit(1);
	    }
            /*tsum[7] += StopTimer(7);*/

    /* now catalog the ingested files with the database */
    sum->mode = ARCH;
    token = (char *)rindex(tlmname, '.');
    *token = 0;			/* elim .tlm for ds name */
    sum->dsname = tlmname;
    sum->group = 1;		/* !!TBD ck if always use group 1 */
    sum->storeset = 0;		/* always use set 0 for datacapture */
    sum->reqcnt = 1;
    if(SUM_put(sum, h0log)) {   /* save the data segment for archiving */
      printk("**Error: on SUM_put()\n");
    }
    else {
      printk("*SUM_put() successfull for wd = %s\n", *sum->wd);
      printk("Marked for archive data unit ds_index=%ld\n", *dsixpt);
    }

    sprintf(cmd, "/bin/mv %s %s", name, pipedir);
    /*StartTimer(7);*/
    printk("*mv qac file to %s\n", pipedir);
    printk("%s\n", cmd);
    if(system(cmd)) {
      printk("***Error on: %s\n", cmd);
    }
    sprintf(cmd, "/bin/mv %s %s", tlmfile, pipedir);
    printk("*mv tlm file to %s\n", pipedir);
    printk("%s\n", cmd);
    if(system(cmd)) {
      printk("***Error on: %s\n", cmd);
    }
    /*tsum[7] += StopTimer(7);*/
    /*printk("Time for cp and mv of raw files = %fsec\n", tsum[7]);*/

    /* new stuff to do lev0 below !!!!TBD check !!! */
    sprintf(xxname, "%s/%s.tlm", alloc_wd, tlmname);
    tlmactive = 1;              /* set active for sigalrm */
    if(get_tlm(xxname)) {       /* lev0 extraction of image */
      h0log("***Error in lev0 extraction for %s\n", xxname);
    }
    tlmactive = 0;
    ttmp = StopTimer(NUMTIMERS-1);
    printk("Rate tlm %s bytes in %f sec\n", tlmsize, ttmp);

  }
  free(nameptr);
  /*if(!found) { printk("No .qac files found in %s\n", DIRDDS); }*/
}

/* Initial setup stuff called when main is first entered.
 */
void setup(int argc, char *argv[])
{
  int i;
  time_t tval;
  struct tm *t_ptr;
  char logname[128], string[128], cwdbuf[128], idstr[256];

  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
    signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
    signal(SIGTERM, sighandler);
  signal(SIGALRM, alrm_sig);

  tval = time(NULL);
  t_ptr = localtime(&tval);
  sprintf(datestr, "%d.%02d.%02d_%02d:%02d:%02d", 
	  (t_ptr->tm_year+1900), (t_ptr->tm_mon+1),
	  t_ptr->tm_mday, t_ptr->tm_hour, t_ptr->tm_min, t_ptr->tm_sec);
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  if(!logfile) {		/* no logfile given on command line */
    sprintf(logname, H0LOGFILE, username, getpid());
    /*open_h0log(logname, "w");	/* open new file for write */
  } else {
    /*open_h0log(logfile, "a");   /* open given file for append */
  }
  printk_set(h0log, h0log);	/* set for printk calls */
  printk("%s\n", datestr);
  getcwd(cwdbuf, 126);
  sprintf(idstr, "Cwd: %s\nCall: ", cwdbuf);
  for(i=0; i < argc; i++) { 	/* echo cmd line */
    sprintf(string, "%s%s", argv[i], (i < argc-1) ? " " : "");
    strcat(idstr, string);
  }
  strcat(idstr, "\n");
  sprintf(string, "ingest_tlm started as pid=%d user=%s\n", getpid(), username);
  strcat(idstr, string);
  printk("*%s", idstr);
  /*printk("*%s\n", datestr);*/
  /*load_all_apids_hk_configs();   /* load hk config files */
  umask(002);			/* allow group write */
  open_sum();			/* open with sum_svc */
}

/*int main(int argc, char *argv[])
/*{
/*  get_cmd(argc, argv);		/* check the calling sequence */
/*  setup(argc, argv);		/* start pvm and init things */
/*  alarm(ALRMSEC);		/* ck for partial images every 60 sec */
/*  while(1) {
/*    do_ingest();		/* loop to get files from the input dir */
/*    do_pipe2soc();		/* get any .parc files from pipeline backend */
/*    sleep(4);
/*    if(sigtermflg) { now_do_term_sig(); }
/*  }
/*}
*/

