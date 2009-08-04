%module SUMSAPI
%include "typemaps.i"

%typemap(memberin) char * dsname {
  $1 = (char *)malloc(strlen($input)+1);
  strcpy($1, $input);
}

%typemap(in) uint64_t * dsix_ptr {
	AV *tempav;
	I32 len;
	int i;
	SV  **tv;
	if (!SvROK($input))
	    croak("Argument $argnum is not a reference.");
        if (SvTYPE(SvRV($input)) != SVt_PVAV)
	    croak("Argument $argnum is not an array.");
        tempav = (AV*)SvRV($input);
	len = av_len(tempav);
	$1 = (uint64_t*) malloc((len+1)*sizeof(uint64_t));
 	for (i = 0; i <= len; i++) {
	    tv = av_fetch(tempav, i, 0);	
	    $1[i] = strtoull(SvPV(*tv,PL_na), 0, 0);
        }
};

// Creates a new Perl array and places an uint64_t array * into it
%typemap(out) uint64_t * dsix_ptr {
        char temp[256];
	AV *myav;
	SV **svs;
	int i = 0,len = 0;
	/* Figure out how many elements we have */
	while ($1[len])
	   len++;
        len=1; /* only care for the first one */
	svs = (SV **) malloc((len)*sizeof(SV *));
	for (i = 0; i < len ; i++) {
            sprintf(temp,"%llu", $1[i]); 
	    svs[i] = sv_newmortal();
            sv_setpv((SV*)svs[i],temp);
	};
	myav =	av_make(len,svs);
	free(svs);
        $result = newRV((SV*)myav);
        sv_2mortal($result);
        argvi++;
}


// This tells SWIG to treat char ** as a special case
%typemap(in) char ** wd {
	AV *tempav;
	I32 len;
	int i;
	SV  **tv;
	if (!SvROK($input))
	    croak("Argument $argnum is not a reference.");
        if (SvTYPE(SvRV($input)) != SVt_PVAV)
	    croak("Argument $argnum is not an array.");
        tempav = (AV*)SvRV($input);
	len = av_len(tempav);
	$1 = (char **) malloc((len+2)*sizeof(char *));
	for (i = 0; i <= len; i++) {
	    tv = av_fetch(tempav, i, 0);	
	    $1[i] = (char *) SvPV(*tv,PL_na);
        }
	$1[i] = NULL;
};

// This cleans up the char ** array after the function call
%typemap(freearg) char ** wd {
	free($1);
}

// Creates a new Perl array and places a NULL-terminated char ** into it
%typemap(out) char ** wd{
	AV *myav;
	SV **svs;
	int i = 0,len = 0;
	/* Figure out how many elements we have */
	while ($1[len])
	   len++;
	svs = (SV **) malloc(len*sizeof(SV *));
	for (i = 0; i < len ; i++) {
	    svs[i] = sv_newmortal();
	    sv_setpv((SV*)svs[i],$1[i]);
	};
	myav =	av_make(len,svs);
	free(svs);
        $result = newRV((SV*)myav);
        sv_2mortal($result);
        argvi++;
}

%{ 
/* Put header files here or function declarations like below */
#include <rpc/rpc.h> 

typedef unsigned int SUMID_t;

typedef struct SUM_struct
{
  SUMID_t uid;
  CLIENT *cl;            /* client handle for calling sum_rpc_svc */
  int debugflg;		 /* verbose debug mode if set */
  int mode;              /* bit map of various modes */
  int tdays;             /* touch days for retention */
  int group;             /* group # for the given dataseries */
  int storeset;          /* assign storage from JSOC, DSDS, etc. Default JSOC */
  int status;		 /* return status on calls. 1 = error, 0 = success */
  double bytes;
  char *dsname;          /* dataseries name */
  char *username;	 /* user's login name */
  char *history_comment; /* history comment string */
  int reqcnt;            /* # of entries in arrays below */
  uint64_t *dsix_ptr;    /* ptr to array of dsindex uint64_t */
  char **wd;		 /* ptr to array of char * */
} SUM_t;

extern SUM_t *SUM_open(char *server, char *db, int (*history)(const char *fmt, ...));
extern int SUM_close(SUM_t *sum, int (*history)(const char *fmt, ...));
extern int SUM_get(SUM_t *sum, int (*history)(const char *fmt, ...));
extern int SUM_put(SUM_t *sum, int (*history)(const char *fmt, ...));
extern int SUM_alloc(SUM_t *sum, int (*history)(const char *fmt, ...));
extern int SUM_poll(SUM_t *sum);
extern int SUM_wait(SUM_t *sum);

%}

typedef uint32_t SUMID_t;

typedef struct SUM_struct
{
  SUMID_t uid;
  CLIENT *cl;            /* client handle for calling sum_rpc_svc */
  int debugflg;		 /* verbose debug mode if set */
  int mode;              /* bit map of various modes */
  int tdays;             /* touch days for retention */
  int group;             /* group # for the given dataseries */
  int storeset;          /* assign storage from JSOC, DSDS, etc. Default JSOC */
  int status;		 /* return status on calls. 1 = error, 0 = success */
  double bytes;
  char *dsname;          /* dataseries name */
  char *username;	 /* user's login name */
  char *history_comment; /* history comment string */
  int reqcnt;            /* # of entries in arrays below */
  uint64_t *dsix_ptr;    /* ptr to array of dsindex uint64_t */
  char **wd;		 /* ptr to array of char * */
} SUM_t;

%inline %{
SUM_t *SUMOpen(char *server, char *db) {
  return SUM_open(server, db, printf);
}

int SUMAlloc(SUM_t *sum) {
/*   printf("%lf\n", sum->bytes); */
  return SUM_alloc(sum, printf);
}

int SUMGet(SUM_t *sum) {
/*   int i;  */
/*   printf("suid = "); */
/*   for (i = 0; i < sum->reqcnt; i++) { */
/*     printf("%ld ", sum->dsix_ptr[i]); */
/*   } */
/*   printf("\n"); */
/*   printf("tdays = %d\n", sum->tdays); */
/*   printf("reqcnt = %d\n", sum->reqcnt); */
/*   printf("mode = %d\n", sum->mode); */
  return SUM_get(sum, printf);
}

int SUMPut(SUM_t *sum) {
/*   printf("reqcnt = %d ", sum->reqcnt); */
/*   printf("dsix = %lld ", sum->dsix_ptr[0]); */
/*   printf("wd = %s ", sum->wd[0]); */
/*   printf("mode = %d ", sum->mode); */
/*   printf("dsname = %s\n", sum->dsname); */
  return SUM_put(sum, printf);
}

int SUMClose(SUM_t *sum) {
  return SUM_close(sum, printf);
}

%}
