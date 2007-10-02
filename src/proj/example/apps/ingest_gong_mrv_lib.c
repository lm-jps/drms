#define HAVE_LONGLONG   /* This is defined here so the LONGLONG
                           typedef in cfortran.h and fitsio.h
                           don't clash */
#include <jsoc_main.h>
#include <drms_fortran.h>
#include <fitsio.h>
#include <string.h>
#include <dirent.h>

static char * formatObsDateTime(char *, char * , char *);
static int    replace_str( char *, char, char );
int    gong2drms_set_keywords (char *, int, char *);

int gong2drms_set_keywords (char *rec_hdl, int headlen, char *header) {
  DRMS_Record_t  * record = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  int status=0;
  int i;

  int key_len=0;

  char *ghist=(char *) NULL;
  char card[FLEN_CARD], svalue[FLEN_CARD];
  char date_part[250], obs_datetime[250];
  long ivalue;
  float fvalue;
  const char *p;
  
/* from cfitsio START */
  char keyname[FLEN_KEYWORD], value[FLEN_VALUE], comm[FLEN_COMMENT];
/* cfitsio END */

  p=header;

  for (i=0; i<headlen; i+=80, p+=80) {
    status=0;
    strncpy(card, p, 80);
    card[80]='\0';
    ffgknm(card, keyname, &key_len, &status);

    if (keyname[0] == '\0') break;

    ffpsvc(card, value, comm, &status);

    /* truncate trailing non-significant blanks */
    /* string are returned quoted :             */
    /*  E.g. 'SE    '                           */
    /*     That's why of the -2 in the for init */
    for (i = (strlen(value) - 2); i >= 0 && value[i] == ' '; i--) {
        value[i+1] = '\0'; value[i]='\'';
    }


    if ( !strcmp(keyname, "BSCALE") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "BSCALE", fvalue);
    }
    if ( !strcmp(keyname, "BZERO") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "BZERO", svalue);
    }
    if ( !strcmp(keyname, "OBJECT") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "OBJECT", svalue);
    }
    if ( !strcmp(keyname, "ORIGIN") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "ORIGIN", svalue);
    }
    if ( !strcmp(keyname, "DATE") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "DATE", fvalue);
    }
    if ( !strcmp(keyname, "IRAFNAME") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "IRAFNAME", svalue);
    }
    if ( !strcmp(keyname, "IRAF-MAX") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "IRAF__MAX", fvalue);
    }
    if ( !strcmp(keyname, "IRAF-MIN") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "IRAF__MIN", fvalue);
    }
    if ( !strcmp(keyname, "IRAF-BPX") ) {
      ffc2ii(value, &ivalue, &status);
      drms_setkey_int(record, "IRAF__BPX", ivalue);
    }
    if ( !strcmp(keyname, "IRAFTYPE") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "IRAFTYPE", svalue);
    }
    if ( !strcmp(keyname, "ORIGIN") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "ORIGIN", svalue);
    }

    if ( !strcmp(keyname, "MAP_TYPE") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "MAP_TYPE", svalue);
    }
    if ( !strcmp(keyname, "ITIME") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "ITIME", fvalue);
    }

    if ( !strcmp(keyname, "DATE-OBS") ) {
      ffc2s(value, svalue, &status);
      strcpy(date_part,svalue);
    }

    if ( !strcmp(keyname, "TIME-OBS") ) {
      ffc2s(value, svalue, &status);
      drms_setkey_string(record, "DATETIME__OBS", formatObsDateTime(obs_datetime, date_part, svalue));
    }

    if ( !strcmp(keyname, "SITENAME") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "SITENAME", svalue);
    }

    if ( !strcmp(keyname, "SITE") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "SITE", svalue);
    }

    if ( !strncmp(keyname, "GHIST", sizeof(char) * 5) ) {
      if ( !strcmp(keyname, "GHISTSEQ") ) {
        ffc2ii(value,&ivalue,&status);
        //fprintf(stderr, "Malloacing [%ld]\n", sizeof(char)*80*(ivalue));
        ghist=malloc((size_t) sizeof(char)*80*(ivalue));
        memset(ghist,'\0',(size_t) sizeof(char)*80*(ivalue));
      } else {
        ffc2s(value,svalue,&status);
        //fprintf(stderr, "svalue [%s]\n", svalue);
        strcpy(ghist+ sizeof(char) * strlen(ghist),svalue);
        //fprintf(stderr, "ghist [%s]\n", ghist);
        drms_setkey_string(record, "GHIST", ghist);
      }
    }

    if ( !strcmp(keyname, "TYPE") ) {
      ffc2ii(value, &ivalue, &status);
      drms_setkey_int(record, "TYPE", ivalue);
    }

    if ( !strcmp(keyname, "DTYPE") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "DTYPE", svalue);
    }

    if ( !strcmp(keyname, "VELSCALE") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "VELSCALE", fvalue);
    }

    if ( !strcmp(keyname, "VEL_BIAS") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "VEL_BIAS", fvalue);
    }

    if ( !strcmp(keyname, "VCOR1") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "VCOR1", fvalue);
    }
    if ( !strcmp(keyname, "OFFSET") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "GOFFSET", fvalue);
    }
    if ( !strcmp(keyname, "FNDLMBXC") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "FNDLMBXC", fvalue);
    }
    if ( !strcmp(keyname, "FNDLMBYC") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "FNDLMBYC", fvalue);
    }
    if ( !strcmp(keyname, "FNDLMBMA") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "FNDLMBMA", fvalue);
    }
    if ( !strcmp(keyname, "FNDLMBMI") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "FNDLMBMI", fvalue);
    }
    if ( !strcmp(keyname, "FNDLMBAN") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "FNDLMBAN", fvalue);
    }
    if ( !strcmp(keyname, "C_MA") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "C_MA", fvalue);
    }
    if ( !strcmp(keyname, "C_MI") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "C_MI", fvalue);
    }
    if ( !strcmp(keyname, "PIXLENX") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "PIXLENX", fvalue);
    }
    if ( !strcmp(keyname, "PIXLENY") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "PIXLENY", fvalue);
    }
    if ( !strcmp(keyname, "B0") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "B0", fvalue);
    }
    if ( !strcmp(keyname, "L0") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "L0", fvalue);
    }
    if ( !strcmp(keyname, "P_ANGLE") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "P_ANGLE", fvalue);
    }
    if ( !strcmp(keyname, "SEMIDIAM") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "SEMIDIAM", fvalue);
    }
    if ( !strcmp(keyname, "DELTA_L0") ) {
      ffc2rr(value, &fvalue, &status);
      drms_setkey_float(record, "DELTA_L0", fvalue);
    }
    if ( !strcmp(keyname, "N_IMGMRG") ) {
      ffc2ii(value, &ivalue, &status);
      drms_setkey_int(record, "N_IMGMRG", ivalue);
    }
    if ( !strcmp(keyname, "MRG_IM01") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "MRG_IM01", svalue);
    }
    if ( !strcmp(keyname, "MRG_IM02") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "MRG_IM02", svalue);
    }
    if ( !strcmp(keyname, "MRG_IM03") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "MRG_IM03", svalue);
    }
    if ( !strcmp(keyname, "MRG_IM04") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "MRG_IM04", svalue);
    }
    if ( !strcmp(keyname, "MRG_IM05") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "MRG_IM05", svalue);
    }
    if ( !strcmp(keyname, "MRG_IM06") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "MRG_IM06", svalue);
    }
    if ( !strcmp(keyname, "MRG_IM07") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "MRG_IM07", svalue);
    }
    if ( !strcmp(keyname, "INORM") ) {
      ffc2s(value,svalue,&status);
      drms_setkey_string(record, "INORM", svalue);
    }
  }

  if (ghist != (char * ) NULL) {
    free(ghist);
  }

  return 0;
}

FCALLSCFUN3(INT, gong2drms_set_keywords, F_GONG2DRMS_SET_KEYWORDS, f_gong2drms_set_keywords, STRING, INT, STRING)

char * formatObsDateTime(char * obs_datetime, char * date_part, char * time_part) {
    
  char date[250];

  sprintf(date, "%s %s", date_part, time_part); 

  if (!strcmp(date,"UTC")) {
    strcpy(obs_datetime, date);
  } else {
    sprintf(obs_datetime, "%s UTC", date);
  }

  replace_str(obs_datetime, '/', '.');
  replace_str(obs_datetime, ' ', '_');

  return obs_datetime;
}

int replace_str( char * p, char c_from, char c_to) {
  char *r_chr;
  int count=0;
  while((r_chr=strrchr(p,c_from))!= (char *) NULL) {
    *r_chr = c_to;
    count++;
  }
  return count;
}
