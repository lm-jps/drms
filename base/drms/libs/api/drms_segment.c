/*
 *  drms_segment.c                                              2007.11.26
 *
 *  functions defined:
 *      drms_free_template_segment_struct
 *      drms_free_segment_struct
 *      drms_copy_segment_struct
 *      drms_create_segment_prototypes
 *      drms_template_segments
 *      drms_segment_print
 *      drms_segment_size
 *      drms_segment_setdims
 *      drms_segment_getdims
 *      drms_segment_createinfocon
 *      drms_segment_destroyinfocon
 *      drms_segment_filename
 *      drms_delete_segmentfile
 *      drms_segment_setscaling
 *      drms_segment_getscaling
 *      drms_segment_lookup
 *      drms_segment_lookupnum
 *      drms_segment_read
 *      drms_segment_readslice
 *      drms_segment_write
 *      drms_segment_write_from_file
 *      drms_segment_setblocksize
 *      drms_segment_getblocksize
 *      drms_segment_autoscale
 *      drms_segment_segsmatch
 */

//#define DEBUG

#include "drms.h"
#include "drms_priv.h"
#include <float.h>
#include <dlfcn.h>
#include "xmem.h"
#include "drms_dsdsapi.h"
#include "cfitsio.h"

const char kSIMPLE[] = "SIMPLE";
const char kBITPIX[] = "BITPIX";
const char kNAXIS[] = "NAXIS";
const char kNAXISn[] = "NAXIS%d";

/******** helper functions that don't get exported as part of API ********/
static DRMS_Segment_t * __drms_segment_lookup(DRMS_Record_t *rec, 
					      const char *segname, int depth);

static int drms_segment_set_const(DRMS_Segment_t *seg);
static int drms_segment_checkscaling (const DRMS_Array_t* arr, double bzero, 
				      double bscale, const char *filename);

/* 
   Recursive segment lookup that follows linked segment to 
   their destination until a non-link segment is reached. If the 
   recursion depth exceeds DRMS_MAXLINKDEPTH it is assumed that there 
   is an erroneous link cycle, an error message is written to stderr,
   and a NULL pointer is returned.
*/
static DRMS_Segment_t * __drms_segment_lookup(DRMS_Record_t *rec, 
					      const char *segname, int depth)
{
  int stat;
  DRMS_Segment_t *segment;

  segment = hcon_lookup_lower(&rec->segments, segname);
  if (segment!=NULL && depth<DRMS_MAXLINKDEPTH )
  {
    if (segment->info->islink)
    {
      rec = drms_link_follow(rec, segment->info->linkname, &stat);
      if (stat)
	return NULL;
      else
	return __drms_segment_lookup(rec, segment->info->target_seg, depth+1);
    }
    else
    {
      return segment;
    }
  }
  if (depth>=DRMS_MAXLINKDEPTH)
    fprintf(stderr, "WARNING: Max link depth exceeded for segment '%s' in "
	    "record %lld from series '%s'\n", segment->info->name, rec->recnum, 
	    rec->seriesinfo->seriesname);
  
  return NULL;
}

static DRMS_Type_t Bitpix2Type(int bitpix, int *err)
{
   DRMS_Type_t type = DRMS_TYPE_CHAR;
   int error = 0;

   switch(bitpix)
   {
      case 8:
	type = DRMS_TYPE_CHAR; 
	break;
      case 16:
	type = DRMS_TYPE_SHORT;
	break;
      case 32:
	type = DRMS_TYPE_INT;
	break;
      case 64:
	type = DRMS_TYPE_LONGLONG;  
	break;
      case -32:
	type = DRMS_TYPE_FLOAT;
	break;
      case -64:
	type = DRMS_TYPE_DOUBLE; 	
	break;
      default:
	fprintf(stderr, "ERROR: Invalid bitpix value %d\n", bitpix);
	error = 1;
	break;
   }

   if (err)
   {
      *err = error;
   }

   return type;
}

static int Type2Bitpix(DRMS_Type_t type, int *err)
{
   int bitpix = 0;
   int error = 0;

   switch(type)
   {
      case DRMS_TYPE_CHAR: 
	bitpix = 8;
	break;
      case DRMS_TYPE_SHORT:
	bitpix = 16;
	break;
      case DRMS_TYPE_INT:  
	bitpix = 32;
	break;
      case DRMS_TYPE_LONGLONG:  
	bitpix = 64;
	break;
      case DRMS_TYPE_FLOAT:
	bitpix = -32;
	break;
      case DRMS_TYPE_DOUBLE: 	
      case DRMS_TYPE_TIME: 
	bitpix = -64;
	break;
      case DRMS_TYPE_STRING: 
      default:
	fprintf(stderr, "ERROR: Unsupported DRMS type %d\n", (int)type);
	error = 1;
	break;
   }

   if (err)
   {
      *err = error;
   }

   return bitpix;
}

static void ShootBlanks(DRMS_Array_t *arr, long long blank)
{
   if (arr->type != DRMS_TYPE_FLOAT &&
       arr->type != DRMS_TYPE_DOUBLE &&
       arr->type != DRMS_TYPE_TIME)
   {
      int nelem = drms_array_count(arr);
      DRMS_Value_t val;
      long long dataval;

      while (nelem > 0)
      {
	 void *elem = (char *)arr->data + nelem;

	 DRMS_VAL_SET(arr->type, elem, val);
	 dataval = conv2longlong(arr->type, &(val.value), NULL);

	 if (dataval == blank)
	 {
	    drms_missing_vp(arr->type, elem);
	 }

	 nelem--;
      }
   }
}

static int CreateDRMSArray(CFITSIO_IMAGE_INFO *info, void *data, DRMS_Array_t **arrout)
{
   DRMS_Array_t *retarr = NULL;
   int err = 0;
   DRMS_Type_t datatype;
   int drmsstatus = DRMS_SUCCESS;
   int naxis = 0;
   int axes[DRMS_MAXRANK] = {0};
   int ia;

   if (info && data && arrout)
   {
      if (!(info->bitfield & kInfoPresent_SIMPLE) || !info->simple)
      {
	  err = 1;
	  fprintf(stderr, "Simple FITS file expected.\n");
      }

      if (!err)
      {
	 datatype = Bitpix2Type(info->bitpix, &err);
	 if (err)
	 {
	    fprintf(stderr, "BITPIX = %d not allowed\n", info->bitpix);
	 }
      }

      if (!err)
      {
	 if (info->naxis >= 1 && info->naxis <= DRMS_MAXRANK)
	 {
	    naxis = info->naxis;
	 }
	 else
	 {
	    err = 1;
	    fprintf(stderr, 
		    "%s = %d outside allowable DRMS range [1-%d].\n", 
		    kNAXIS,
		    info->naxis,
		    DRMS_MAXRANK);
	 }
      }

      if (!err)
      {
	 for (ia = 0; ia < naxis; ia++)
	 {
	    axes[ia] = (int)(info->naxes[ia]);
	 }
      }

      if (!err)
      {
	 /* retarr steals data */
	 retarr = drms_array_create(datatype, naxis, axes, data, &drmsstatus);
      }
      else
      {
	 err = 1;
      }

      retarr->bzero = 0.0;
      retarr->bscale = 1.0;

      if (!err && info->bitpix > 0)
      {
	 /* BLANK isn't stored anywhere in DRMS.  Just use DRMS_MISSING_XXX. 
	  * But need to convert data blanks to missing. */
	 if (info->bitfield & kInfoPresent_BLANK)
	 {
	    ShootBlanks(retarr, info->blank);
	 }

	 if (info->bitfield & kInfoPresent_BZERO)
	 {
	    retarr->bzero = info->bzero;
	 }

	 if (info->bitfield & kInfoPresent_BSCALE)
	 {
	    retarr->bscale = info->bscale;
	 }
      }

      if (!err)
      {
	 retarr->israw = 1; /* FITSRW should never apply BSCALE or BZERO */
	 *arrout = retarr;
      }
   }

   return err;
}

static int SetImageInfo(DRMS_Array_t *arr, CFITSIO_IMAGE_INFO *info)
{
   int err = 0;
   int ia;

   if (info)
   {
      memset(info, 0, sizeof(CFITSIO_IMAGE_INFO));
      info->bitpix = Type2Bitpix(arr->type, &err);

      if (!err)
      {
	 if (arr->naxis > 0)
	 {
	    info->naxis = arr->naxis;
	 }
	 else
	 {
	    err = 1;
	 }
      }

      if (!err)
      {
	 for (ia = 0; ia < arr->naxis; ia++)
	 {
	    info->naxes[ia] = (long)(arr->axis[ia]);
	 }

	 info->simple = 1;
	 info->extend = 0; /* baby steps - trying to dupe what happens in FITS protocol. */
	 info->bitfield = (info->bitfield | kInfoPresent_SIMPLE);
	 
	 if (info->bitpix > 0)
	 {
	    /* An integer type - need to set BLANK, and possibly BZERO and BSCALE. */
	    DRMS_Type_Value_t missing;
	    drms_missing(arr->type, &missing);

	    info->blank = conv2longlong(arr->type, &missing, NULL);
	    info->bitfield = (info->bitfield | kInfoPresent_BLANK);

	    if (arr->israw)
	    {
	       /* This means that the data COULD BE not real values,
		* and to get real values they need to be scaled by bzero/bscale. */
#ifdef ICCCOMP
#pragma warning (disable : 1572)
#endif
	       if (arr->bscale != 1.0 || fabs(arr->bzero) != 0.0)
	       {
		  info->bscale = arr->bscale;
		  info->bzero = arr->bzero;

		  info->bitfield = (info->bitfield | kInfoPresent_BSCALE);
		  info->bitfield = (info->bitfield | kInfoPresent_BZERO);
	       }
#ifdef ICCCOMP
#pragma warning (default : 1572)
#endif
	    }
	 }
      }
   }

   return err;
}

int drms_segment_set_const(DRMS_Segment_t *seg) {
  XASSERT(seg->info->scope == DRMS_CONSTANT &&
	  !seg->info->cseg_recnum);
  DRMS_Record_t *rec = seg->record;
  seg->info->cseg_recnum = rec->recnum;

  // write back to drms_segment table
  char stmt[DRMS_MAXQUERYLEN];
  char *namespace = ns(rec->seriesinfo->seriesname);
  sprintf(stmt, "UPDATE %s." DRMS_MASTER_SEGMENT_TABLE
	  " SET cseg_recnum = %lld WHERE seriesname = '%s' and segmentname = '%s'", 
	  namespace, rec->recnum, rec->seriesinfo->seriesname, seg->info->name);
  free(namespace);
  if(drms_dms(rec->env->session, NULL, stmt)) {
    fprintf(stderr, "Failed to update drms_segment table for constant segment\n");
    return DRMS_ERROR_QUERYFAILED;
  }
  return DRMS_SUCCESS;
}

int drms_segment_checkscaling (const DRMS_Array_t *a, double zero, double scale,
    const char *filename) {
  int ret = 1;
  double numz = fabs (a->bzero - zero);
  double nums = fabs (a->bscale - scale);
  double denz = fabs (zero);
  double dens = fabs (scale);

  denz += fabs (a->bzero);
  dens += fabs (a->bscale);

  if ((numz > 1.0e-11 * denz) || (nums > 1.0e-11 * dens)) {
    fprintf (stderr, "BZERO and BSCALE in FITS file '%s' do not match "
        "those in DRMS database.\n", filename);    
    fprintf (stderr, "reldif (bzero) = %g,  reldif (bscale) = %g\n",
	numz / denz, nums / dens); 
    ret = 0;
  }
  return ret;
}

/****************************** API functions ******************************/
void drms_free_template_segment_struct (DRMS_Segment_t *segment) {
  free (segment->info);
}

void drms_free_segment_struct (DRMS_Segment_t *segment) {
  XASSERT(segment);
  return;
}

void drms_copy_segment_struct(DRMS_Segment_t *dst, DRMS_Segment_t *src)
{
  /* Copy struct. */
  memcpy(dst,src,sizeof(DRMS_Segment_t));
  return;
}

/* returns a pointer to the target's segment container. */
HContainer_t *drms_create_segment_prototypes(DRMS_Record_t *target, 
					     DRMS_Record_t *source, 
					     int *status)
{
   HContainer_t *ret = NULL;
   DRMS_Segment_t *tSeg = NULL;
   DRMS_Segment_t *sSeg = NULL;

   XASSERT(target != NULL && target->segments.num_total == 0 && source != NULL);

   if (target != NULL && target->segments.num_total == 0 && source != NULL)
   {
      *status = DRMS_SUCCESS;
      HIterator_t *hit = hiter_create(&(source->segments));
      XASSERT(hit);

      while (hit && ((sSeg = hiter_getnext(hit)) != NULL))
      {
	 if (sSeg->info && strlen(sSeg->info->name) > 0)
	 {
	    XASSERT(tSeg = hcon_allocslot_lower(&(target->segments), sSeg->info->name));
	    memset(tSeg, 0, sizeof(DRMS_Segment_t));
	    XASSERT(tSeg->info = malloc(sizeof(DRMS_SegmentInfo_t)));
	    memset(tSeg->info, 0, sizeof(DRMS_SegmentInfo_t));

	    if (tSeg && tSeg->info)
	    {
	       /* record */
	       tSeg->record = target;
	       
	       /* segment info*/
	       memcpy(tSeg->info, sSeg->info, sizeof(DRMS_SegmentInfo_t));

	       /* axis is allocated as static array */
	       memcpy(tSeg->axis, sSeg->axis, sizeof(int) * DRMS_MAXRANK);

	        if (tSeg->info->protocol == DRMS_TAS)
		{
		   /* blocksize is allocated as static array */
		   memcpy(tSeg->blocksize, sSeg->blocksize, sizeof(int) * DRMS_MAXRANK);
		}
	    }
	    else
	    {
	       *status = DRMS_ERROR_OUTOFMEMORY;
	    }
	 }
	 else
	 {
	    *status = DRMS_ERROR_INVALIDSEGMENT;
	 }
      }

      if (hit)
      {
	 hiter_destroy(&hit);
      }

      if (*status == DRMS_SUCCESS)
      {
	 ret = &(target->segments);
      }
   }
   else
   {
      *status = DRMS_ERROR_INVALIDRECORD;
   }

   return ret;
}

/* 
   Build the segment part of a dataset template by
   using the query result holding a list of 
   (segmentname, segnum, form, scope, type, 
   naxis, axis, unit, protocol, description)
   tuples to initialize the array of segment descriptors.
*/
int drms_template_segments(DRMS_Record_t *template)
{
  DRMS_Env_t *env;
  int i,n,status = DRMS_NO_ERROR;
  char buf[1024], query[DRMS_MAXQUERYLEN], *p, *q;
  DRMS_Segment_t *seg;
  DB_Binary_Result_t *qres;

  env = template->env;

  /* Initialize container structure. */
  hcon_init(&template->segments, sizeof(DRMS_Segment_t), DRMS_MAXSEGNAMELEN, 
	    (void (*)(const void *)) drms_free_segment_struct, 
	    (void (*)(const void *, const void *)) drms_copy_segment_struct);

  /* Get segment definitions and add to template. */
  char *namespace = ns(template->seriesinfo->seriesname);
  sprintf(query, "select segmentname, segnum, scope, type, "
	  "naxis, axis, unit, protocol, description, "
	  "islink, linkname, targetseg, cseg_recnum from %s.%s where "
	  "seriesname ~~* '%s' order by segnum", 
	  namespace, DRMS_MASTER_SEGMENT_TABLE, template->seriesinfo->seriesname);
  free(namespace);
  if ((qres = drms_query_bin(env->session, query)) == NULL)
    return DRMS_ERROR_QUERYFAILED; /* SQL error. */

#ifdef DEBUG
  printf("#0\n");
  db_print_binary_result(qres);
#endif

  if (qres->num_rows>0 && qres->num_cols != 13 )
  {
    status = DRMS_ERROR_BADFIELDCOUNT;
    goto bailout;   
  }
  for (i = 0; i<(int)qres->num_rows; i++)
  {
    /* Name */
    db_binary_field_getstr(qres, i, 0, 1024, buf);    
    seg = hcon_allocslot_lower(&template->segments, buf);
    memset(seg,0,sizeof(DRMS_Segment_t));
    XASSERT(seg->info = malloc(sizeof( DRMS_SegmentInfo_t)));
    memset(seg->info,0,sizeof(DRMS_SegmentInfo_t));
    seg->record = template;
    strcpy(seg->info->name, buf);
    /* Number */
    seg->info->segnum = db_binary_field_getint(qres, i, 1);
    seg->info->islink = db_binary_field_getint(qres, i, 9);
    if (seg->info->islink) {
      /* Link segment */
      db_binary_field_getstr(qres, i, 10, sizeof(seg->info->linkname),seg->info->linkname);      
      db_binary_field_getstr(qres, i, 11, sizeof(seg->info->target_seg),seg->info->target_seg);
      seg->info->scope = DRMS_VARIABLE;
      seg->info->type = DRMS_TYPE_INT;
      seg->info->unit[0] = '\0';
      seg->info->protocol = DRMS_GENERIC;
    } else {
      /* Simple segment */
      /* Scope */
      db_binary_field_getstr(qres, i, 2, 1024, buf);    
      seg->info->cseg_recnum = 0;
      if (!strcmp(buf, "constant")) {
	seg->info->scope = DRMS_CONSTANT;
	seg->info->cseg_recnum = db_binary_field_getlonglong(qres, i, 12);
      }
      else if (!strcmp(buf, "variable"))
	seg->info->scope = DRMS_VARIABLE;
      else if (!strcmp(buf, "vardim"))
	seg->info->scope = DRMS_VARDIM;
      else
	{
	  printf("ERROR: Invalid segment scope specifier '%s'\n",buf);
	  goto bailout;
	}    
      /* Type */
      db_binary_field_getstr(qres, i, 3, 1024,buf);
      seg->info->type  = drms_str2type(buf);
      /* Unit */
      db_binary_field_getstr(qres, i, 6, DRMS_MAXUNITLEN, seg->info->unit);    
      /* Protocol */
      db_binary_field_getstr(qres, i, 7, 1024, buf);    
      seg->info->protocol = drms_str2prot(buf);
    }
    /* Naxis */      
    seg->info->naxis = db_binary_field_getint(qres, i, 4);
    /* Axis */
    db_binary_field_getstr(qres, i, 5, 1024,buf);
    n = 0;
    p = buf;
    /* Extract the axis dimensions (and in case of the TAS protocol 
       block sizes from a copy separated list. */
    while(*p)
      {
	XASSERT(n<2*seg->info->naxis); 
	while(!isdigit(*p))
	  ++p;
	q = p;
	while(isdigit(*p))
	  ++p;	       
	*p++ = 0;
	if (n<seg->info->naxis)
	  seg->axis[n] = atoi(q);
	else
	  seg->blocksize[n - seg->info->naxis] = atoi(q);	
	n++;
      }
    if (seg->info->protocol!=DRMS_TAS)
      {
	XASSERT(n==seg->info->naxis);
	memcpy(seg->blocksize,seg->axis,n*sizeof(int));
      }
    else
      {
	XASSERT(n==2*seg->info->naxis);
      }

    /* filename */
    seg->filename[0] = '\0';
    /* Comment */
    db_binary_field_getstr(qres, i, 8, DRMS_MAXCOMMENTLEN, seg->info->description);    
  }
#ifdef DEBUG
  printf("#4\n");
  db_print_binary_result(qres);
#endif
  db_free_binary_result(qres);      
  return DRMS_SUCCESS;

 bailout:
  db_free_binary_result(qres);
  return status;
}



/* Print the fields of a keyword struct to stdout. */
void drms_segment_print(DRMS_Segment_t *seg)
{
  drms_segment_fprint(stdout, seg);
}


/* Prints the fields of a keyword struct to file "keyfile". */
void drms_segment_fprint(FILE *keyfile, DRMS_Segment_t *seg)
{
  int i;
  const int fieldwidth=13;

  fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "name", seg->info->name);
  fprintf(keyfile, "\t%-*s:\t%d\n", fieldwidth, "segnum", seg->info->segnum);
  fprintf(keyfile, "\t%-*s:\t%s\n", fieldwidth, "description", seg->info->description);
  if (seg->info->islink) {
    fprintf(keyfile, "\t%-*s:\t%s\n", fieldwidth, "linkname", seg->info->linkname);
    fprintf(keyfile, "\t%-*s:\t%s\n", fieldwidth, "target segment", seg->info->target_seg);
  } else {
    switch(seg->info->scope)
      {
      case DRMS_CONSTANT:
	fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "scope", "CONSTANT");
	break;
      case DRMS_VARIABLE:
	fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "scope", "VARIABLE");
	break;
      case DRMS_VARDIM:
	fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "scope", "VARIABLE DIMENSION");
	break;
      default:
	fprintf(keyfile, "\t%-*s:\t%s %d\n", fieldwidth, "scope", "Illegal value",
	       (int)seg->info->scope);
      }
    fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "unit", seg->info->unit);
    fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth+9, "type", 
	   drms_type2str(seg->info->type));

    /* Scaling information. */
    {
      double bzero,bscale;
      if (!drms_segment_getscaling(seg,&bzero,&bscale))
	{
	  fprintf(keyfile, "\t%-*s:\t%g\n", fieldwidth+9, "bzero", bzero);
	  fprintf(keyfile, "\t%-*s:\t%g\n", fieldwidth+9, "bscale", bscale);
	}
    }

    /* Protocol info. */ 
    const char *protstr = drms_prot2str(seg->info->protocol);
    if (protstr)
    {
       fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "protocol", protstr);
    }
    else
    {
       fprintf(keyfile, "\t%-*s:\t%s %d\n", fieldwidth, "protocol", "Illegal value",
	       (int)seg->info->protocol);
    }

    if (strlen(seg->filename)) {
      fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "filename", seg->filename);  
    }
  }
  /* Array info. */
  fprintf(keyfile, "\t%-*s:\t%d\n", fieldwidth+9, "naxis", seg->info->naxis);
  for (i=0; i<seg->info->naxis; i++)
  {
    fprintf(keyfile, "\t%-*s[%2d]:\t%d\n", fieldwidth+5, "axis", i, 
	   seg->axis[i]);
  }

}

  
/* Return segment size in bytes. */  
long long drms_segment_size(DRMS_Segment_t *seg, int *status)
{
  int i;
  long long size;

  size = 1;
  for (i=0; i<seg->info->naxis; i++)
    size *= seg->axis[i];
  if (seg->info->type == DRMS_TYPE_STRING)
    size = size*sizeof(char *);
  else
    size *= drms_sizeof(seg->info->type);
  if (status)
    *status = DRMS_SUCCESS;
  return size;
}

int drms_segment_setdims(DRMS_Segment_t *seg, DRMS_SegmentDimInfo_t *di)
{
   int status = DRMS_SUCCESS;

   if (seg && di)
   {
      seg->info->naxis = di->naxis;
      memcpy(seg->axis, di->axis, sizeof(int) * di->naxis);
   }
   else
   {
      status = DRMS_ERROR_INVALIDDATA;
   }

   return status;
}

/* Get the record's segment axis dimensions. */
int drms_segment_getdims(DRMS_Segment_t *seg, DRMS_SegmentDimInfo_t *di)
{
   int status = DRMS_SUCCESS;

   if (seg && di)
   {
      di->naxis = seg->info->naxis;
      memcpy(di->axis, seg->axis, sizeof(int) * di->naxis);
   }

   return status;
}

HContainer_t *drms_segment_createinfocon(DRMS_Env_t *drmsEnv, 
					 const char *seriesName, 
					 int *status)
{
     HContainer_t *ret = NULL;

     DRMS_Record_t *template = drms_template_record(drmsEnv, seriesName, status);
     
     if (*status == DRMS_SUCCESS)
     {
	  int size = hcon_size(&(template->segments));
	  if (size > 0)
	  {
	       char **nameArr = (char **)malloc(sizeof(char *) * size);
	       DRMS_SegmentInfo_t **valArr = 
		 (DRMS_SegmentInfo_t **)malloc(sizeof(DRMS_SegmentInfo_t *) * size);

	       if (nameArr != NULL && valArr != NULL)
	       {
		    HIterator_t hit;
		    hiter_new(&hit, &(template->segments));
		    DRMS_Segment_t *seg = NULL;

		    int iSeg = 0;
		    while ((seg = hiter_getnext(&hit)) != NULL)
		    {
			 nameArr[iSeg] = seg->info->name;
			 valArr[iSeg] = seg->info;

			 iSeg++;
		    }

		    ret = hcon_create(sizeof(DRMS_SegmentInfo_t), 
				      DRMS_MAXSEGNAMELEN,
				      NULL,
				      NULL,
				      (void **)valArr,
				      nameArr,
				      size);
	       }
	       else
	       {
		    *status = DRMS_ERROR_OUTOFMEMORY;
	       }

	       if (nameArr != NULL)
	       {
		    free(nameArr);
	       }

	       if (valArr != NULL)
	       {
		    free(valArr);
	       }
	  }
     }

     return ret;
}

void drms_segment_destroyinfocon(HContainer_t **info)
{
   hcon_destroy(info);
}

/* Return absolute path to segment file in filename.
   filename must be able the hold at least DRMS_MAXPATHLEN bytes. */
void drms_segment_filename(DRMS_Segment_t *seg, char *filename)
{
   if (seg->info->protocol == DRMS_DSDS)
   {
      /* For the DSDS protocol, filename is not used. */
      if (filename)
      {
	 *filename = '\0';
      }
   }
   else if (seg->info->protocol != DRMS_LOCAL)
   {
      if (strlen(seg->filename)) {
	 if (seg->info->protocol == DRMS_TAS)
	   CHECKSNPRINTF(snprintf(filename, DRMS_MAXPATHLEN, "%s/%s",
				  seg->record->su->sudir, seg->filename), DRMS_MAXPATHLEN);
	 else
	   CHECKSNPRINTF(snprintf(filename, DRMS_MAXPATHLEN, "%s/" DRMS_SLOTDIR_FORMAT "/%s",
				  seg->record->su->sudir, seg->record->slotnum, seg->filename), DRMS_MAXPATHLEN);
      } else {
	 if (seg->info->protocol == DRMS_TAS)
	   CHECKSNPRINTF(snprintf(filename, DRMS_MAXPATHLEN, "%s/%s.tas",
				  seg->record->su->sudir, seg->info->name), DRMS_MAXPATHLEN);
	 else
	   CHECKSNPRINTF(snprintf(filename, DRMS_MAXPATHLEN, "%s/" DRMS_SLOTDIR_FORMAT "/%s.%s",
				  seg->record->su->sudir, seg->record->slotnum, seg->info->name,
				  drms_prot2ext(seg->info->protocol)), DRMS_MAXPATHLEN);
      }
   }
   /* for DRMS_LOCAL, filename is already set */
}

/* Delete the file storing the data of a segment. */  
int drms_delete_segmentfile (DRMS_Segment_t *seg) {
  char filename[DRMS_MAXPATHLEN];

  drms_segment_filename (seg, filename);
  if (unlink (filename)) {
    perror ("ERROR in drms_delete_segmentfile: unlink failed with");
    return DRMS_ERROR_UNLINKFAILED;
  } else return DRMS_SUCCESS;
}

/* Set segment scaling. Can only be done when creating a new segment. */
int drms_segment_setscaling(DRMS_Segment_t *seg, double bzero, double bscale)
{
  int status;
  char tmpstr[20];

  if (!seg->record->readonly)
  {
    sprintf(tmpstr,"bzero[%d]",seg->info->segnum);      
    status = drms_setkey_double(seg->record, tmpstr, bzero);
    if (!status)
    {
      sprintf(tmpstr,"bscale[%d]",seg->info->segnum);      
      status = drms_setkey_double(seg->record, tmpstr, bscale);
    }
    return status;
  }
  else
    return DRMS_ERROR_RECORDREADONLY;
}


/* Set segment scaling. Can only be done when creating a new segment. */
int drms_segment_getscaling(DRMS_Segment_t *seg, double *bzero, double *bscale)
{
  int status;
  char tmpstr[20];

  sprintf(tmpstr,"bzero[%d]",seg->info->segnum);      
  *bzero = drms_getkey_double(seg->record, tmpstr,&status);
  if (status)
    *bzero = 0.0;
  sprintf(tmpstr,"bscale[%d]",seg->info->segnum);      
  *bscale = drms_getkey_double(seg->record, tmpstr,&status);
  if (status)
    *bscale = 1.0;
  return status;
}

/*  Wrapper for __drms_segment_lookup without the recursion depth counter  */
DRMS_Segment_t *drms_segment_lookup (DRMS_Record_t *rec, const char *segname) {
  DRMS_Segment_t *seg = __drms_segment_lookup (rec, segname, 0);

  if (!seg) return NULL;
  if (seg->info->scope == DRMS_CONSTANT) {

    // If the recnum of the record where the constant segment is stored
    // has not yet been set, return the current segment (for writing
    // purpose), otherwise return the constant segment.
    if (!seg->info->cseg_recnum) {
      return seg;
    } else {
      if (seg->record->recnum == seg->info->cseg_recnum) {
	return seg;
      } else {
	int status = 0;
	DRMS_Record_t *rec2 = drms_retrieve_record(rec->env, rec->seriesinfo->seriesname, seg->info->cseg_recnum, NULL, &status);
	if (status) {
	  fprintf(stderr, "Failed to retrieve record for constant segment, recnum = %lld", seg->info->cseg_recnum);
	  return NULL;
	}
	return drms_segment_lookup(rec2, segname);
      }
    }
  } else return seg;
}

DRMS_Segment_t *drms_segment_lookupnum(DRMS_Record_t *rec, int segnum)
{
  DRMS_Segment_t *seg = hcon_index2slot(&rec->segments, segnum, NULL);
  // This is to properly handle link segment and constant segment.
  return drms_segment_lookup(rec, seg->info->name);
}



/*************************** Segment Data functions **********************/



/* Open an array data segment. 

   a) If the corresponding data file exists, read the 
   entire data array into memory. Convert it to the type given as 
   argument. If type=DRMS_TYPE_RAW then  the data is 
   read into an array of the same type it is stored as on disk.
   b) If the data file does not exist, then return a data array filed with 
   the MISSING value for the given type.

   The read functions do not apply bscale or bzero.  This conversion is
   performed at the end of the function (via drms_array_convert_inplace()).
*/  


DRMS_Array_t *drms_segment_read(DRMS_Segment_t *seg, DRMS_Type_t type, 
				int *status)
{
  int stat=0,i;
  DRMS_Array_t *arr = NULL;
  char filename[DRMS_MAXPATHLEN];
  int start[DRMS_MAXRANK+1], end[DRMS_MAXRANK+1];	
  FILE *fp;
  DRMS_Record_t *rec;
  double bzero, bscale;

  CHECKNULL_STAT(seg,status);
  
  rec = seg->record;



  if (seg->info->scope == DRMS_CONSTANT &&
      !seg->info->cseg_recnum) {
    fprintf(stderr, "ERROR in drms_segment_read: constant segment has not yet"
	    " been initialized. Series = %s.\n",  rec->seriesinfo->seriesname);
    stat = DRMS_ERROR_INVALIDACTION;
    goto bailout1;
  }

  if (seg->info->protocol == DRMS_GENERIC)
    {
    fprintf(stderr, "ERROR in drms_segment_read: Not appropriate function"
       "for DRMS_GENERIC segment.  Series = %s.\n",  rec->seriesinfo->seriesname);
    stat = DRMS_ERROR_INVALIDACTION;
    goto bailout1;
    }

  if (rec->sunum != -1LL && rec->su==NULL)
  {
    /* The storage unit has not been requested from SUMS yet. Do it. */
    if ((rec->su = drms_getunit(rec->env, rec->seriesinfo->seriesname, 
				rec->sunum, 1, &stat)) == NULL)
    {
      fprintf(stderr,"ERROR in drms_segment_read: Cannot read segment for "
	      "record with no storage unit slot.\nseries=%s, sunum=%lld\n",
	      rec->seriesinfo->seriesname, rec->sunum);
      goto bailout1;
    }
    rec->su->refcount++;
  }

  drms_segment_filename(seg, filename);
#ifdef DEBUG
  printf("Trying to open segment file '%s'.\n",filename);
#endif

  if (seg->info->protocol == DRMS_DSDS || seg->info->protocol == DRMS_LOCAL)
  {
     char *dsdsParams;
     int ds;
     int rn;
     char *locfilename;

     if (seg->info->protocol == DRMS_DSDS)
     {
	dsdsParams = (char *)malloc(sizeof(char) * kDSDS_MaxHandle);
	if (DSDS_GetDSDSParams(seg->record->seriesinfo, dsdsParams))
	{
	   fprintf(stderr, "Couldn't get DSDS keylist.\n");
	   goto bailout1;
	}

	ds = drms_getkey_int(seg->record, kDSDS_DS, &stat);
	rn = drms_getkey_int(seg->record, kDSDS_RN, &stat);

	locfilename = NULL;
     }
     else
     {
	dsdsParams = NULL;
	ds = -1;
	rn = -1;
	locfilename = strdup(seg->filename);
     }

     /* The DSDS and LOCAL protocols do not use SUMS.  Call into libdsds (if available) 
      * to obtain data. */
     static void *hDSDS = NULL;
     static int attempted = 0;

     if (!attempted && !hDSDS)
     {
	kDSDS_Stat_t dsdsstat;
	hDSDS = DSDS_GetLibHandle(kLIBDSDS, &dsdsstat);
	if (dsdsstat != kDSDS_Stat_Success)
	{
	   stat = DRMS_ERROR_CANTOPENLIBRARY;
	}

	attempted = 1;
     }

     if (hDSDS)
     {
	kDSDS_Stat_t dsdsStat;
	pDSDSFn_DSDS_segment_read_t pFn_DSDS_segment_read = 
	  (pDSDSFn_DSDS_segment_read_t)DSDS_GetFPtr(hDSDS, kDSDS_DSDS_SEGMENT_READ);
	pDSDSFn_DSDS_free_array_t pFn_DSDS_free_array = 
	  (pDSDSFn_DSDS_free_array_t)DSDS_GetFPtr(hDSDS, kDSDS_DSDS_FREE_ARRAY);

	if (pFn_DSDS_segment_read && pFn_DSDS_free_array)
	{
	   DRMS_Array_t *copy = NULL;
	   
	   if (stat == DRMS_SUCCESS)
	   {
	      arr = (*pFn_DSDS_segment_read)(dsdsParams, ds, rn, locfilename, &dsdsStat);
	   }
	   else
	   {
	      goto bailout1;
	   }

	   if (dsdsStat == kDSDS_Stat_Success)
	   {
	      /* Copy - the DSDS array should be freed by libdsds. */
	      long long datalen = drms_array_size(arr);
	      void *data = calloc(1, datalen);
	      if (data)
	      {
		 memcpy(data, arr->data, datalen);
		 copy = drms_array_create(arr->type, arr->naxis, arr->axis, data, &stat);
		 if (stat != DRMS_SUCCESS)
		 {
		    if (data)
		    {
		       free(data);
		    }
		    goto bailout;
		 }
	      }
	      else
	      {
		 stat = DRMS_ERROR_OUTOFMEMORY;
		 goto bailout;
	      }

	      copy->bzero = arr->bzero;
	      copy->bscale = arr->bscale;
	      copy->israw = arr->israw;

	      (*pFn_DSDS_free_array)(&arr);
	      arr = copy;

	      drms_segment_getscaling(seg, &bzero, &bscale);

	      if (!drms_segment_checkscaling(arr, bzero, bscale, NULL))
	      {
		 stat = 1;
		 goto bailout;
	      }
	   }
	   else
	   {
	      stat = DRMS_ERROR_LIBDSDS;
	      fprintf(stderr, "Error reading DSDS segment.\n");
	      goto bailout1;
	   }
	}
	else
	{
	   stat = DRMS_ERROR_LIBDSDS;
	   goto bailout1;
	}
     }
     else
     {
	fprintf(stdout, "Your JSOC environment does not support DSDS database access.\n");
	stat = DRMS_ERROR_NODSDSSUPPORT;
     }

     if (dsdsParams)
     {
	free(dsdsParams);
     }

     if (locfilename)
     {
	free(locfilename);
     }

  } /* protocols DRMS_DSDS || DRMS_LOCAL */
  else if ((fp = fopen(filename,"r")) == NULL)
  {
    /* No such file. Create a new array filled with MISSING. */
    if (type == DRMS_TYPE_RAW)
      arr = drms_array_create(seg->info->type, seg->info->naxis,seg->axis, NULL, status);
    else
      arr = drms_array_create(type, seg->info->naxis,seg->axis, NULL, status);
    drms_array2missing(arr);
    drms_segment_getscaling(seg, &arr->bzero, &arr->bscale);
    if (type == DRMS_TYPE_RAW)
    {
      arr->israw = 1;
    }
    else
      arr->israw = 0;
    /* Set information about mapping from parent segment to array. */ 
    for (i=0;i<arr->naxis;i++)
      arr->start[i] = 0;
    arr->parent_segment = seg;
    if (status)
      *status = DRMS_SUCCESS;
    return arr;
  }
  else
  {
    drms_segment_getscaling(seg, &bzero, &bscale);

    switch(seg->info->protocol)
    {
    case DRMS_GENERIC:
    case DRMS_MSI:
      fclose(fp);
      stat = DRMS_ERROR_NOTIMPLEMENTED;
      goto bailout1;
      break;
    case DRMS_BINARY:
      fclose(fp);
      XASSERT(arr = malloc(sizeof(DRMS_Array_t)));
      if ((stat = drms_binfile_read(filename, 0, arr)))
      {
	fprintf(stderr,"Couldn't read segment from file '%s'.\n",
		filename);      
	goto bailout1;
      }
      break;
    case DRMS_BINZIP:
      fclose(fp);
      XASSERT(arr = malloc(sizeof(DRMS_Array_t)));
      if ((stat = drms_zipfile_read(filename, 0, arr)))
      {
	fprintf(stderr,"Couldn't read segment from file '%s'.\n",
		filename);      
	goto bailout1;
      }
      break;
    case DRMS_FITZ:
    case DRMS_FITS:
      {
	 fclose(fp);

	 CFITSIO_IMAGE_INFO *info = NULL;
	 void *image = NULL;

	 /* Call Tim's function to read data */
	 if (cfitsio_read_file(filename, &info, &image, NULL) == CFITSIO_SUCCESS)
	 {
	    if (CreateDRMSArray(info, image, &arr))
	    {
	       fprintf(stderr,"Couldn't read segment from file '%s'.\n", filename);      
	       goto bailout1;
	    }

	    /* Check that values of BZERO, BSCALE in the file and in the DRMS
	       database agrees. */
	    if (!drms_segment_checkscaling(arr, bzero, bscale, filename))
	    {
	       stat = 1;
	       goto bailout;
	    }

	    /* Don't free image - arr has stolen it. */
	    cfitsio_free_these(&info, NULL, NULL);
	 }
	 else
	 {
	    fprintf(stderr,"Couldn't FITS file '%s'.\n", filename); 
	    stat = 1;
	    goto bailout1;
	 }
      }
      break;
    case DRMS_FITZDEPRECATED:
    case DRMS_FITSDEPRECATED:
      {
	int headlen;
	char *header;
	
	fclose(fp);
	if ((arr=drms_readfits(filename, 1, &headlen, &header, NULL)) == NULL)
	{
	  fprintf(stderr,"Couldn't read segment from file '%s'.\n",
		  filename);      
	  goto bailout1;
	}
	free(header);
	/* Check that value of BZERO, BSCALE in the file and in the DRMS
	   database agrees. */
	if (!drms_segment_checkscaling(arr, bzero, bscale, filename))
	{
	   stat = 1;
	   goto bailout;
	}
      }
      break;
    case DRMS_TAS:
      /* Read the slice in the TAS file corresponding to this record's 
	 slot. */
      for (i=0; i<seg->info->naxis; i++)
      {
	start[i] = 0;
	end[i] = seg->axis[i]-1;
      }
      start[seg->info->naxis] = seg->record->slotnum;
      end[seg->info->naxis] =  seg->record->slotnum;
      
      XASSERT(arr = malloc(sizeof(DRMS_Array_t)));
#ifdef DEBUG
      printf("segment_read: bzero = %f, bscale=%f\n",bzero,bscale);
#endif
      if ((stat = drms_tasfile_readslice(fp, type, bzero, bscale,  
					 seg->info->naxis+1, start, end, arr)))
      {
	fprintf(stderr,"Couldn't read segment from file '%s'.\n",
		filename);      
	goto bailout1;
      }
      arr->naxis -= 1;
      fclose(fp);
      //      drms_array_print(arr," ","\n");
      break;
    default:
      fclose(fp);
      stat = DRMS_ERROR_UNKNOWNPROTOCOL;
      goto bailout1;
    }
  }
  
  /* Check that dimensions match template. */
  if (seg->info->protocol != DRMS_TAS && arr->type != seg->info->type) {
    fprintf (stderr, "Data types in file (%d) do not match those in segment " 
	"descriptor (%d).\n", (int)arr->type, (int)seg->info->type);
    goto bailout;
  }
  if (arr->naxis != seg->info->naxis) {
    fprintf (stderr, "Number of axis in file (%d) do not match those in "
	    "segment descriptor (%d).\n", arr->naxis, seg->info->naxis);
    goto bailout;
  }
  for (i=0;i<arr->naxis;i++) {    
    if (arr->axis[i] != seg->axis[i]) {
      fprintf (stderr,"Dimension of axis %d in file (%d) do not match those"
	  " in segment descriptor (%d).\n", i, arr->axis[i], seg->axis[i]);
      goto bailout;
    }
  }

  /* Set information about mapping from parent segment to array. */ 
  for (i=0;i<arr->naxis;i++)
    arr->start[i] = 0;
  arr->parent_segment = seg;
  
  /* Scale and convert to desired type. */
  /* If this is protocol FITSIO, then arr->bzero == bzero. */
  arr->bzero = bzero; /* RHS is what the DRMS keyword says; LHS is what the FITS file says.  */
  arr->bscale = bscale;
  if (type == DRMS_TYPE_RAW)
  {
    arr->israw = 1;
  }
  else if (seg->info->protocol != DRMS_TAS && 
	   (arr->type != type || bscale != 1.0 || bzero != 0.0))
  {
    drms_array_convert_inplace(type, arr->bzero, arr->bscale, arr);
#ifdef DEBUG
    printf("converted with bzero=%g, bscale=%g\n",arr->bzero, arr->bscale);
#endif
    arr->israw = 0;    
  }

  if (status)
    *status = DRMS_SUCCESS;
  
  return arr;

 bailout:
  free(arr->data);
  free(arr);
 bailout1:
#ifdef DEBUG
  printf("Segment = \n");
  drms_segment_print(seg);
#endif
  if (status)
    *status = stat;
  return NULL;  
}



/* Open a data segment. 

   a) If the corresponding data file exists, read the 
   entire data array into memory. Convert it to the type given as 
   argument. If type=DRMS_TYPE_RAW then the data is 
   read into an array of the same type it is stored as on disk.
   b) If the data file does not exist, then return a data array filed with 
   the MISSING value for the given type.

   How does this differ from drms_segment_read

*/  
DRMS_Array_t *drms_segment_readslice(DRMS_Segment_t *seg, DRMS_Type_t type, 
				     int *start, int *end,   int *status)
{
  int stat=0,i;
  DRMS_Array_t *arr, *tmp;
  char filename[DRMS_MAXPATHLEN];
  FILE *fp;
  DRMS_Record_t *rec;
  double bzero, bscale;
  int start1[DRMS_MAXRANK+1], end1[DRMS_MAXRANK+1];	

  CHECKNULL_STAT(seg,status);
  
  rec = seg->record;
  if (rec->sunum != -1LL && rec->su==NULL)
  {
    /* The storage unit has not been requested from SUMS yet. Do it. */
    if ((rec->su = drms_getunit(rec->env, rec->seriesinfo->seriesname, 
				rec->sunum, 1, &stat)) == NULL)
    {
      fprintf(stderr,"ERROR in drms_segment_read: Cannot read for "
	      "record with no storage unit slot.\n");
      goto bailout1;
    }
    rec->su->refcount++;
  }  

  drms_segment_filename(seg, filename);
#ifdef DEBUG
  printf("Trying to open segment file '%s'.\n",filename);
#endif
  if ((fp = fopen(filename,"r")) == NULL)
  {
    stat = DRMS_ERROR_UNKNOWNSEGMENT;
    goto bailout1;
  }
  else
  {
    drms_segment_getscaling(seg, &bzero, &bscale);

    switch(seg->info->protocol)
    {
    case DRMS_BINARY:
      fclose(fp);
      XASSERT(arr = malloc(sizeof(DRMS_Array_t)));
      if ((stat = drms_binfile_read(filename, 0, arr)))
      {
	fprintf(stderr,"Couldn't read segment from file '%s'.\n",
		filename);      
	goto bailout1;
      }
      break;
    case DRMS_BINZIP:
      fclose(fp);
      XASSERT(arr = malloc(sizeof(DRMS_Array_t)));
      if ((stat = drms_zipfile_read(filename, 0, arr)))
      {
	fprintf(stderr,"Couldn't read segment from file '%s'.\n",
		filename);      
	goto bailout1;
      }
      break;
    case DRMS_TAS:
      /* Select the slice from the slot belonging to this record. */
      memcpy(start1, start, seg->info->naxis*sizeof(int));
      memcpy(end1, end, seg->info->naxis*sizeof(int));
      start1[seg->info->naxis] = seg->record->slotnum;
      end1[seg->info->naxis] = seg->record->slotnum;
      XASSERT(arr = malloc(sizeof(DRMS_Array_t)));
      if ((stat = drms_tasfile_readslice(fp, type, bzero, bscale, 
					 seg->info->naxis+1, start1, end1, arr)))
      {
	fprintf(stderr,"Couldn't read segment from file '%s'.\n",
		filename);      
	goto bailout1;
      }
      arr->naxis -= 1;
      fclose(fp);
      break;
    case DRMS_FITZ:
    case DRMS_FITS:
      {
	 fprintf(stderr, "Readslice not implemented for DRMS_FITZ and DRMS_FITS protocols.\n");
	 goto bailout;
      }
    case DRMS_GENERIC:
      fclose(fp);
      *status = DRMS_ERROR_INVALIDACTION;
      return NULL;    
      break;
    case DRMS_MSI:
      fclose(fp);
      *status = DRMS_ERROR_NOTIMPLEMENTED;
      return NULL;    
      break;
    case DRMS_FITZDEPRECATED:
    case DRMS_FITSDEPRECATED:
      {
      double abscale = fabs(bscale);
      int headlen;
      char *header;

      fclose(fp);
      if ((arr = drms_readfits (filename, 1, &headlen, &header, NULL)) == NULL)
      {
	fprintf (stderr, "Couldn't read segment from file '%s'.\n",
		  filename);      
	goto bailout1;
      }
      free(header);
	/*  Check that values of BZERO, BSCALE in the file and in the DRMS
	   						database agree  */
	if (fabs(arr->bzero - bzero) > 100 * DBL_EPSILON * abscale) {
	  fprintf(stderr,"BZERO and BSCALE in FITS file '%s' do not match those"
		  " in DRMS database.\n",filename);      
	  goto bailout;
	}
      break;
      }
    default:
      fclose(fp);
      if (status)
	*status = DRMS_ERROR_UNKNOWNPROTOCOL;
      return NULL;
    }
  }
				   /*  Check that dimensions match template  */
  if (arr->naxis != seg->info->naxis) {
    fprintf (stderr, "Number of axis in file (%d) do not match those in "
	"segment descriptor (%d).\n", arr->naxis, seg->info->naxis);
    goto bailout;
  }
  if (seg->info->protocol != DRMS_TAS) {
    if (arr->type != seg->info->type) {
      fprintf (stderr, "Data types in file (%d) do not match those in segment "
	  "descriptor (%d).\n", (int)arr->type, (int)seg->info->type);
      goto bailout;
    }
    for (i=0;i<arr->naxis;i++) {
      if (arr->axis[i] != seg->axis[i]) {
	fprintf(stderr,"Dimension of axis %d in file (%d) do not match those"
		" in segment descriptor (%d).\n",i,arr->axis[i], 
		seg->axis[i]);
	goto bailout;
      }
    }
  }
  arr->parent_segment = seg;
  for (i=0;i<arr->naxis;i++)
    arr->start[i] = start[i];
  arr->bzero = bzero;
  arr->bscale = bscale;

  /* Cut out the desired part of the array. */
  if (seg->info->protocol != DRMS_TAS)  
  {
    if ((tmp = drms_array_slice(start,end,arr)) == NULL)
    drms_free_array(arr);
    arr = tmp;
    if (!arr)
      goto bailout1;
    /* Scale and convert to desired type. */
    if (type != DRMS_TYPE_RAW )
    {
      if (arr->type != type || bscale != 1.0 || bzero != 0.0)
	drms_array_convert_inplace(type, arr->bzero, arr->bscale, arr);
    }
  }

  if (type == DRMS_TYPE_RAW )
    arr->israw=1;
  else
    arr->israw=0;

  
  if (status)
    *status = DRMS_SUCCESS;
  
  return arr;

 bailout:
  drms_free_array(arr);
 bailout1:
  if (status)
    *status = stat;
  return NULL;
}




/* Write the array argument to the file occupied by the
   segment argument. The array dimension and type must match the
   segment dimension and type. */
int drms_segment_write(DRMS_Segment_t *seg, DRMS_Array_t *arr, int autoscale)
{
  int status,i;
  char filename[DRMS_MAXPATHLEN]; 
  DRMS_Array_t *out;
  double bscale, bzero;
  int start[DRMS_MAXRANK+1]={0};
  FILE *fp;

  if (seg->info->scope == DRMS_CONSTANT &&
      seg->info->cseg_recnum) {
    fprintf(stderr, "ERROR in drms_segment_write: constant segment has already"
	    " been initialized. Series = %s.\n",  seg->record->seriesinfo->seriesname);
    return DRMS_ERROR_INVALIDACTION;
  }

  if (seg->info->protocol == DRMS_GENERIC)
    {
    fprintf(stderr, "ERROR in drms_segment_write: Not appropriate function"
       "for DRMS_GENERIC segment.  Series = %s.\n", seg->record->seriesinfo->seriesname);
    return(DRMS_ERROR_INVALIDACTION);
    }

  if (seg && arr)
  {
    if (seg->record->readonly)
    {
      fprintf(stderr,"Cannot write segment to read-only record.\n");
      return DRMS_ERROR_RECORDREADONLY;
    }
    if (arr->data == NULL)
    {
      fprintf(stderr,"Array contains no data!\n");
      return DRMS_ERROR_NULLPOINTER;
    }
	
    if (arr->naxis != seg->info->naxis)
    {
      fprintf(stderr,"Number of axis in file (%d) do not match those in "
	      "segment descriptor (%d).\n",arr->naxis,seg->info->naxis);
      return DRMS_ERROR_INVALIDDIMS;
    }
    for (i=0;i<arr->naxis;i++)
    {
      if (arr->axis[i] != seg->axis[i])
      {
	fprintf(stderr,"Dimension of axis %d in file (%d) do not match those"
		" in segment descriptor (%d).\n",i,arr->axis[i], 
		seg->axis[i]);
	return DRMS_ERROR_INVALIDDIMS;
      }
    }

    if (autoscale)
      drms_segment_autoscale(seg, arr);
      

    /* Normally, scale and convert to the format implied by the parent
       segment type, bzero and bscale. */
    drms_segment_getscaling(seg, &bzero, &bscale);
    if (arr->israw)
    {
      if (arr->bzero != bzero || arr->bscale != bscale)
      {
	/* Writing raw array to a segment with a different scaling. */
	bzero = (arr->bzero-bzero)/bscale;
	bscale = arr->bscale/bscale;
      }
      else
      {
        bzero = 0.0;
	bscale = 1.0;
      }
    }
    else if (fabs(bzero)!=0.0 || bscale!=1.0)
    {
      bzero = -bzero/bscale;
      bscale = 1.0/bscale;
    }

#ifdef DEBUG      
    printf("in write_segment:  bzero=%g, bscale = %g\n",bzero,bscale);
#endif
    /* Convert to desired type. */
    if( seg->info->protocol != DRMS_TAS  && 
	( arr->type != seg->info->type || fabs(bzero)!=0.0 || bscale!=1.0 ))
    {
      out = drms_array_convert(seg->info->type, bzero, bscale, arr);
      drms_segment_getscaling(seg, &out->bzero, &out->bscale);
      out->israw = 1;
    }
    else 
    {
      out = arr;
    }
    
    drms_segment_filename(seg, filename);
    if (!strlen(seg->filename)) {
      strncpy(seg->filename, rindex(filename, '/')+1, DRMS_MAXSEGFILENAME-1);
    }

    switch(seg->info->protocol)
    {
    case DRMS_BINARY:
      if ((status = drms_binfile_write(filename, out)))
	goto bailout;
      break;
    case DRMS_BINZIP:
      if ((status = drms_zipfile_write(filename, out)))
	goto bailout;
      break;
    case DRMS_FITZ:
      {
	 if (out->type == DRMS_TYPE_STRING)
	 {
	    fprintf(stderr, "Can't save string data into a fits file.\n");
	    goto bailout;
	 }

	 CFITSIO_IMAGE_INFO imginfo;

	 if (!SetImageInfo(out, &imginfo))
	 {
	    /* Need to change the compression parameter to something meaningful 
	     * (although new users should just use the DRMS_FITS protocol )*/
	    if (cfitsio_write_file(filename, &imginfo, out->data, seg->cparms, NULL))
	      goto bailout;
	 }
	 else
	 {
	    goto bailout;
	 }
      }
      break;
    case DRMS_FITS:
      {
	 if (out->type == DRMS_TYPE_STRING)
	 {
	    fprintf(stderr, "Can't save string data into a fits file.\n");
	    goto bailout;
	 }

	 CFITSIO_IMAGE_INFO imginfo;

	 if (!SetImageInfo(out, &imginfo))
	 {
	    /* */
	    if (cfitsio_write_file(filename, &imginfo, out->data, seg->cparms, NULL))
	      goto bailout;
	 }
	 else
	 {
	    goto bailout;
	 }
      }
      break;  
    case DRMS_MSI:
      return DRMS_ERROR_NOTIMPLEMENTED;
      break;   
    case DRMS_TAS:
      
#ifdef DEBUG      
      printf("Writing slice to '%s', bzero=%e, bscale=%e\n",
	     filename,bzero,bscale);      
      //      drms_array_print(out," ","\n");
#endif

      memset(start, 0, seg->info->naxis*sizeof(int));
      start[seg->info->naxis] = seg->record->slotnum;
      out->naxis = seg->info->naxis+1;
      out->axis[seg->info->naxis] = 1;
      if ((fp = fopen(filename,"r+"))==NULL)
      {
	status = DRMS_ERROR_IOERROR;
	goto bailout;
      }
      status = drms_tasfile_writeslice(fp, bzero, bscale, start, out);
      fclose(fp);
      out->naxis = seg->info->naxis;
      out->axis[seg->info->naxis] = 0;
      if (status)
	goto bailout;
      break;
    case DRMS_FITZDEPRECATED:
      if ((status = drms_writefits(filename, 1, 0, NULL, out)))
	goto bailout;
      break;
    case DRMS_FITSDEPRECATED:
      if ((status = drms_writefits(filename, 0, 0, NULL, out)))
	goto bailout;
      break; 
    default:
      return DRMS_ERROR_UNKNOWNPROTOCOL;
    }
  
    if (out!=arr)
      drms_free_array(out);

  if (seg->info->scope == DRMS_CONSTANT &&
      !seg->info->cseg_recnum) {
    if (seg->record->lifetime == DRMS_TRANSIENT) {
      fprintf(stderr, "Error: cannot set constant segment in a transient record\n");
      goto bailout;
    }
    return drms_segment_set_const(seg);
  }

  return DRMS_SUCCESS;
  }
  else
    return DRMS_ERROR_NULLPOINTER;

 bailout:
  if (out && out!=arr)
    drms_free_array(out);
  fprintf(stderr,"ERROR: Couldn't write segment to file '%s'.\n", filename);
  return status;
}

int drms_segment_write_from_file(DRMS_Segment_t *seg, char *infile) {
  char *filename;            /* filename without path */
  char outfile[DRMS_MAXPATHLEN];
  FILE *in, *out;            /* input and output file stream */
  size_t read_size;          /* number of bytes on last read */
  const unsigned int bufsize = 16*1024;
  char *buf = malloc(bufsize*sizeof(char)); /* buffer for data */

  if (seg->info->scope == DRMS_CONSTANT &&
      seg->info->cseg_recnum) {
    fprintf(stderr, "ERROR in drms_segment_write: constant segment has already"
	    " been initialized. Series = %s.\n",  seg->record->seriesinfo->seriesname);
    return DRMS_ERROR_INVALIDACTION;
  }

  if (seg->record->readonly) {
    fprintf(stderr, "ERROR in drms_segment_write_from_file: Can't use "
	    "on readonly segment\n");
    return DRMS_ERROR_RECORDREADONLY;
  }

  // check protocol 
  if (seg->info->protocol != DRMS_GENERIC)  {
    fprintf(stderr, "ERROR in drms_segment_write_from_file: Can't use "
	    "on non-DRMS_GENERIC segment.  Series = %s.\n", seg->record->seriesinfo->seriesname);
    return DRMS_ERROR_INVALIDACTION;
  }

  if ((in = fopen(infile, "r")) == NULL) {
    fprintf(stderr, "Error:Unable to open %s\n", infile);
    goto bailout;
  }
  // strip path from infile
  filename = rindex(infile, '/');
  if (filename) 
    filename++;
  else 
    filename = infile;

  CHECKSNPRINTF(snprintf(seg->filename, DRMS_MAXSEGFILENAME, "%s", filename), DRMS_MAXSEGFILENAME);
  drms_segment_filename(seg, outfile);
  if ((out = fopen(outfile, "w")) == NULL) {
    fprintf(stderr, "Error:Unable to open %s\n", outfile);
    goto bailout;
  }
  while (1) {
    read_size = fread(buf, 1, bufsize, in);
    if (ferror(in)) {
	 fprintf(stderr, "Error:Read error\n");
	 goto bailout1;
    }
    else if (read_size == 0) {
	 break;              /* end of file */
    }
    
    fwrite(buf, 1, read_size, out);
  }
  fclose(in);
  fclose(out); 
  free(buf);
  buf = NULL;
  
  if (seg->info->scope == DRMS_CONSTANT &&
      !seg->info->cseg_recnum) {

    if (seg->record->lifetime == DRMS_TRANSIENT) {
      fprintf(stderr, "Error: cannot set constant segment in a transient record\n");
      goto bailout;
    }
    return drms_segment_set_const(seg);
  }  

  return DRMS_SUCCESS;

 bailout1:
  unlink(outfile);
 bailout:
  if (buf) 
    free(buf);
  seg->filename[0] = '\0';
  return 1;
}

void drms_segment_setblocksize(DRMS_Segment_t *seg, int *blksz)
{
  memcpy(seg->blocksize,blksz,seg->info->naxis*sizeof(int));
}


void drms_segment_getblocksize(DRMS_Segment_t *seg, int *blksz)
{
  memcpy(blksz,seg->blocksize,seg->info->naxis*sizeof(int));
}
 

/* Set the scaling of the segment such that the values in the array
   can be stored in an (scaled) integer data segment without overflow.
*/
void drms_segment_autoscale(DRMS_Segment_t *seg, DRMS_Array_t *arr)
{
  int i, n, iscale;
  double outmin, outmax;
  double inmin, inmax;
  double val, bscale, bzero;

  switch(seg->info->type)
  {
  case DRMS_TYPE_CHAR: 
    outmin = (double)SCHAR_MIN+1;
    outmax = (double)SCHAR_MAX;
    break;
  case DRMS_TYPE_SHORT:
    outmin = (double)SHRT_MIN+1;
    outmax = (double)SHRT_MAX;
    break;
  case DRMS_TYPE_INT:  
    outmin = (double)INT_MIN+1;
    outmax = (double)INT_MAX;
    break;
  case DRMS_TYPE_LONGLONG:  
    outmin = (double)LLONG_MIN+1;
    outmax = (double)LLONG_MAX;
    break;
  case DRMS_TYPE_FLOAT:  
  case DRMS_TYPE_DOUBLE: 	
  case DRMS_TYPE_TIME: 
  case DRMS_TYPE_STRING: 
    drms_segment_setscaling(seg,0.0,1.0);
    return;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)seg->info->type);
    XASSERT(0);
  }

  n = drms_array_count(arr);
  /* Does the scaling preserve integers? */
  iscale = (trunc(arr->bscale)==arr->bscale && trunc(arr->bzero)==arr->bzero);
  bzero=0.0;
  bscale=1.0;
  switch(arr->type)
  {
  case DRMS_TYPE_CHAR: 
    if (arr->israw || CHAR_MAX>outmax || CHAR_MIN<outmin)
    {
      char *p = (char *)arr->data;
      inmin = (double) *p;
      inmax = (double) *p++;
      for (i=1; i<n; i++)
      {
	val = (double) *p++;
	if (val<inmin)
	  inmin = val;
	if (val>inmax)
	  inmax = val;
      }
      if (arr->israw)
      {
	inmax = arr->bscale*inmax + arr->bzero;
	inmin = arr->bscale*inmin + arr->bzero;
	/* If the existing scaling preserves integers and fits
	   in the target range, don't mess with it. */
	if ( iscale && inmax<=outmax && inmin>=outmin)
	{
	  bzero = arr->bzero;
	  bscale = arr->bscale;
	}
	else
	{
	  bzero = (inmax+inmin)/2;
	  bscale = (inmax-inmin)/(outmax-outmin);
	}
      }
      else 
      {
	if (inmax<=outmax && inmin>=outmin)
	{
	  bzero = 0.0;
	  bscale = 1.0;
	}
	else
	{
	  bzero = (inmax+inmin)/2;
	  bscale = (inmax-inmin)/(outmax-outmin);
	}
      }  
    }
    drms_segment_setscaling(seg,bzero,bscale);
    break;
  case DRMS_TYPE_SHORT:
    if (arr->israw || SHRT_MAX>outmax || SHRT_MIN<outmin)
    {
      short *p = (short *)arr->data;
      inmin = (double) *p;
      inmax = (double) *p++;
      for (i=1; i<n; i++)
      {
	val = (double) *p++;
	if (val<inmin)
	  inmin = val;
	if (val>inmax)
	  inmax = val;
      }
      if (arr->israw)
      {
	inmax = arr->bscale*inmax + arr->bzero;
	inmin = arr->bscale*inmin + arr->bzero;
	/* If the existing scaling preserves integers and fits
	   in the target range, don't mess with it. */
	if ( iscale && inmax<=outmax && inmin>=outmin)
	{
	  bzero = arr->bzero;
	  bscale = arr->bscale;
	}
	else
	{
	  bzero = (inmax+inmin)/2;
	  bscale = (inmax-inmin)/(outmax-outmin);
	}
      }
      else 
      {
	if (inmax<=outmax && inmin>=outmin)
	{
	  bzero = 0.0;
	  bscale = 1.0;
	}
	else
	{
	  bzero = (inmax+inmin)/2;
	  bscale = (inmax-inmin)/(outmax-outmin);
	}
      }  
    }
    drms_segment_setscaling(seg,bzero,bscale);
    break;
  case DRMS_TYPE_INT:  
    if (arr->israw || INT_MAX>outmax || INT_MIN<outmin)
    {
      int *p = (int *)arr->data;
      inmin = (double) *p;
      inmax = (double) *p++;
      for (i=1; i<n; i++)
      {
	val = (double) *p++;
	if (val<inmin)
	  inmin = val;
	if (val>inmax)
	  inmax = val;
      }
      if (arr->israw)
      {
	inmax = arr->bscale*inmax + arr->bzero;
	inmin = arr->bscale*inmin + arr->bzero;
	/* If the existing scaling preserves integers and fits
	   in the target range, don't mess with it. */
	if ( iscale && inmax<=outmax && inmin>=outmin)
	{
	  bzero = arr->bzero;
	  bscale = arr->bscale;
	}
	else
	{
	  bzero = (inmax+inmin)/2;
	  bscale = (inmax-inmin)/(outmax-outmin);
	}
      }
      else 
      {
	if (inmax<=outmax && inmin>=outmin)
	{
	  bzero = 0.0;
	  bscale = 1.0;
	}
	else
	{
	  bzero = (inmax+inmin)/2;
	  bscale = (inmax-inmin)/(outmax-outmin);
	}
      }  
    }
    drms_segment_setscaling(seg,bzero,bscale);
    break;
  case DRMS_TYPE_LONGLONG:  
    if (arr->israw || LLONG_MAX>outmax || LLONG_MIN<outmin)
    {
      long long *p = (long long *)arr->data;
      inmin = (double) *p;
      inmax = (double) *p++;
      for (i=1; i<n; i++)
      {
	val = (double) *p++;
	if (val<inmin)
	  inmin = val;
	if (val>inmax)
	  inmax = val;
      }
      if (arr->israw)
      {
	inmax = arr->bscale*inmax + arr->bzero;
	inmin = arr->bscale*inmin + arr->bzero;
	/* If the existing scaling preserves integers and fits
	   in the target range, don't mess with it. */
	if ( iscale && inmax<=outmax && inmin>=outmin)
	{
	  bzero = arr->bzero;
	  bscale = arr->bscale;
	}
	else
	{
	  bzero = (inmax+inmin)/2;
	  bscale = (inmax-inmin)/(outmax-outmin);
	}
      }
      else 
      {
	if (inmax<=outmax && inmin>=outmin)
	{
	  bzero = 0.0;
	  bscale = 1.0;
	}
	else
	{
	  bzero = (inmax+inmin)/2;
	  bscale = (inmax-inmin)/(outmax-outmin);
	}
      }  
    }
    drms_segment_setscaling(seg,bzero,bscale);
    break;
  case DRMS_TYPE_FLOAT: 
    if (!arr->israw && (seg->info->type==DRMS_TYPE_DOUBLE || seg->info->type==DRMS_TYPE_FLOAT))
      drms_segment_setscaling(seg,0.0,1.0);
    else
    {
      float *p = (float *)arr->data;
      inmin = (double) *p;
      inmax = (double) *p++;
      for (i=1; i<n; i++)
      {
	val = (double) *p++;
	if (val<inmin)
	  inmin = val;
	if (val>inmax)
	  inmax = val;
      }
      if (arr->israw)
      {
	inmax = arr->bscale*inmax + arr->bzero;
	inmin = arr->bscale*inmin + arr->bzero;
      }
      bzero = (inmax+inmin)/2;
      bscale = (inmax-inmin)/(outmax-outmin);
      drms_segment_setscaling(seg,bzero,bscale);
    }
    break;
  case DRMS_TYPE_DOUBLE: 	
  case DRMS_TYPE_TIME: 
    if (!arr->israw && (seg->info->type==DRMS_TYPE_DOUBLE))
      drms_segment_setscaling(seg,0.0,1.0);
    else
    {
      double *p = (double *)arr->data;
      inmin = (double) *p;
      inmax = (double) *p++;
      for (i=1; i<n; i++)
      {
	val = (double) *p++;
	if (val<inmin)
	  inmin = val;
	if (val>inmax)
	  inmax = val;
      }
      if (arr->israw)
      {
	inmax = arr->bscale*inmax + arr->bzero;
	inmin = arr->bscale*inmin + arr->bzero;
      }
      bzero = (inmax+inmin)/2;
      bscale = (inmax-inmin)/(outmax-outmin);
      drms_segment_setscaling(seg,bzero,bscale);
    }
    break;
  case DRMS_TYPE_STRING: 
    drms_segment_setscaling(seg,0.0,1.0);
    return;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)arr->type);
    XASSERT(0);
  }
}

int drms_segment_segsmatch(const DRMS_Segment_t *s1, const DRMS_Segment_t *s2)
{
   int ret = 1;

   if (s1 && s2)
   {
      int nDims = s1->info->naxis;
      if (nDims == s2->info->naxis)
      {
	 int i = 0;
	 for (; i < nDims; i++)
	 {
	    if (s1->axis[i] != s2->axis[i])
	    {
	       ret = 0;
	       break;
	    }
	 }

	 if (s1->info->protocol == DRMS_TAS && s2->info->protocol == DRMS_TAS)
	 {
	    for (i = 0; ret == 1 && i < nDims; i++)
	    {
	       if (s1->blocksize[i] != s2->blocksize[i])
	       {
		  ret = 0;
		  break;
	       }
	    }
	 }
      }
      else
      {
	 ret = 0;
      }

      if (ret == 1)
      {
	 if ((s1->info->type != s2->info->type) ||
	     (s1->info->protocol != s2->info->protocol) ||
	     (s2->info->scope != s2->info->scope))
	 {
	    ret = 0;
	 }
      }
   }
   else if (s1 || s2)
   {
      ret = 0;
   }

   return ret;
}

int drms_segment_export_tofile(DRMS_Segment_t *seg, const char *fileout)
{
   return drms_segment_mapexport_tofile(seg, NULL, NULL, fileout);
}

int drms_segment_mapexport_tofile(DRMS_Segment_t *seg, 
				  const char *clname, 
				  const char *mapfile,
				  const char *fileout)
{
   int stat = DRMS_SUCCESS;

   CFITSIO_KEYWORD *fitskeys = NULL;
   HContainer_t *keys = &(seg->record->keywords);
   HIterator_t *hit = hiter_create(keys);

   if (hit)
   {
      DRMS_Keyword_t *key = NULL;
      const char *keyname = NULL;
      char segnum[4];

      snprintf(segnum, sizeof(segnum), "%d", seg->info->segnum);

      while ((key = hiter_getnext(hit)) != NULL)
      {
	 keyname = drms_keyword_getname(key);

	 if (drms_keyword_getperseg(key))
	 {
	    /* Ensure that this keyword is relevant to this segment. */
	    if (!strstr(keyname, segnum))
	    {
	       continue;
	    }
	 }
	    
	 if (drms_keyword_mapexport(key, clname, mapfile, &fitskeys))
	 {
	    fprintf(stderr, "Couldn't export keyword '%s'.\n", keyname);
	    stat = DRMS_ERROR_EXPORT;
	 }
      }

      hiter_destroy(&hit);
   }
   
   if (!stat)
   {
      switch (seg->info->protocol)
      {
	 case DRMS_FITS:
	   {
	      DRMS_Array_t *arrout = drms_segment_read(seg, DRMS_TYPE_RAW, &stat);
	      if (arrout)
	      {
		 /* Need to manually add required keywords that don't exist in the record's 
		  * DRMS keywords. */
		 CFITSIO_IMAGE_INFO imginfo;
      
		 if (!SetImageInfo(arrout, &imginfo))
		 {
		    /* Not sure if data need to be scaled, or if the original blank value
		     * should be resurrected. */
		    if (arrout->type == DRMS_TYPE_STRING)
		    {
		       fprintf(stderr, "Can't save string data into a fits file.\n");
		       stat = DRMS_ERROR_EXPORT;
		    }
		    else
		    {
		       if (cfitsio_write_file(fileout, &imginfo, arrout->data, seg->cparms, fitskeys))
		       {
			  fprintf(stderr, "Can't write fits file '%s'.\n", fileout);
			  stat = DRMS_ERROR_EXPORT;
		       }
		    }
		 }
		 else
		 {
		    fprintf(stderr, "Data array being exported is invalid.\n");
		    stat = DRMS_ERROR_EXPORT;
		 }

		 drms_free_array(arrout);
	      }
	   }
	   break;
	 default:
	   fprintf(stderr, 
		   "Data export does not support data segment protocol '%s'.\n", 
		   drms_prot2str(seg->info->protocol));
      }
   }

   if (fitskeys)
   {
      cfitsio_free_keylist(&fitskeys);
   }

   return stat;
}

/* recout is the export series' output record (which was cloned from the original). */
int drms_segment_export(DRMS_Record_t *recout,
			DRMS_Segment_t *segin)
{
   return drms_segment_mapexport(recout, segin, NULL, NULL);
}

/* recout is the export series' output record (which was cloned from the original). */
int drms_segment_mapexport(DRMS_Record_t *recout,
			   DRMS_Segment_t *segin,
			   const char *clname,
			   const char *mapfile)
{
   int status = DRMS_SUCCESS;
   DRMS_Segment_t *segout = NULL;
   char filein[DRMS_MAXPATHLEN];
   char fileout[DRMS_MAXPATHLEN];
   char *basename = NULL;
   unsigned long long tsize = 0;
   struct stat filestat;
   char dir[DRMS_MAXPATHLEN];

   if (recout)
   {
      segout = drms_segment_lookupnum(recout, 0);
   }
   else
   {
      fprintf(stderr, "Must provide export series record to export data to.\n");
      status = DRMS_ERROR_EXPORT;  
   }

   if (segout)
   {
      drms_record_directory(segin->record, dir, 1); /* This fetches the input data from SUMS. */
      drms_segment_filename(segin, filein); /* input seg file name */

      if (!stat(filein, &filestat))
      {
	 tsize = filestat.st_size;
	 basename = rindex(filein, '/');
	 if (basename) 
	 {
	    basename++;
	 }
	 else 
	 {
	    basename = filein;
	 }

	 CHECKSNPRINTF(snprintf(segout->filename, DRMS_MAXSEGFILENAME, "%s", basename), DRMS_MAXSEGFILENAME);
	 drms_segment_filename(segout, fileout);

	 status = drms_segment_mapexport_tofile(segin, clname, mapfile, fileout);

	 if (status == DRMS_SUCCESS)
	 {
	    /* Set output record keywords. */
	    drms_setkey_time(recout, gExpStr[kExport_ExpTime].str, CURRENT_SYSTEM_TIME);
	    drms_setkey_int(recout, gExpStr[kExport_DataSize].str, tsize);
	    drms_setkey_int(recout, gExpStr[kExport_Status].str, 1);
	 }
      }
      else
      {
	 fprintf(stderr, "Unable to open source file '%s'.\n", filein);
	 status = DRMS_ERROR_EXPORT;
      }
   }
   else
   {
      fprintf(stderr, "Series '%s' contains no data segments.\n", kEXPORTSERIES);
      status = DRMS_ERROR_EXPORT;
   }

   return status;
}
