/*
 *  v2helio.c                            ~soi/(version)/src/modules/v2helio.c
 *
 *  This module interpolates CCD velocity data to estimate values that
 *    would be obtained at specified equal increments of heliographic 
 *    longitude and sine of latitude.  Apodization and corrections for
 *    solar rotation and limbshift are included. 
 *
 *  Responsible:  Kay Leibrand                   KLeibrand@solar.Stanford.EDU
 *
 *  Bugs:
 *    This module is under development.  Look for ??? in comments.
 *
 *  Planned updates:
 *    Decide when and where and if default values should be used.
 *
 *  Revision history is at end of file.
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <sys/time.h>
#include "jsoc_main.h"
#include "astro.h"
#include "drms_dsdsapi.h"

#define PI		(M_PI)
#define RADSINDEG 	(PI/180)
#define ARCSECSINRAD 	(3600*180/PI)
#define DAYSINYEAR	(365.2425)
#define SECSINDAY	(86400)
#define TAU_A		(499.004782)	/* light time for unit dist, secs/au */
#define TCARR		(25.38)		/* days */
#define RTRUE		(6.96000000e8)	/* meters */
#define AU		(1.49597870e11)	/* meters/au */
#define MAXLEN		(256)
#define NO_DATASET	(-1)
#define NO_IMAGE	(-1)
#define kMAX_SKIPERRMSG  1024
#define kMAXROWS        65536

/* cmd-line parameters */
#define kRecSetIn       "recsin"
#define kSeriesOut      "dsout"
#define kCarrStretch    "CARRSTRETCH"
#define kMAXMISSVALS    "MAXMISSVALS"
#define kAPODIZE        "APODIZE"
#define kAPINNER        "APINNER"
#define kAPWIDTH        "APWIDTH"
#define kAPEL           "APEL"
#define kAPX            "APX"
#define kAPY            "APY"
#define kLGSHIFT        "LGSHIFT"
#define kMCORLEV        "MCORLEV"
#define kMOFFSET        "MOFFSET"
#define kVCORLEV        "VCORLEV"
#define kINTERPO        "INTERPO"
#define kDATASIGN       "DATASIGN"
#define kMAPRMAX        "MAPRMAX"
#define kREF_L0         "REF_L0"
#define kOUTTYPE        "OUTTYPE"
#define kOUTSCALE       "OUTSCALE"
#define kOUTBIAS        "OUTBIAS"
#define kSOLAR_P        "SOLAR_P"
#define kPSIGN          "PSIGN"
#define kPERR           "PERR"
#define kIERR           "IERR"
#define kCHECKO_FLAG    "o"
#define kRMAX_FLAG      "z"
#define kREF_T0         "REF_T0"
#define kMAPMMAX        "MAPMMAX"
#define kMAPLGMAX       "MAPLGMAX"
#define kMAPLGMIN       "MAPLGMIN"
#define kSINBDIVS       "SINBDIVS"
#define kMAPBMAX        "MAPBMAX"
#define kDISTORT        "DISTORT"
#define kCUBIC          "CUBIC"
#define kTILTALPHA      "TILTALPHA"
#define kTILTBETA       "TILTBETA"
#define kTILTFEFF       "TILTFEFF"
#define kDEBUGLOGFLAG   "d"

/* Keywords in the input data */
#define kT_OBS          "T_OBS"
#define kMISSVALS       "MISSVALS"
#define kI_DREC         "I_DREC"
#define kT_REC          "T_REC"
#define kQUALITY        "QUALITY"
#define kOBS_B0         "OBS_B0"
#define kOBS_L0         "OBS_L0"
#define kOBS_CR         "OBS_CR"
#define kS_MAJOR        "S_MAJOR"
#define kS_MINOR        "S_MINOR"
#define kS_ANGLE        "S_ANGLE"
#define kIM_SCALE       "IM_SCALE"
#define kX_SCALE        "X_SCALE"
#define kY_SCALE        "Y_SCALE"
#define kOBS_VR         "OBS_VR"
#define kOBS_VW         "OBS_VW"
#define kOBS_VN         "OBS_VN"
#define kORIENT         "ORIENT"
#define kOBS_DIST       "OBS_DIST"
#define kR_SUN          "R_SUN"
#define kXSCALE         "XSCALE"
#define kYSCALE         "YSCALE"
#define kX0             "X0"
#define kY0             "Y0"

/* Keywords in the output data */
#define kSN             "SN"
#define kPIXELS         "PIXELS"
#define kMAPCOLS        "MAPCOLS"
#define kMAPROWS        "MAPROWS"
#define kMAPBMIN        "MAPBMIN"
#define kSINBDELT       "SINBDELT"
#define kSHIFTFLG       "SHIFTFLG"
#define kLSHIFT         "LSHIFT"
#define kMAP_L0         "MAP_L0"
#define kBFITZERO       "BFITZERO"
#define DPC             "DPC"
#define DPC_STR         "DPC_STR"
#define DPC_OBSR        "DPC_OBSR"
#define DPC_FORM        "DPC_FORM"
#define DPC_CROP        "DPC_CROP"
#define DPC_ORGN        "DPC_ORGN"
#define DPC_RATE        "DPC_RATE"
#define DPC_SMPL        "DPC_SMPL"
#define DPC_CONF        "DPC_CONF"
#define BLDVER00        "BLDVER00"
#define BLDVER10        "BLDVER10"
#define BLDVER15        "BLDVER15"
#define BLDVER18        "BLDVER18"
#define FDRADIAL        "FDRADIAL"
#define CARSTRCH        "CARSTRCH"
#define DIFROT_A        "DIFROT_A"
#define DIFROT_B        "DIFROT_B"
#define DIFROT_C        "DIFROT_C"

typedef enum
{
   V2HStatus_Success,
   V2HStatus_MissingParameter,
   V2HStatus_IllegalTimeFormat,
   V2HStatus_TimeConvFailed,
   V2HStatus_Unimplemented,
   V2HStatus_IllegalOrientation
} V2HStatus_t;

char *module_name = "v2helio";

ModuleArgs_t module_args[] = 
{
   {ARG_STRING,  kRecSetIn, "", "Input data records."},
   {ARG_STRING,  kSeriesOut, "", "Output data series."},
   {ARG_FLOAT,   kMAPRMAX,  "0.95", "", ""},
   {ARG_INT,     kMAPMMAX,  "1536", "", ""},	/* determines mapcols */
						/* default value is 3*512 */
   {ARG_FLOAT,   kMAPLGMAX, "72.0", "", ""},	/* degrees */    
   {ARG_FLOAT,   kMAPLGMIN, "-72.0", "", ""},
   {ARG_FLOAT,   kMAPBMAX,  "72.0", "", ""},  
   {ARG_INT,     kSINBDIVS, "512", "", ""},	/* # of = increments in sinB */
						/* from sin(0) to sin(PI/2) */
   {ARG_STRING,  kREF_T0,   "1987.01.03_17:31:12", "", ""},
   {ARG_FLOAT,   kREF_L0,   "0.0", "", ""},

   {ARG_FLOAT,   kSOLAR_P,  "999.0", "", ""},	/* can't use D_MISSING here */
   {ARG_FLOAT,   kPSIGN,    "1.0", "", ""},	/* Sign of P. For MWO data. */
   {ARG_FLOAT,   kPERR,     "0.0", "", ""},	/* Fixed P-angle error. Maybe -0.22. */
   {ARG_FLOAT,   kIERR,     "0.0", "", ""},	/* Error in Carrington inclination. Maybe -0.10. */

   {ARG_INT,     kINTERPO,  "1", "0", "1"},	/* 2 methods - see soi_fun.h */
   {ARG_INT,     kAPODIZE,  "0", "", ""},	/* see soi_fun.h or apodize.c */
   {ARG_FLOAT,   kAPINNER,  "0.90", "", ""},	/* start of apodization */
   {ARG_FLOAT,   kAPWIDTH,  "0.05", "", ""},	/* width of apodization */
   {ARG_INT,     kAPEL,     "0", "", ""},	/* do elliptical apodization */
						/* described by apx and apy */
   {ARG_FLOAT,   kAPX,      "1.00", "", ""},	/* divide the x position by this before applying apodization */
   {ARG_FLOAT,   kAPY,      "1.00", "", ""},	/* divide the y position by this before applying apodization */
   {ARG_INT,     kLGSHIFT,  "0", "0", "2"}, 	/* 0=none; 1=fixed rate; 2=nearest Degree */
   {ARG_INT,     kVCORLEV,  "2", "0", "2"}, 	/* 3 levels - see soi_fun.h*/
   {ARG_INT,     kMCORLEV,  "0", "0", "1"}, 	/* 2 levels - see soi_fun.h*/
   {ARG_INT,     kMOFFSET,  "0", "0", "1"}, 	/* 1=apply BFITZERO correction*/
   {ARG_STRING,  kOUTTYPE,  "no scaling", "", ""},  /* bits in scaled output */
   {ARG_FLOAT,   kOUTSCALE, "1.0", "", ""},    /* scale for output */
   {ARG_FLOAT,   kOUTBIAS,  "0.0", "", ""},    /* bias for scaled output */
   {ARG_INT,     kDISTORT,  "0", "0", "1000"}, /* 0 for none, 1 for FD, 2 for vw */
   {ARG_FLOAT,   kCUBIC,    "7.06E-9", "", ""}, /* Cubic distortion in FD units */
   {ARG_FLOAT,   kTILTALPHA,"2.59", "", ""}, /* TILT of CCD in degrees */
   {ARG_FLOAT,   kTILTBETA, "56.0", "", ""}, /* Direction of TILT in degrees */
   {ARG_FLOAT,   kTILTFEFF, "12972.629", "", ""}, /* Effective focal length */
   {ARG_FLAG,    kCHECKO_FLAG,  "0", "-1", "1"},  /* Non 0 skips checko (SESW assumed) */
   {ARG_FLAG,    kRMAX_FLAG,    "0", "-1", "1"},  /* Non 0 sets data outside RMAX MISSING */
   {ARG_FLAG,    kDEBUGLOGFLAG, "0", "-1", "1"},  /* debug messages */
   {ARG_INT,     kDATASIGN,  "0", "-1", "1"}, 	/* Non 0 forces datasigh to value*/
   {ARG_INT,     kMAXMISSVALS, "0", "", "0"},  /* max. allowed MISSING pixels */
   {ARG_INT,     kCarrStretch, "0", "0", ""},  /* 0 - don't correct for diff rot, 1 - correct */
   {ARG_FLOAT,   DIFROT_A,   "13.562", "", ""}, /* A coefficient in diff rot adj (offset) */
   {ARG_FLOAT,   DIFROT_B,   "-2.04", "", ""},  /* B coefficient (to sin(lat) ^ 2) */
   {ARG_FLOAT,   DIFROT_C,   "-1.4875", "", ""},  /* c coefficient (to sin(lat) ^ 4) */
   {ARG_END,     "", "", "", ""}
};

HContainer_t *gParamCont = NULL;
long long gGUID = 0;

static double V2Hstr2Time(const char *timestr, V2HStatus_t *stat); 
static void CheckO(const char *orientation, V2HStatus_t *stat);

static void CleanParamMem()
{
   if (gParamCont)
   {
      hcon_destroy(&gParamCont);
   }
}

static void FreeParam(const void *v)
{
   /* v is a pointer to char * */
   void **vv = (void **)v;
   if (vv && *vv)
   {
      free(*vv);
   }
}

static void InsertParam(const char *keyn, const void *val)
{
   char buf[DRMS_MAXHASHKEYLEN];

   if (!gParamCont)
   {
      gParamCont = hcon_create(sizeof(void *), 
			       DRMS_MAXHASHKEYLEN, 
			       FreeParam, 
			       NULL, 
			       NULL, 
			       NULL, 
			       NULL);
   }

   if (gParamCont)
   {
      if (hcon_lookup(gParamCont, keyn))
      {
	 snprintf(buf, sizeof(buf), "%s%lld", keyn, gGUID);
	 gGUID++;
	 hcon_insert(gParamCont, buf, val);
      }
      else
      {
	 hcon_insert(gParamCont, keyn, val);
      }
   }
}

static const char *V2HgetKeyStr(DRMS_Record_t *rec, const char *keyn, int *status)
{
   void *mem = (void *)drms_getkey_string(rec, keyn, status);

   if (mem)
   {
      InsertParam(keyn, mem);
   }

   return (const char *)mem;
}

static const char *V2HparamsGetStr(CmdParams_t *params, char *name, int *status)
{
   void *mem = (void *)cmdparams_get_str(params, name, status);

   if (mem)
   {
      InsertParam(name, mem);
   }

   return (const char *)mem;
}

/* Non-fatal errors - print out error value */
static inline void SkipErr(int status, DRMS_Record_t *rec, char *msg, ...)
{
   /* logs error */
   char buf[kMAX_SKIPERRMSG];
   va_list valist;
   int ds;
   int rn;

   va_start(valist, msg);
   vsnprintf(buf, sizeof(buf), msg, valist);
   va_end(valist);
   ds = drms_getkey_int(rec, kDSDS_DS, NULL);
   rn = drms_getkey_int(rec, kDSDS_RN, NULL);
   fprintf(stderr, "*** warning %d: %s (skipping record ds=%d, rn=%d)\n", status, buf, ds, rn);
}

static inline void ParameterDef(int status, 
				char *pname, 
				double defaultp, 
				double *p, 
				DRMS_Record_t *rec)
{
   /* logs warning and sets parameter to default value */
   if (status != 0)
   {
      *p = defaultp;
      int ds =  drms_getkey_int(rec, kDSDS_DS, NULL);
      int rn =  drms_getkey_int(rec, kDSDS_RN, NULL); 

      fprintf(stderr, "warning: default value %g used for %s (record ds=%d, rn=%d)\n", 
	      defaultp, 
	      pname, 
	      ds, 
	      rn);
   }
}

/* Fatal errors - logs error and returns 1 to indicate abort */
#define PARAMETER_ERROR(CHKERR, ERROR, REC, PNAME)                  \
  if (CHKERR)                                                       \
  {                                                                 \
    int dsint = -1;                                                 \
    int rnint = -1;                                                 \
    if (REC)                                                        \
    {                                                               \
       dsint = drms_getkey_int(REC, kDSDS_DS, NULL);                \
       rnint = drms_getkey_int(REC, kDSDS_RN, NULL);                \
    }                                                               \
    fprintf(stderr,                                                 \
	    "fatal error %d: parameter %s (record ds=%d, rn=%d)\n", \
	    (int)ERROR,                                             \
            PNAME,                                                  \
  	    dsint,                                                  \
	    rnint);                                                 \
    CleanParamMem();                                                \            
    return 1;                                                       \
  }

/* Segment will be empty, but there will be a record! */
static void CreateBlankRecord(DRMS_Record_t *inrec, DRMS_Record_t *outrec, const char *tobsstr)
{
  /* create 'blank' data */
   drms_copykey(outrec, inrec, kI_DREC);
   drms_copykey(outrec, inrec, kT_REC);
   drms_copykey(outrec, inrec, kQUALITY);
   drms_setkey_string(outrec, kT_OBS, tobsstr);
}

int DoIt(void)
{
   int status = DRMS_SUCCESS;
   int error = 0;

   const char *tobsstr, *trefstr, *orientation;
   int ds;
   int rn;
   int paramsign;
   int longitude_shift, velocity_correction, interpolation, apodization;
   int mag_correction;
   int mag_offset;
   int row;
   int mapped_lmax, sinb_divisions, mapcols, maprows, nmax, nmin;
   int length[2];
   int carrStretch = 0;
   float diffrotA = 0.0;
   float diffrotB = 0.0;
   float diffrotC = 0.0;
   const char *outtypeStr = NULL;
   DRMS_Type_t outtype = DRMS_TYPE_FLOAT;
   V2HStatus_t vret = V2HStatus_Success;
   double tobs, tmearth, tref;
   double smajor, sminor, sangle;
   double xscale, yscale, imagescale, xs;
   int xpixels, ypixels, pixels;
   double obs_vr, obs_vw, obs_vn, vsign;
   double b0, bmax, bmin, sinBdelta;
   double longmax, longmin, longmax_adjusted, longmin_adjusted, longinterval;
   double p0, p, rmax;
   double ierr, perr, psign;
   double x0, y0;
   double obsdist, longshift, obsl0, refl0, mapl0, longrate, rtrue, rsun, S;
   double rsunDef;
   int obsCR;
   int apel;
   double apinner, apwidth, apx, apy;
   double scale, bias;
   double colsperdeg;
   LIBASTRO_RotRate_t rRates[kMAXROWS];

   int skip_checko = cmdparams_isflagset(&cmdparams, kCHECKO_FLAG);
   int debugLog = cmdparams_isflagset(&cmdparams, kDEBUGLOGFLAG);
   int NaN_beyond_rmax = cmdparams_isflagset(&cmdparams, kRMAX_FLAG);
   int maxmissvals = cmdparams_get_int(&cmdparams, kMAXMISSVALS, &status);

   LIBASTRO_Dist_t distP;
   DRMS_Record_t *outrec = NULL;
   DRMS_Segment_t *segin = NULL;
   DRMS_Array_t *inarr = NULL;
   DRMS_Array_t *outarr = NULL;

   struct timeval tv0;
   struct timeval tv;

   /* read Carrington coordinate correction for differential sun rotation */
   carrStretch = cmdparams_get_int(&cmdparams, kCarrStretch, &status);
   diffrotA = cmdparams_get_float(&cmdparams, DIFROT_A, &status);
   diffrotB = cmdparams_get_float(&cmdparams, DIFROT_B, &status);
   diffrotC = cmdparams_get_float(&cmdparams, DIFROT_C, &status);

   longrate = 360.0 / TCARR - 360.0 / DAYSINYEAR; /* degrees per day */
   longrate /= SECSINDAY; /* degrees per sec */
   rtrue = RTRUE/AU;  /* au */

   apodization = cmdparams_get_int(&cmdparams, kAPODIZE, &status);
   apinner = cmdparams_get_double(&cmdparams, kAPINNER, &status);
   apwidth = cmdparams_get_double(&cmdparams, kAPWIDTH, &status);
   apel = cmdparams_get_int(&cmdparams, kAPEL, &status);
   apx = cmdparams_get_double(&cmdparams, kAPX, &status);
   apy = cmdparams_get_double(&cmdparams, kAPY, &status);
   longitude_shift = cmdparams_get_int(&cmdparams, kLGSHIFT, &status);
   mag_correction = cmdparams_get_int(&cmdparams, kMCORLEV, &status);
   mag_offset = cmdparams_get_int(&cmdparams, kMOFFSET, &status);
   velocity_correction = cmdparams_get_int(&cmdparams, kVCORLEV, &status);
   interpolation = cmdparams_get_int(&cmdparams, kINTERPO, &status);
   paramsign = cmdparams_get_int(&cmdparams, kDATASIGN, &status);
   rmax = cmdparams_get_double(&cmdparams, kMAPRMAX, &status);
   refl0 = cmdparams_get_double(&cmdparams, kREF_L0, &status);

   outtypeStr = V2HparamsGetStr(&cmdparams, kOUTTYPE, &status);
   if (status == DRMS_SUCCESS && strcmp(outtypeStr, "no scaling") != 0)
   {
      outtype = drms_str2type(outtypeStr);
   }

   scale = cmdparams_get_double(&cmdparams, kOUTSCALE, &status);
   bias = cmdparams_get_double(&cmdparams, kOUTBIAS, &status);
   p0 = cmdparams_get_double(&cmdparams, kSOLAR_P, &status);
   psign = cmdparams_get_double(&cmdparams, kPSIGN, &status);
   perr = cmdparams_get_double(&cmdparams, kPERR, &status);
   ierr = cmdparams_get_double(&cmdparams, kIERR, &status);

   SetDistort(&cmdparams, 
	      kDISTORT, 
	      kCUBIC,
	      kTILTALPHA,
	      kTILTBETA,
	      kTILTFEFF,
	      &distP);

   trefstr = V2HparamsGetStr(&cmdparams, kREF_T0, &status);
   PARAMETER_ERROR(status, status, NULL, kREF_T0)

   tref = V2Hstr2Time(trefstr, &vret); /* secs */
   PARAMETER_ERROR(vret, vret, NULL, kREF_T0)

   /* determine mapcols and adjust longmin and longmax */
   mapped_lmax = cmdparams_get_int(&cmdparams, kMAPMMAX, &status); 
   longmax = cmdparams_get_double(&cmdparams, kMAPLGMAX, &status); /* degrees */
   longmin = cmdparams_get_double(&cmdparams, kMAPLGMIN, &status); /* degrees */
   longinterval = (180.0) / mapped_lmax;	                   /* degrees */
   /* 
    * This does not always handle the case where 1/longinterval is an integer 
    * correctly.
    */

   /* the next two statement do nothing, right? */
   /* why do nmin and max keep getting set with different RHSs? */
   nmin = (int)(longmin / longinterval); /* round towards 0 */
   nmax = (int)(longmax / longinterval); /* round towards 0 */
   colsperdeg = mapped_lmax / 180.0;
   nmin = (int)(longmin * colsperdeg); /* round towards 0 */
   nmax = (int)(longmax * colsperdeg); /* round towards 0 */
   mapcols = nmax - nmin + 1;
   longmin_adjusted = nmin * longinterval;
   longmax_adjusted = nmax * longinterval;

   /* determine maprows, bmax, bmin, and sinBdelta */
   sinb_divisions = cmdparams_get_int(&cmdparams, kSINBDIVS, &status);
   sinBdelta = 1.0/sinb_divisions;
   bmax = cmdparams_get_double(&cmdparams, kMAPBMAX, &status);     /* degrees */
   bmin = -bmax; 
   nmax = (int)(sin(RADSINDEG*bmax)*sinb_divisions); /* round towards 0 */
   maprows = 2*nmax;

   char *inRecQuery = cmdparams_get_str(&cmdparams, kRecSetIn, NULL);
   DRMS_RecordSet_t *inRecSet = drms_open_records(drms_env, inRecQuery, &status);

   if (status == DRMS_SUCCESS)
   {
      int nRecs = inRecSet->n;
      int iRec;

      gettimeofday(&tv0, NULL);

      for (iRec = 0; !error && iRec < nRecs; iRec++)
      {
	 DRMS_Record_t *inrec = inRecSet->records[iRec];

	 /* create an out record */
	 DRMS_RecordSet_t *rs = drms_create_records(drms_env, 
						    1, 
						    kSeriesOut,
						    DRMS_PERMANENT, 
						    &status);

	 error = (status != DRMS_SUCCESS);

	 if (!error)
	 {
	    outrec = rs->records[0];
	 }

	 if (!error && outrec && inrec)
	 {
	    if (iRec == 0)
	    {
	       /* XXX Do outseries stuff here. */
	    }
	    
	    ds = drms_getkey_int(inrec, kDSDS_DS, &status);
	    rn = drms_getkey_int(inrec, kDSDS_RN, &status);

	    tobsstr = V2HgetKeyStr(inrec, kT_OBS, &status); /* v2helio owns string */

	    if ((!tobsstr) || (!*tobsstr))
	    {
	       CreateBlankRecord(inrec, outrec, tobsstr);
	       SkipErr(status, inrec, "unable to locate critical keyword %s", kT_OBS);
	       continue; /* go to next image */
	    }

	    segin = drms_segment_lookup(inrec, kDSDS_Segment);

	    if (segin)
	    {
	       inarr = drms_segment_read(segin, segin->info->type, &status);
	       if (inarr)
	       {
		  InsertParam("arraydata", inarr->data);
		  InsertParam("array", inarr);
	       } /* inarr */
	    } /* segin */

	    if (!segin || status != DRMS_SUCCESS || !inarr)
	    {
	       CreateBlankRecord(inrec, outrec, tobsstr);
	       SkipErr(status, inrec, "no data to process");
	       continue; /* go to next image */
	    }

	    if (maxmissvals > 0) 
	    {
	       int missvals = drms_getkey_int(inrec, kMISSVALS, &status);
	       if (status != DRMS_SUCCESS || missvals > maxmissvals) 
	       {
		  CreateBlankRecord(inrec, outrec, tobsstr);
		  SkipErr(0, inrec, "%d pixels MISSING, which is more than is allowed", missvals);
		  continue;
	       }
	    }

	    /* assemble arguments */

	    tobs = V2Hstr2Time(tobsstr, &vret); /*secs*/
	    PARAMETER_ERROR(vret, vret, inrec, kT_OBS);

	    b0 = drms_getkey_double(inrec, kOBS_B0, &status);
	    PARAMETER_ERROR(status, V2HStatus_MissingParameter, inrec, kOBS_B0);

	    obsl0 = drms_getkey_double(inrec, kOBS_L0, &status);
	    PARAMETER_ERROR(status, V2HStatus_MissingParameter, inrec, kOBS_L0);

	    obsCR = drms_getkey_int(inrec, kOBS_CR, &status);
	   
	    if (!obsCR || status != DRMS_SUCCESS)
	    {
	       /* May 2, 2005. Commented this out to allow for no OBS_CR in MWO data.
		  Still printes warning. OBS_CR is not used by this program or
		  by helio2mlat. */
	       fprintf(stderr, "warning: missing OBS_CR.\n");
	    }

	    if (p0 == 999.0) 
	    {
	       p = drms_getkey_double(inrec, kSOLAR_P, &status);
	       PARAMETER_ERROR(status, V2HStatus_MissingParameter, inrec, kSOLAR_P);
	    } 
	    else 
	    {
	       p = p0;
	    }

	    /* fix for 1988 MWO
	       p = 180.0 - p ;
	    */
	    p = psign * p ;

	    /* 991839442. corresponds to hour 73878 minute 57 second 22
	       or 73878.956 or day 3078.2898, roughly when B0 is 0 */
	    /* b0=b0 * 0.986207; */ /* One way of correcting. */
	    /* The following is pretty good */
	    /* b0=b0-0.1*sin((tobs-991839442.)/31557600.*2*PI);*/
	    b0 = b0 + ierr * sin((tobs - 991839442.) / 31557600. * 2 * PI);

	    /* p=p-0.2; */
	    /* p=p-0.2+0.1*cos((tobs-991839442.)/31557600.*2*PI); */
	    p = p + perr - ierr * cos((tobs - 991839442.) / 31557600. * 2 * PI);

	    smajor = drms_getkey_double(inrec, kS_MAJOR, &status);
	    ParameterDef(status, kS_MAJOR, 1.0, &smajor, inrec);

	    sminor = drms_getkey_double(inrec, kS_MINOR, &status);
	    ParameterDef(status, kS_MINOR, 1.0, &sminor, inrec);

	    sangle = drms_getkey_double(inrec, kS_ANGLE, &status);
	    ParameterDef(status, kS_ANGLE, 0.0, &sangle, inrec);

	    xscale = drms_getkey_double(inrec, kX_SCALE, &status);
	    PARAMETER_ERROR(status, V2HStatus_MissingParameter, inrec, kX_SCALE);

	    yscale = drms_getkey_double(inrec, kY_SCALE, &status);
	    PARAMETER_ERROR(status, V2HStatus_MissingParameter, inrec, kY_SCALE);

	    imagescale = drms_getkey_double(inrec, kIM_SCALE, &status);
	    PARAMETER_ERROR(status, V2HStatus_MissingParameter, inrec, kIM_SCALE);

	    if (paramsign != 0)
	    {
	       vsign = paramsign;
	    }
	    else
	    {
	       vsign = drms_getkey_double(inrec, kDATASIGN, &status);
	       ParameterDef(status, kDATASIGN, 1.0, &vsign, inrec);
	    }

	    if (velocity_correction) 
	    {
	       obs_vr = drms_getkey_double(inrec, kOBS_VR, &status);
	       ParameterDef(status, kOBS_VR, 0.0, &obs_vr, inrec);

	       obs_vw = drms_getkey_double(inrec, kOBS_VW, &status);
	       ParameterDef(status, kOBS_VW, 0.0, &obs_vw, inrec);

	       obs_vn = drms_getkey_double(inrec, kOBS_VN, &status);
	       ParameterDef(status, kOBS_VN, 0.0, &obs_vn, inrec);
	    }

	    if (!skip_checko)
	    {
	       orientation = V2HgetKeyStr(inrec, kORIENT, &status);
	       PARAMETER_ERROR(status, status, inrec, kORIENT);
	       CheckO(orientation, &vret);
	    } 
	    else 
	    {
	       char foo[] = "SESW    ";
	       orientation = strdup(foo);
	       InsertParam(foo, orientation);
	    }

	    obsdist = drms_getkey_double(inrec, kOBS_DIST, &status);
	    ParameterDef(status, kOBS_DIST, 1.0, &obsdist, inrec);
	    S = rtrue / obsdist; /* radians - approx. arcsin(rtrue/obsdist) */

	    rsun = drms_getkey_double(inrec, kR_SUN, &status);
	    rsunDef = ARCSECSINRAD * S / sqrt(1.0 - S * S) / imagescale;	   
	    ParameterDef(status, kR_SUN, rsunDef, &rsun, inrec);
   
	    xs = drms_getkey_double(inrec, 
				    kXSCALE, 
				    &status); /* yeah, there is X_SCALE AND XSCALE */
	    if (status == DRMS_SUCCESS)
	    {
	       /* New meaning of R_SUN, change value to old one if X_SCALE == Y_SCALE
		  otherwise give up */
	       if (xscale == yscale) 
	       {
		  rsun = rsun * xscale;
	       }
	       else 
	       {
		  PARAMETER_ERROR(1, V2HStatus_Unimplemented, inrec, "X_SCALE != Y_SCALE")
	       }
	    }

	    if (longitude_shift == 1) 
	    {
	       tmearth = tobs+TAU_A*(1.0-obsdist); 
	       longshift = (obsl0-refl0)+longrate*(tmearth-tref); /* degrees */ 
	       while (longshift > 180.0) longshift-=360.0; 
	       while (longshift < -180.0) longshift+=360.0;
	    }
	    else if (longitude_shift == 2) /* Shift center to nearest Carrington Degree */
	    {
	       longshift =  obsl0 - (int)(obsl0);
	       if (longshift > 0.5) longshift -= 1.0;
	    }
	    else if (longitude_shift == 3)  /* Shift center to nearest tenth of a degree */
	    {
	       longshift = (obsl0 * 10 - (int)(obsl0 * 10)) / 10;
	       if (longshift > 0.5) longshift -= 1.0;
	    }
	    else
	    {
	       longshift = 0.0;
	    }

	    mapl0 = obsl0 - longshift;

	    xpixels = inarr->axis[0];
	    ypixels = inarr->axis[1];
	    pixels  = xpixels * ypixels;

	    x0 = drms_getkey_double(inrec, kX0, &status);
	    ParameterDef(status, kX0, xpixels / 2, &x0, inrec);

	    y0 = drms_getkey_double(inrec, kY0, &status);
	    ParameterDef(status, kY0, ypixels / 2, &y0, inrec);

	    /* construct the output array */
	    length[0] = mapcols; length[1] = maprows;
	    outarr = drms_array_create(DRMS_TYPE_FLOAT, 2, length, NULL, &status);

	    if (status != DRMS_SUCCESS)
	    {
	       SkipErr(status, inrec, "unable to create output array");
	       continue;
	    }

	    /* ??? how many of these attributes do we really need? */
	    /* ??? what attributes are missing ??? */
	    drms_setkey_string(outrec, kT_OBS, tobsstr);
	    drms_copykey(outrec, inrec, kI_DREC);
	    drms_copykey(outrec, inrec, kT_REC);
	    drms_copykey(outrec, inrec, kQUALITY);
	    drms_copykey(outrec, inrec, kMISSVALS);
	    drms_setkey_int(outrec, kSN, rn);
	    drms_setkey_int(outrec, kPIXELS, pixels);
	    drms_setkey_double(outrec, kOBS_B0, b0);
	    drms_setkey_double(outrec, kOBS_L0, obsl0);
	    drms_setkey_int(outrec, kOBS_CR, obsCR);
	    drms_setkey_double(outrec, kSOLAR_P, p);
	    drms_setkey_double(outrec, kX0, x0);
	    drms_setkey_double(outrec, kY0, y0);
	    drms_setkey_int(outrec, kMAPMMAX, mapped_lmax);
	    drms_setkey_int(outrec, kMAPCOLS, mapcols);
	    drms_setkey_int(outrec, kMAPROWS, maprows);
	    drms_setkey_double(outrec, kMAPLGMAX, longmax_adjusted);
	    drms_setkey_double(outrec, kMAPLGMIN, longmin_adjusted);
	    drms_setkey_double(outrec, kMAPBMAX, bmax);
	    drms_setkey_double(outrec, kMAPBMIN, bmin);
	    drms_setkey_double(outrec, kSINBDELT, sinBdelta);
	    drms_setkey_double(outrec, kMAPRMAX, rmax);
	    drms_setkey_int(outrec, kAPODIZE, apodization);
	    drms_setkey_double(outrec, kAPINNER, apinner);
	    drms_setkey_double(outrec, kAPWIDTH, apwidth);
	    drms_setkey_int(outrec, kAPEL, apel);
	    drms_setkey_double(outrec, kAPX, apx);
	    drms_setkey_double(outrec, kAPY, apy);
	    drms_setkey_int(outrec, kMCORLEV, mag_correction);
	    drms_setkey_int(outrec, kVCORLEV, velocity_correction);
	    drms_setkey_int(outrec, kINTERPO, interpolation);
	    drms_setkey_int(outrec, kSHIFTFLG, longitude_shift);
	    drms_setkey_double(outrec, kLSHIFT, longshift);
	    drms_setkey_double(outrec, kMAP_L0, mapl0);

	    /* Need to know whether the input was a 1-minute or 5-minute magnetogram */
	    drms_copykey(outrec, inrec, DPC);
	    drms_copykey(outrec, inrec, DPC_STR);
	    drms_copykey(outrec, inrec, DPC_OBSR);
	    drms_copykey(outrec, inrec, DPC_FORM);
	    drms_copykey(outrec, inrec, DPC_CROP);
	    drms_copykey(outrec, inrec, DPC_ORGN);
	    drms_copykey(outrec, inrec, DPC_RATE);
	    drms_copykey(outrec, inrec, DPC_SMPL);
	    drms_copykey(outrec, inrec, DPC_CONF);

	    /* magsynop needs to know the build version of the fits files used. */
	    drms_copykey(outrec, inrec, BLDVER00);
	    drms_copykey(outrec, inrec, BLDVER10);
	    drms_copykey(outrec, inrec, BLDVER15);
	    drms_copykey(outrec, inrec, BLDVER18);

	    /* magsynop needs to know whether the fits file contains radial values or not. */
	    drms_copykey(outrec, inrec, FDRADIAL);

	    /* save the differential rotation correction parameters */
	    if (carrStretch)
	    {
	       int csVal = 1;
	       drms_setkey_int(outrec, CARSTRCH, csVal);
	       drms_setkey_float(outrec, DIFROT_A, diffrotA);
	       drms_setkey_float(outrec, DIFROT_B, diffrotB);
	       drms_setkey_float(outrec, DIFROT_C, diffrotC);
	    }

	    if (mag_offset) 
	    {
	       float *dat = (float *)inarr->data;
	       double bfitzero = drms_getkey_double(inrec, kBFITZERO, &status);
	       int i;
	       char str[80];

	       if (status == DRMS_SUCCESS || isnan(bfitzero)) 
	       {
#if 0
		  /* XXX - not sure what to do with history. */
		  sds_append_history(out_sds, 
				     "MDI magnetogram zero level correction was requested but\n");
		  sds_append_history(out_sds,
				     "BFITZERO keyword was not found.  No correction was made.\n");
#endif
	       } 
	       else 
	       {
		  for (i = 0; i < pixels; ++i) 
		  {
		     dat[i] -= (float)bfitzero;
		  }

#if 0
		  /* XXX - not sure what to do with history. */
		  sprintf(str,
			  "An MDI magnetogram zero level correction of %.4g Gauss\n",
			  bfitzero);
		  sds_append_history(out_sds,str);
		  sprintf(str, "was subtracted from the input image before remapping.\n");
		  sds_append_history(out_sds,str);
#endif
	       }
	    }

	    if (status = Obs2helio((float *)inarr->data, 
				   (float *)outarr->data,
				   xpixels, 
				   ypixels, 
				   x0, 
				   y0, 
				   b0 * RADSINDEG, 
				   p * RADSINDEG, 
				   S, 
				   rsun, 
				   rmax,
				   interpolation, 
				   mapcols, 
				   maprows, 
				   longmin_adjusted * RADSINDEG, 
				   longinterval * RADSINDEG,
				   longshift * RADSINDEG, 
				   sinBdelta, 
				   smajor, 
				   sminor, 
				   sangle * RADSINDEG, 
				   xscale, 
				   yscale, 
				   orientation, 
				   mag_correction,
				   velocity_correction, 
				   obs_vr, 
				   obs_vw, 
				   obs_vn, 
				   vsign, 
				   NaN_beyond_rmax,
				   carrStretch,
				   &distP,
				   diffrotA,
				   diffrotB,
				   diffrotC,
				   rRates,
				   sizeof(rRates))) 
	    {
	       SkipErr(status, inrec, "Failure in obs2helio");
	       continue; /* go to next image */
	    }

	    /* Output to log rotation rate (deg/day) vs. latitude */
	    if (debugLog)
	    {
	       fprintf(stdout, "latitude\tsidereal rotation rate (deg/day)\n");
	       for (row = 0; row < maprows && row < sizeof(rRates); row++)
	       {
		  fprintf(stdout, "%10f\t%32f\n", rRates[row].lat / RADSINDEG, rRates[row].r);
	       }
	    }

	    if (status = apodize((float *)outarr->data,
				 b0 * RADSINDEG, 
				 mapcols, 
				 maprows,
				 longmin_adjusted * RADSINDEG,
				 longinterval * RADSINDEG,
				 sinBdelta,
				 apodization, 
				 apinner, 
				 apwidth, 
				 apel, 
				 apx, 
				 apy)) 
	    { 
	       SkipErr(status, inrec, "Failure in apodize");
	       continue; /* go to next image */
	    }

	    if (outtype != DRMS_TYPE_FLOAT && outtype != DRMS_TYPE_RAW)
	    {
	       drms_array_convert_inplace(outtype, bias, scale, outarr);
	    }

	    /* Write to the out record */
	    if (outrec)
	    {
	       DRMS_Segment_t *segout = drms_segment_lookup(outrec, kDSDS_Segment);
	       if (segout)
	       {

		  /* copy global keywords to out */
#if 0
		  /* XXX */
		    sds_append_attrs(out_vds->global_attributes, in_vds->global_attributes);
		  vds_setkey_str(out_vds, "CONFORMS", "TS_EQ");
#endif

		  drms_segment_write(segout, outarr, 0);
	       }

	       drms_close_records(rs, DRMS_INSERT_RECORD);
	    }

	    gettimeofday(&tv, NULL);
	    fprintf(stdout, 
		    "rn %d done (%f elapsed)\n", 
		    rn, 
		    (float)((tv.tv_usec - tv0.tv_usec) / 1000));
	 } /* inrec */
      } /* iRec */
   }

   if (inRecSet)
   {
      drms_close_records(inRecSet, DRMS_FREE_RECORD);
   }

   CleanParamMem();

   gettimeofday(&tv, NULL);
   fprintf(stdout, "Total time spent %f\n", (float)((tv.tv_usec - tv0.tv_usec) / 1000));
   return error;

} /* end v2helio_strategy */

static double V2Hstr2Time(const char *timestr, V2HStatus_t *stat)
{
   /* convert keyword time format to UNIX time_t -- no fractional seconds */
   time_t return_t = 0;
   static struct tm t;

   t.tm_isdst = 0; /* t.tm_gmtoff = 0; t.tm_zone = NULL;*/ 

   if(!timestr) 
   {
      *stat = V2HStatus_MissingParameter;
   }
   else if (sscanf(timestr,"%d.%d.%d_%d:%d:%d",
		   &(t.tm_year), &(t.tm_mon), &(t.tm_mday), 
		   &(t.tm_hour), &(t.tm_min), &(t.tm_sec)) != 6)
   {
      *stat = V2HStatus_IllegalTimeFormat;
   }
   else 
   {
      t.tm_mon -= 1; 
      t.tm_year -= 1900; 
      return_t = mktime(&t);

      *stat = (return_t == (time_t)-1) ? V2HStatus_TimeConvFailed : V2HStatus_Success;
   }
   return (double)return_t;
}

static void CheckO(const char *orientation, V2HStatus_t *stat)
{
   /* check for legal orientation string */
   static char o[MAXLEN];
   char *c = o;

   if(!orientation)
   {
      *stat = V2HStatus_MissingParameter;
   } 
   else if (4 != sscanf(orientation, "%[NS]%[EW]%[NS]%[EW]", c++, c++, c++, c++))
   {
      *stat = V2HStatus_IllegalOrientation;
   }
   else if ((o[0] == o[2]) && (o[1] == o[3]))
   { 
      *stat = V2HStatus_IllegalOrientation;
   }
   else if ((o[0] !=o [2]) && (o[1] != o[3])) 
   {
      *stat = V2HStatus_IllegalOrientation;
   }
   else 
   {
      *stat = V2HStatus_Success;   
   }
}
