/*
 *  soi_error.h						~soi/(version)/include
 *
 *  Declarations required for error and information logging between
 *    strategy-level and interface support routines.
 *
 *  C++ programs should include the corresponding file "soi_error.hxx".
 *  Additional information is in the following man pages:
 *
 *  Responsible:  Kay Leibrand			KLeibrand@solar.Stanford.EDU
 *
 *  Bugs:
 *	soi_error.hxx does not exist.
 *	No man pages.
 *
 *  Revision history is at the end of the file.
 */

#ifndef SOI_ERROR_INCL

/****************************************************************************/
/**************************  INCLUDE STATEMENTS  ****************************/
/****************************************************************************/

#ifndef SOI_VERSION_INCL
#include <soi_version.h>
#endif

/****************************************************************************/
/****************************  DEFINE STATEMENTS  ***************************/
/****************************************************************************/

#define SOI_ERROR_VERSION_NUM	(2.8)

#define NO_ERROR		(0)

#define SDS_PTR_NULL		(512)
#define SDS_INPUT_PTR_NULL	(514)
#define SDS_OUTPUT_PTR_NULL	(515)
#define SDS_DATA_PTR_NULL	(516)
#define SDS_RANK_ZERO		(517)
#define SDS_RANK_ERROR		(518)
#define SDS_DIM_ERROR		(519)
#define SDS_INVALID_DATATYPE	(520)
#define DATA_OUT_OF_RANGE	(521)

#define ATTRIBUTE_PTR_NULL	(622)
#define ATTRNAME_NULL		(623)
#define ATTRVALUE_NULL		(624)
#define ATTRVALUE_TYPE_UNKNOWN	(625)
#define ATTRIBUTE_NOT_FOUND	(626)
#define ATTRIBUTE_VALUE_NAN	(627)

#define CANT_OPEN_FILE		(730)
#define IO_ERROR		(732)
#define BAD_FILE_DESCRIPTOR	(733)
#define READ_FAILED		(734)
#define CANT_OPEN_INPUT_FILE	(735)
#define CANT_OPEN_OUTPUT_FILE	(736)
#define UNSUPPORTED_DATA_FORMAT (737)
#define SDS_FILENAME_NULL	(738)

#define MALLOCED_NULL_PTR	(740)
#define MALLOC_FAILED		(741)

#define CANT_OPEN_PIPE		(760)
#define CANT_FORK		(761)
#define CANT_EXEC		(762)
#define CANT_MKDIR		(763)
#define CANT_LOCK_FILE		(764)
#define CANT_ACCESS		(765)

#define IDS_ERROR		(800)
#define IDS_PTR_NULL		(801)
#define IDS_FITS_SERIES_ERROR	(802)
#define IDS_SLICE_SERIES_ERROR	(803)
#define IDS_RANK_ZERO		(804)

#define SDS_CONVERT_FAILED	(900)
#define SDS_STATS_ERROR		(901)

#define THIS_SHOULDNT_HAPPEN	(1020)
#define MAKES_NO_SENSE		(1021)
#define DOESNOT_COMPUTE		(1022)
#define NOT_IMPLEMENTED		(1023)
#define BAD_WORKING_DIRECTORY	(1024)
#define BAD_EXT_COFF		(1025)

#define CDF_ERROR		(1100)

#define VDS_ERROR	        (1200)
#define VDS_PTR_NULL	        (1201)
#define VDS_NAMELIST_ERROR	(1202)
#define UNKNOWN_DATASET_TYPE	(1203)
#define UNKNOWN_INTERNAL_TYPE	(1204)
#define VDS_RECORD_ERROR	(1205)
#define VDS_SELECT_ERROR	(1206)
#define VDS_VAR_EXISTS          (1210)
#define VDS_OPEN_ERROR          (1211)
#define VDS_CLOSE_ERROR         (1212)
#define VDS_GLOBAL_ATTRS_NULL   (1213)
#define VDS_NO_DATASET		(1214)
#define VDS_GATHER_ERROR	(1215)
#define VDS_NO_OVERVIEW_FILE	(1216)
#define VDS_OVERVIEW_READ_ERROR (1217)
#define VDS_OPEN_BAD_FILELIST	(1218)
#define VDS_OPEN_NO_RECORD_INFO (1219)
#define VDS_CONFORMANCE_ERROR	(1220)
#define VDS_NO_FILENAME		(1221)

#define ALL_DATA_MISSING	(0)
#define SOME_DATA_MISSING	(-1)
#define DATA_AVAILABLE		(1)
						    /*  ascii table support  */
#define AT_PTR_NULL		(1300)
#define AT_FILENAME_NULL	(1301)
#define AT_COL_ERROR		(1302)
								 /*  binlog */
#define BAD_BINLOG_DEFINITION	(1400)
#define BAD_DPC_FILE		(1401)
#define BAD_QUALITY_LOG		(1402)
#define CANNOT_CREATE_BINLOG	(1403)
#define CANNOT_GET_MODTIME	(1404)
#define CANNOT_OPEN_DIR		(1405)
#define CANNOT_READ_BINLOG	(1406)
#define CANNOT_WRITE_BINLOG	(1407)
#define NO_DPC_FILES		(1408)
#define NO_QUALITY_FILES	(1409)
#define UNSUPPORTED_LEVEL	(1410)
								   /*  keys  */
#define KEY_NOT_FOUND		(1501)
#define KEY_WRONG_TYPE		(1502)
#define KEYTYPE_NOT_IMPLEMENTED	(1503)
  								   /*  FITS  */
#define FITS_ERROR			(1600)
#define FITS_NONCONFORMING		(1601)
#define FITS_HAS_EXTENSIONS  		(1602) 
#define FITS_EXTENSION_ASCII_TABLE	(1603) 
#define FITS_EXTENSION_IMAGE		(1604) 
#define FITS_EXTENSION_BINARY_TABLE	(1605) 
#define FITS_UNRECOGNIZED_EXTENSION	(1606) 
#define FITS_UNSUPPORTED_EXTENSION	(1607) 
#define FITS_HAS_RANDOM_GROUPS		(1608) 
  								  /*  names  */
#define CANNOT_MAKE_NAME		(2001)
#define CLASS_SYNTAX_ERROR		(2002)
#define DATACOLLECTION_SYNTAX_ERROR	(2003)
#define DATASUPERSET_SYNTAX_ERROR 	(2004)
#define DATA_SELECTOR_SYNTAX_ERROR	(2005)
#define MISSING_DATA_NAME		(2006)
#define MISSING_ROOT_KEY		(2007)
#define RANGE_SYNTAX_ERROR 		(2008)
#define CANNOT_FILL_TEMPLATE		(2009)
#define TEMPLATE_SYNTAX_ERROR		(2010)
#define NAME_TOO_LONG			(2011)

					/*  strategy modules and functions  */
#define MISSING_FILES		(3000)
#define NO_IMAGE_TIME		(3001)
#define NO_IMAGE_DATE		(3002)
#define RANK_MISMATCH		(3003)
#define DIMENSION_MISMATCH	(3004)
#define UNSUPPORTED_DATATYPE	(3005)
#define CVTLM_ERROR		(3006)
#define EXTRACT_DPC_ERROR	(3007)
#define INGEST_SVC_ERROR	(3008)
#define OUTGEST_SVC_ERROR	(3009)
#define SN_BLOCKING_ERR         (3010)
#define CANT_DO_ODD_NUM_LATS    (3011)
#define MODES_REQ_GT_PRESENT    (3012)
#define FFT_LENGTH_ERR          (3013)
#define ILLEGAL_LMIN            (3014)
#define MALLOC_ERR              (3015)
#define ILLEGAL_DELTA_F         (3016)
#define ILLEGAL_DURATION        (3017)
#define INGEST_HRSC_ERROR       (3018)
#define INGEST_HRSC_LOG_ERROR   (3019)
#define MERGE_HRSC_ERROR	(3020)
#define MERGE_HRSC_LOG_ERROR	(3021)
#define GRPDS_ERROR		(3022)
#define REQUIRED_PARAMETER_MISSING	(3023)
#define ILLEGAL_TILE_NAME	(3024)
#define NO_TILE_INTERSECTION	(3025)
#define ATLAS_PAGE_ERROR	(3026)
#define ILLEGAL_TIME_FORMAT     (3027)
#define TIME_CONVERSION_FAILED  (3028)
#define ILLEGAL_ORIENTATION     (3029)
#define MMLI_ERROR		(3030)
#define DEFAULT_PARAMETER_USED	(3031)
#define INGEST_5K_ERROR         (3032)
#define TILE_BLOCKING_ERROR 	(3033)
#define INCONSISTENT_CONTRAINTS	(3034)
#define INGEST_5K_LOG_ERROR     (3035)
#define EXTRACT_5K_ERROR	(3036)
#define DATA_INDEX_ERROR	(3037)
#define UNSUPPORTED_INTERPOLATION       (3038)
#define UNSUPPORTED_V_CORRECTION        (3039)
#define GATHER_INFO_NO_INPUT_DATA (3040)
#define OVR2NAME_ERROR (3041)
#define MDICAL_ERROR (3042)
#define CPINFO_ERROR (3043)
#define REQUIRED_KEYVALUE_MISSING	(3044)
#define UNSUPPORTED_M_CORRECTION        (3045)
#define SAINFO_ERROR		(3046)


					      /*  near-line storage access  */
#define LAGO_BUSY               (4001)
#define LAGO_CANT_OPEN_FILE     (4002)
#define LAGO_CLOSE_ERROR        (4003)
#define LAGO_CORRUPT            (4004)
#define LAGO_ERROR              (4005)
#define LAGO_INFO_ERROR         (4006)
#define LAGO_INVALID_COMMAND    (4007)
#define LAGO_INVENTORY_FAILURE  (4008)
#define LAGO_LOAD_ERROR         (4009)
#define LAGO_NO_BARCODE         (4010)
#define LAGO_NO_TAPE            (4011)
#define LAGO_NO_DRIVE           (4012)
#define LAGO_NO_UID             (4013)
#define LAGO_NO_DIRECTORY       (4014)
#define LAGO_NO_FILENUM         (4015)
#define LAGO_OPEN_ERROR         (4016)
#define LAGO_RESPONSE_TOO_LONG  (4017)
#define LAGO_SYNTAX_ERROR       (4018)
#define LAGO_TAPE_IN_USE        (4019)
#define LAGO_TIMEOUT            (4020)
#define LAGO_UNKNOWN_BARCODE    (4021)
#define LAGO_UNKNOWN_TAPE       (4022)
#define LAGO_UNKNOWN_UID        (4023)
#define LAGO_SPAWN_ERROR        (4024)
#define LAGO_SEND_ERROR         (4025)
#define LAGO_RECEIVE_ERROR      (4026)
#define LAGO_PACK_ERROR         (4027)
#define LAGO_UNPACK_ERROR       (4028)
#define LAGO_NO_REQUESTS        (4029)
#define LAGO_PVM_INFO_ERROR     (4030)

#define AMPEX_BUSY		(4501)
#define AMPEX_CANT_OPEN_FILE	(4502)
#define AMPEX_CLOSE_ERROR	(4503)
#define AMPEX_CORRUPT		(4504)
#define AMPEX_ERROR		(4505)
#define AMPEX_INFO_ERROR		(4506)
#define AMPEX_INVALID_COMMAND	(4507)
#define AMPEX_INVENTORY_FAILURE	(4508)
#define AMPEX_LOAD_ERROR		(4509)
#define AMPEX_NO_BARCODE		(4510)
#define AMPEX_NO_TAPE		(4511)
#define AMPEX_NO_DRIVE		(4512)
#define AMPEX_NO_UID		(4513)
#define AMPEX_NO_DIRECTORY	(4514)
#define AMPEX_NO_FILENUM		(4515)
#define AMPEX_OPEN_ERROR		(4516)
#define AMPEX_RESPONSE_TOO_LONG	(4517)
#define AMPEX_SYNTAX_ERROR	(4518)
#define AMPEX_TAPE_IN_USE	(4519)
#define AMPEX_TIMEOUT		(4520)
#define AMPEX_UNKNOWN_BARCODE	(4521)
#define AMPEX_UNKNOWN_TAPE	(4522)
#define AMPEX_UNKNOWN_UID	(4523)
#define AMPEX_SPAWN_ERROR	(4524)
#define AMPEX_SEND_ERROR		(4525)
#define AMPEX_RECEIVE_ERROR	(4526)
#define AMPEX_PACK_ERROR		(4527)
#define AMPEX_UNPACK_ERROR	(4528)
#define AMPEX_NO_REQUESTS	(4529)
#define AMPEX_PVM_INFO_ERROR	(4530)
#define AMPEX_POSITION_ERROR	(4531)
#define AMPEX_STORE_ERROR       (4532)
#define AMPEX_READ_ERROR	(4533)
#define AMPEX_WRITE_ERROR	(4534)
#define AMPEX_INIT_ERROR	(4535)
#define AMPEX_NO_HOST           (4536)
#define AMPEX_UNEXP_POS         (4537)

					 /* dsds_svc &  DSDS catalog access  */
#define DATA_NOT_FOUND	(5000) /* db qry returned no rows called from any func*/
#define DS_DATA_QRY	(5001) /* usually no such dataset */
			       /* querying the database failed */
#define DS_DATA_REQ	(5002) /* invalid prefix sending to DS_DataRequest */
#define DS_DATA_UPD	(5003) /* err updating the db and/or detected nulls */
			       /* for not null columns */
#define DS_PALC_ERR	(5004) /* err accessing partn_alloc */
#define DS_PE_ERR	(5005) /* err accessing open_pe */
#define DS_PAVAIL_ERR	(5006) /* err accessing partn_avail */
#define DS_TAPE_UPD	(5007) /* err accessing tape */
#define DS_ARCHIVE	(5008) /* return value if data is offline */
#define DS_LAGO_SEND_ERR (5009)	/* err sending msg to lago_svc */
#define DS_UID_ERR	(5010)	/* dsds_svc can't open pseudo-pe */
#define DS_ALLOC_ERR	(5011)	/* can't allocate storage */
#define DS_MAX_LAGO_ERR	(5012)	/* max# of active lago_svc retrieves*/
#define DS_NO_LAGO	(5013)	/* no lago/ampex_svc is running */
#define DS_DEALLOC_ERR	(5014)	/* can't deallocate storage */
#define DS_OPEN_ERR	(5015)	/* DS_Open() call failure */
#define DS_NO_DATA	(5016)	/* DS_DataRequest retrieves no rows */
#define DS_ILL_REQ	(5017)	/* dsds_svc got an illegal request # */
#define DS_CONNECT_DB  (5018)  /* DS_ConnectDB fails */
#define DS_DISCONNECT_DB  (5019)  /* DS_DisconnectDB fails */
#define DS_SQL_CMD  (5020)  /* DS_SqlDo fails */
#define DS_COMMIT_DB  (5021)  /* DS_Commit fails */
#define DS_ROLLBACK_DB  (5022)  /* DS_Rollback fails */
#define DS_NO_SYNC_STOR  (5023)  /* DS_SyncStor fails */
#define DS_NO_DSDS_SVC  (5024)  /* no dsds_svc running */
#define DS_PACK_ERROR  (5025)  /* err packing pvm msg */
#define DS_UNPACK_ERROR  (5026)  /* err unpacking pvm msg */
#define DS_DSDS_SEND_ERR  (5027)  /* err sending pvm msg */
#define DS_RESP_TIMEOUT  (5028)  /* response from dsds_svc timeout */
#define DS_CATDEL_ERR  (5029)  /* DS_CatDelete fails */

#define DS_AMPEX_SEND_ERR	(5509)	/* err sending msg to ampex_svc */
#define DS_MAX_AMPEX_ERR	(5512)	/* max# of active ampex_svc retrieves*/
#define DS_NO_AMPEX	(5513)	/* no ampex_svc is running */
#define DS_MALLOC_ERR	(5514)	/* dsds_svc can't malloc */
#define DS_LOGIC_ERR	(5515)	/* dsds_svc logic err, see its log file */
#define DS_TOUCH_NEG	(5516)	/* pe/peq using illegal negative touch value */
#define DS_NAME_OBSOLETE (5517)	/* explicit svc_name/svc_version no longer supported*/
#define DS_NOT_ARCHIVED	(5518) /* can't mv a ds has not been archived yet */
#define DS_MV_PERM	(5519) /* can't mv a perm ds */
#define DS_MV_NF	(5520) /* can't find the moved ds dp entry */
#define DS_MV_NUP	(5521) /* mv can't update dsds_main or partn_alloc */

#define RPC_SVC_NOT	(5600)	/* pe_rpc_svc not running or error  */

#define MDI_SW_ERR	(5700)	/* bad MDI_SW row in MDI_log_01d file */

#define MDI_ERROR 		(9000)	/* start MDI telemetry errors here  */
#define MDI_PKT_MUNGED		(9001)
#define UNKNOWN_DPC		(9002)
#define MDI_DPC_TABLE_ERROR	(9003)
#define MDI_INDEX_FILE_ERROR	(9004)
#define MDI_INFO_FILE_ERROR	(9005)
#define DPC_IS_ZERO		(9006)
#define DPC_NOT_FOUND		(9007)
#define MDI_5K_DECOMPRESS_ERROR	(9008)
#define MDI_5K_SEGNUM_ERROR	(9009)
#define MDI_MISSING_PACKETS	(9010)
#define MDI_MISSING_SEGMENTS	(9011)
#define MDI_5k_TFR_FORMAT_ERROR	(9012)
#define MDI_NO_DP_HEADERS	(9013)
#define MDI_BAD_REFTIMES	(9014)
#define MDI_DECOMPRESS_ERROR	(9015)
#define NOT_LIST_DATA		(9016) /* related error codes for DPC-based  */
#define UNSUPPORTED_DPC		(9017)			      /* processing  */

#define NO_SUCH_HK_MNEMONIC     (9050) /* MDI_hk.c error codes for           */
#define NO_HK_MNEMONICS_FOUND   (9051) /* housekeeping processing            */
#define MDI_HK_PKT_FMT_ERROR    (9052)
#define SC1_HK_PKT_FMT_ERROR    (9053)
#define SC2_HK_PKT_FMT_ERROR    (9054)
#define SC3_HK_PKT_FMT_ERROR    (9055)
#define SC4_HK_PKT_FMT_ERROR    (9056)
#define HK_PARM_TBL_ERROR       (9057)
#define HK_PCA_TBL_ERROR        (9058)
#define HK_CA_TBL_ERROR         (9059)
#define HK_PCD_TBL_ERROR        (9060)
#define HK_ITEM_NOT_REGISTER    (9061)
#define HK_ITEM_NOT_DIGITAL     (9062)
#define HK_ITEM_NOT_ANALOG      (9063)
#define HK_TYPE_NOT_RECOGNIZED  (9064)
#define HK_INVALID_LIST_LEN     (9065)
#define HK_INVALID_LIST_PTR     (9066)
#define HK_INVALID_LIST_ERRPTR  (9067) 

#define HK_SFD_TROUBLE 		(9100)

#define LEV0_STATS_ERROR	(9200)
#define MAKE_LIST_ERROR		(9300)

#define FSEEK_ERROR	(10000)	
#define FREAD_ERROR	(10001)	
#define FWRITE_ERROR    (10002) 
#define FLAT_FILE_ERROR (10003) 
#define FOPEN_ERROR     (10004) 

/* abortflg values for a strategy module to send back to pe */
#define ABORTFLG_OK	(0)	/* module completed successfully */
#define ABORTFLG_NG	(1)	/* module aborted, don't save output ds */
#define ABORTFLG_NG_SV	(2)	/* module aborted, save output ds */

#define SOI_ERROR_INCL	1

/****************************************************************************/
/*********************  GLOBAL & EXTERN DECLARATIONS  ***********************/
/****************************************************************************/

extern int soi_errno;		       	    /*  global soi_error variable  */
/* soi_errno should be declared (and possibly initialized) ONCE, globally, 
   normally in the main procedure file, for each executable which uses it.
   e.g. 
	#include soi_error.h
	int soi_errno = NO_ERROR;
        ...
        main()
        ...
*/

/****************************************************************************/
/****************************  MACRO DEFINITIONS  ***************************/
/****************************************************************************/

/****************************************************************************/
/***************************  FUNCTION PROTOTYPES  **************************/
/****************************************************************************/

					          /*  source file: error.c  */
extern void die (char *fmt, int status, ...);
extern void ignore (char *fmt, ...);
extern int sethistory (char *filename);
extern int setlogfile (char *filename);
extern void report_msg (char *fmt, ...);
extern void write_history (char *fmt, ...);
extern int write_logfile (char *fmt, ...);
/*extern int write_log (char *fmt, ...);*/

extern void perrstk (int (*printfunct)(const char *, ...));
extern void errstk (char *fmt, ...);
extern void initerrstk();

#endif
