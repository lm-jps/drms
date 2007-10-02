#ifndef __LOAD_HK_CONFIGS_H
#define __LOAD_HK_CONFIGS_H

/*****************************************************************************
 * Filename: load_hk_configs.h                                               *
 * Author: Carl Cimilluca                                                    *
 * Create Date: August, 5, 2005                                              *
 * Description: This file contains the structures and link list structures   *
 *              used to save the configuration formats for the Housekeeping  *
 *              Packet format from the high rate bit stream coming from      *
 *              the HMI-EGSE-SS telemetry simulator and DDS.                 *
 * Revision History:                                                         *
 * Draft/  Who    Date           Description                                 *
 * Version                                                                   *
 * ------- ----  -------------  ------------------------------------------   *
 * 0.0     Carl   08/05/2005     Created                                     *
 * 0.1     Carl   10/31/2005     Added version number fields as string       *
 *                               buffer rather than as a float value.        *
 * 0.2     Carl   01/19/2006     Add conv_type to keyword structure.         *
 *                               Increased MAXLINE_IN_FILE to 150 to read    *
 *                               Added  hmi_id_version_number and            *
 *                               aia_id_version_number to structures         *
 * 0.3     Carl   04/04/2006     Increase keyword lines read in from the     *
 *                               APID-#-VERSION-# files from 200 to array    *
 *                               size 3000. There where 756 lines loaded.    *
 * 0.4     Carl   05/10/2006     Added structures and define to do analog    *
 *                               and digital conversions of raw values to    *
 *                               engineering values.Added new functions and  *
 *                               updated function calls.                     *
 * 0.5     Carl  07/11/2006     Added Define parameter HMI_ID_TYPE,          *
 *                              AIA_ID_TYPE,etc. Added structures            *
 *                              ALG_Conversion_struct and                    *
 *                              DSC_Conversion_struct. Increase max number   *
 *                              of coffs to 6. Formatted structures in file. *
 *                              Added define for HK_MAX_TLM_NAME             *
 *****************************************************************************/
/******************************defines****************************************/
#define MAXLINE_IN_FILE         150
#define MAX_NUM_KW_LINES        3000
#define MAX_DATE_SIZE           15
#define MAX_HK_MNM              30
#define MAX_HK_KYWD             10
#define MAX_HK_VALUE_TYPE       10
#define MAX_APID_POINTERS       4095
#define MAX_DIRECTORY_NAME      200
#define MAX_FILE_NAME           300
#define ERROR_LOADING_HK_DATA   1
#define ERROR_SAVING_FILE_LINE  1
#define ERROR_SAVING_APID_LINE  1
#define SUCESSFUL               0
#define SUCCESSFUL              0
#define MAX_SIZE_CHANGE_TIME    15
#define MAX_SIZE_CHANGE_DATE    15
#define MAX_CHAR_VERSION_NUMBER 10
#define MAX_NUM_ACON_LINES      1000
#define MAXLINE_ACON_IN_FILE    200
#define MAX_NUM_DCON_LINES      4000
#define MAXLINE_DCON_IN_FILE    200
#define MAX_VALUE_DSC           100
#define MAX_NUMBER_COFFICIENTS  6
#define HMI_ID_TYPE             "HMI"
#define AIA_ID_TYPE             "AIA"
#define OTHER_ID_TYPE           "SSIM"
#define MAX_PACKET_ID_TYPE      100
#define HK_MAX_TLM_NAME         200

/*************************typedef structures**********************************/
/*****************************************************************************/
/* Structure used to contain  pointer to the HK Configuration                */
/*  data based on the APID of packet.  This is a linked list .               */
/*                                                                           */
/*****************************************************************************/
typedef struct APID_Pointer_HK_Configs_struct    
{
  int apid;                              /*Make either hex or i
					   decimal value*/
  struct HK_Config_Files_struct *ptr_hk_configs;/*Pointer to HK 
						  Configurations struct*/
  struct APID_Pointer_HK_Configs_struct *next;/* Link List's next node */
} APID_Pointer_HK_Configs;
/*****************************************************************************/
/* Structure used to contain file parameters and pointer to the              */
/* many keyword parameters each with 6 or so parameters .                    */
/* The structure is a link list of hk_config-file structures                 */
/*****************************************************************************/
typedef struct HK_Config_Files_struct  
{
  int apid_number;
  char packet_id_type[MAX_PACKET_ID_TYPE];
  char file_version_number[MAX_CHAR_VERSION_NUMBER];/*Value for the apid-version<version-
				                      number>.txt file */
  char parameter_version_number[MAX_CHAR_VERSION_NUMBER];/*Values in GTCIDS map file for 
				   HMI_VER_NUM_SEQ_STATUS */
  char date[MAX_DATE_SIZE];      /*Example: 2005/07/06 or 10 characters*/
  int number_bytes_used;        /* Example: 112 in apid file and stanford file*/
  struct Keyword_Parameter_struct *keywords;/*Pointer to array or vector of
					      keyword_parameter structures*/
  struct HK_Config_Files_struct  *next;
} HK_Config_Files;
                                                                                                           
/****************************************************************************/
/*   Structures used to contain values for analog and digital               */
/*   conversion values to use to set engineering values for                 */
/*   A and D type of keywords. This structure is a linked list.             */
/****************************************************************************/
typedef struct ALG_Conversion_struct
{
  int     number_of_coeffs;
  double  coeff [MAX_NUMBER_COFFICIENTS];
} ALG_Conversion;

typedef struct DSC_Conversion_struct
{
  int     low_range;
  int     high_range;
  char    dsc_value[MAX_VALUE_DSC];
  struct  DSC_Conversion_struct  *next;
} DSC_Conversion;

/****************************************************************************/
/*   Structure used to contain values of hk keyword parameters              */
/*   This structure is a linked list of keyword_parameter structures        */
/****************************************************************************/
typedef struct Keyword_Parameter_struct  {
  char telemetry_mnemonic_name[MAX_HK_MNM];/*Make 30-original name
					     from Lockheed Database and
					     STANFORD_TLM_HMI_AIA.txt files*/
  char keyword_name[MAX_HK_KYWD]; /*Make size at least 10 - 8 chararacters
				    of less parameter passed to L0P Decode
				    component */
  int start_byte;       /*L0P Decode will use this to parse packet data */
  int start_bit_number;
  int bit_length;       /*L0P Decode will use this to determine bit length
			  to parse packet data */
  char conv_type;       /* type of conversion facTtor */
  char type[MAX_HK_VALUE_TYPE];     /* Make at least 5 -Examples: UB, UL1, i
				       UL2, etc  or 3 characters*/
  struct ALG_Conversion_struct *alg_ptr;
  struct DSC_Conversion_struct *dsc_ptr;
  struct Keyword_Parameter_struct *next;
} Keyword_Parameter;
/****************************************************************************/
/*   Structure used to contain values of filename from directory            */
/****************************************************************************/
typedef struct APID_HKPFD_Files_struct  
{
  char version_number[MAX_CHAR_VERSION_NUMBER];
  int apid; 
  char directory_name[MAX_DIRECTORY_NAME];
  char filename[MAX_FILE_NAME];
  struct APID_HKPFD_Files_struct *next;
}   APID_HKPFD_Files;
/****************************************************************************/
/*   Structure used to contain values of GTCIDS file                        */
/****************************************************************************/
typedef  struct GTCIDS_Version_Number_struct   
{
  char file_version_number[MAX_CHAR_VERSION_NUMBER];
  char hmi_id_version_number[MAX_CHAR_VERSION_NUMBER];
  char aia_id_version_number[MAX_CHAR_VERSION_NUMBER];/*Values in GTCIDS map file */
  char change_date[MAX_SIZE_CHANGE_DATE];
  char change_time[MAX_SIZE_CHANGE_TIME];
  struct GTCIDS_Version_Number_struct  *next;
}   GTCIDS_Version_Number;
/*************************function prototypes*********************************/

APID_HKPFD_Files* read_all_hk_config_files(char file_version_number[]);

void load_config_data( APID_HKPFD_Files* , APID_Pointer_HK_Configs* );
int save_hdpf_new_formats(FILE* file_ptr,APID_Pointer_HK_Configs *ptr);
int update_hdpf_new_formats(FILE* file_ptr, HK_Config_Files *ptr_config_node,
			    HK_Config_Files *top_ptr_config_node); 
int load_hdpf_keyword_lines(char keyword_lines[][], int i, HK_Config_Files *ptr_config_node);
int load_hdpf_dsc_lines(char dsc_lines[][], int j, HK_Config_Files *ptr_config_node);  
int load_hdpf_alg_lines(char alg_lines[][], int k, HK_Config_Files *ptr_config_node);
DSC_Conversion* create_hdpf_dsc_nodes(Keyword_Parameter *ptr_hk_keyword);
Keyword_Parameter* create_hdpf_keyword_nodes(HK_Config_Files *ptr_hk_config_node,int number_of_lines);  
GTCIDS_Version_Number * read_gtcids_hk_file(APID_Pointer_HK_Configs *top_apid_ptr);
int load_all_apids_hk_configs(char version_number[]);
void deallocate_apid_ptr_hk_config_nodes(void);
void deallocate_GTCIDS_Version_Number(void);
HK_Config_Files* check_packet_version_number( HK_Config_Files *top_ptr_to_configs,
						char *version_number );
HK_Config_Files* check_file_version_number( HK_Config_Files *top_ptr_to_configs,
						char *version_number );
HK_Config_Files * reread_all_files(APID_Pointer_HK_Configs *apid_ptr, char version_number[]);
int check_free_configs_flag(void);


#ifndef  LOAD_HK_C
extern APID_Pointer_HK_Configs *gb_top_apid_ptr_hk_configs;
#endif

#endif
