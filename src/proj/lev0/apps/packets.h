#ifndef __PACKETS_H
#define __PACKETS_H

#include <stdint.h>


#define PACKETHEADERWORDS   (19) 
#define PACKETHEADERBYTES (2*19) 
#define PACKETHEADERBITS (16*19) 

#define PACKETDATAWORDS   (869) 
#define PACKETDATABYTES (2*869) 
#define PACKETDATABITS (16*869) 


#define PACKETWORDS  (PACKETHEADERWORDS+PACKETDATAWORDS) 
#define PACKETBYTES  (PACKETHEADERBYTES+PACKETDATABYTES) 
#define PACKETBITS   (PACKETHEADERBITS+PACKETDATABITS) 




/*************** Type definitions *******************/

/* Keyword value types. */
typedef enum {DRMS_TYPE_INT8, DRMS_TYPE_INT16, DRMS_TYPE_INT32, 
              DRMS_TYPE_INT64, DRMS_TYPE_UINT8, DRMS_TYPE_UINT16, 
              DRMS_TYPE_UINT32, DRMS_TYPE_UINT64, 
              DRMS_TYPE_FLOAT, DRMS_TYPE_DOUBLE, 
              DRMS_TYPE_TIME, DRMS_TYPE_STRING} DRMS_Type_t;

typedef union DRMS_Type_Value
{
  int8_t   int8_val;
  int16_t  int16_val;
  int32_t  int32_val;
  int64_t  int64_val;
  uint8_t  uint8_val;
  uint16_t uint16_val;
  uint32_t uint32_val;
  uint64_t uint64_val;
  float   float_val;
  double  double_val;
  double  time_val;
  char   *string_val;
} DRMS_Type_Value_t;


/************* Keyword struct ****************/
#define MAX_KEYWORD_NAME_SIZE 64
typedef struct HK_Keyword_struct {
     char           name[MAX_KEYWORD_NAME_SIZE]; 
     char           fitsname[9];
     int64_t        raw_value;    /* 64 bit integer should be able to
                                     hold any header field */
     DRMS_Type_t       eng_type;  /* Engineering value type. */
     DRMS_Type_Value_t eng_value; /* Engineering value. */
     struct HK_Keyword_struct  *next;
} HK_Keyword_t;


/******************* VCDU Packet struct ********************/
typedef struct IM_PDU_Packet_struct 
{
  /* IM_PDU Header fields. */
  uint8_t  im_pdu_id;
  uint64_t im_pdu_count;
  /* M_PDU header */
  uint8_t  spare;
  uint16_t fhp;

  /* Linked list of CCSDS packets. */
  struct CCSDS_Packet_struct *packets;

} IM_PDU_Packet_t;


/******************* CCSDS Packet struct ********************/
typedef struct CCSDS_Packet_struct 
{
  /* CCSDS Header fields. */
  unsigned char  version;
  unsigned char  type;
  unsigned char  shf;
  unsigned short apid;
  unsigned char  sf;
  unsigned short ssc;
  unsigned short length;

  /* Decoded Keywords. */
  HK_Keyword_t *keywords;

  /* Next pointer */
  struct CCSDS_Packet_struct *next;
} CCSDS_Packet_t;


/* Telemetry packet consisting of (possibly byteswapped) raw data and
   parsed header. */
typedef struct SciDataPacket_struct
{  
  /* Science packet headers. */
  unsigned int   shs;
  unsigned int   shss;
  unsigned short ccdhead[4];
  short cropid;
  unsigned char  romode;
  unsigned char  headererr;
  unsigned char  oflow;
// TAP codes
//  H--G     0 - EFGH
//  |  |     1 - EF   2 - FG   3 - GH   4 - HE
//  E--F     5 - E    6 - F    7 - G    8 - H
  unsigned char  tapcode;
  unsigned char  bitselectid;
  unsigned char  compid;
  unsigned char  lutid;
  unsigned int   offset;

  unsigned short *data;
} SciDataPacket_t;



/******************** Prototypes for functions. **********/

/* Housekeeping VCDU decoder to be written by Rasmus. */
int decode_hk_packets(unsigned short *ptr, CCSDS_Packet_t **pk_head);

/* Decoder for single CCSCS HK packet to be written by Carl. */
int decode_hk_keywords(unsigned short *ptr, int apid, HK_Keyword_t **kw_head);


#endif
