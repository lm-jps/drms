/* imgstruct.h
 *
*/

#ifndef IMGSTRUCT_INCL
#define IMGSTRUCT_INCL      1

#define MAXPIXELS 16777216	// 4096*4096

typedef struct {
    int valid;			// boolean
    uint32_t FSN;
    uint32_t FID;
    uint32_t APPID;
    int cropid;
    int lookupid;
    int compid;
    int tapcode;
    int nrow, ncol;
    int datavals;
    int missvals;
    int croppedvals;
    int npackets;
    int num_decomp_errors;
    int last_pix_decomp_error;	// boolean
    char last_tlm_filename[128];
    TIME first_packet_time;
    unsigned short data[MAXPIXELS];
} IMG;

/******************************************************************
// Do nothing.
return SUCCESS;

// Close image.
return SUCCESS_IMG_COMPLETE;

// Log error.
return DECOMP_ERROR;

// Log error and close image.
return LAST_PIX_DECOMP_ERROR;

// Do not send me more packets with the same FSN.
return BAD_CROP_ID;
return BAD_LOOKUP_ID;
return BAD_COMP_ID;
return BAD_TAP_CODE;
******************************************************************/
#endif
