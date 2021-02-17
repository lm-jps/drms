#ifndef __IMAGE_INFO__
#define __IMAGE_INFO__

/* compression enum */
enum __CFITSIO_COMPRESSION_TYPE_enum__
{
#if 0
		/* the FITSIO documentation is misleading; you cannot have compression undefined; images are uncompressed by default */
		CFITSIO_COMPRESSION_UNSET = -1,
#endif
		CFITSIO_COMPRESSION_NONE = 0,
    CFITSIO_COMPRESSION_RICE = 1,
    CFITSIO_COMPRESSION_GZIP1 = 2,
    /* cfitsio supports gzip2 only #if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 27)
     * do not introduce FITSIO macros here - we do not want to require a dependency on FITSIO for all
     * DRMS code that includes cfitsio.h or tawrw.h (which is virtually all code); if there is an attempt
     * to use gzip2 in cfitsio code when FITSIO does not support it, an error will occur at runtime */
    CFITSIO_COMPRESSION_GZIP2 = 3,
    CFITSIO_COMPRESSION_PLIO = 4,
    CFITSIO_COMPRESSION_HCOMP = 5
};

typedef enum __CFITSIO_COMPRESSION_TYPE_enum__ CFITSIO_COMPRESSION_TYPE;

struct cfitsio_image_info
{
      // Require keys for re-creating image
      int bitpix;
      int naxis;
#ifdef CFITSIO_API
      long naxes[CFITSIO_MAX_DIM];
#else
      long naxes[9];
#endif
      unsigned int bitfield; /* describes which of the following are present */
      int simple;            /* bit 0 (least significant bit) */
      int extend;            /* bit 1 */
      long long blank;       /* bit 2 */
      double bscale;         /* bit 3 */
      double bzero;          /* bit 4 */
                             /* bit 5 - this bit is the dirty bit; if set then this means that the value of
                              *   naxes[naxis - 1] has changed since the fits file was created; if the value has
                              *   changed, then the NAXISn keyword must be updated when the fits file is closed. */
      char fhash[PATH_MAX];  /* key to fitsfile ptr stored in gFFPtrInfo */
			CFITSIO_COMPRESSION_TYPE export_compression_type; /* used when creating an image only */
};

#endif
