#include <stdlib.h>
#include <string.h>
#include "rice.h"

#define KMAX 13		// max number of unencoded low-order bits
#define KBITS 4		// number of bits required to represent K

#define CHECK_OVERRUN \
    if (c > out+bufsz) { free(d); return RICE_ENCODE_OVERRUN; }

#define FLUSH \
    *c++=buf>>24; *c++=buf>>16; *c++=buf>>8; *c++=buf; buf = 0; CHECK_OVERRUN

#define PUT_N_BITS(n, val) \
    if (bits2go > n) { \
	bits2go -= n; \
	buf += val << bits2go; \
    } else if (bits2go == n) { \
	buf += val; \
	FLUSH \
	bits2go = 32; \
    } else { \
	buf += val >> (n - bits2go); \
	FLUSH \
	bits2go = 32 - (n - bits2go); \
	buf = val << bits2go; \
    }

#define PUT_FS(n) \
    if (bits2go > n+1) { \
	bits2go -= n+1; \
	buf += 1 << bits2go; \
    } else if (bits2go == n+1) { \
	++buf; \
	FLUSH \
	bits2go = 32; \
    } else { \
	FLUSH \
	bits2go += 32 - (n+1); \
	while (bits2go < 0) { \
	    c += 4; \
	    bits2go += 32; \
	} \
	buf = 1 << bits2go; \
    }

int rice_encode2(
    const short *in,		// input array of pixels
    int nin,			// number of input pixels
    unsigned char *out,		// encoded output byte array
    int bufsz,			// output buffer size
    int blksz)			// number of pixels in a coding block
{
    unsigned short *d;		// array of 1st differences to be encoded
    unsigned char *c;		// pointer to current byte in output
    unsigned buf = 0;		// bit buffer
    unsigned val;		// value to insert into bit buffer
    int bits2go = 32;		// vacancy in bit buffer
    int k;			// number of low order bits to split
    int i, j;
    short curr, last, delta;
    unsigned short top, bottom, kmask, tmp;
    double pixsum;

    d = (unsigned short *) malloc(blksz * sizeof(unsigned short));
    if (!d) return RICE_ENCODE_OUT_OF_MEMORY;

    memset(out, 0, bufsz);

    // output first pixel raw (most significant byte first)
    last = in[0];
    val = last;
    out[0] = val >> 8;
    out[1] = val;

    c = out + 2;

    // Main loop.  Note that index starts at 0.  (d[0] is always 0.)
    for (i = 0; i < nin; i += blksz) {
	pixsum = 0.0;
	if (nin - i < blksz) blksz = nin - i;
	for (j = 0; j < blksz; ++j) {
	    curr = in[i+j];
	    if (last == curr) {
		d[j] = 0;
		continue;
	    }
	    delta = curr - last;	// overflow OK
	    last = curr;
	    d[j] = (delta < 0) ? ~(delta << 1) : (delta << 1);
	    pixsum += d[j];
	}

	// Zero entropy case: output KBITS zero bits
	if (pixsum == 0.0) {
	    if (bits2go > KBITS)
		bits2go -= KBITS;
	    else {
		FLUSH
		bits2go += 32 - KBITS;
	    }
	    continue;
	}

	// Find k. 
	tmp = .5*pixsum/blksz;
	for (k=0; tmp; ++k) tmp >>= 1;

	// Small entropy case, k == 0
	if (k == 0) {
	    val = 1;
	    PUT_N_BITS(KBITS, val)
	    for (j = 0; j < blksz; ++j) {
		val = d[j];
		PUT_FS(val)
	    }
	// Medium entropy case, k <= KMAX
	} else if (k <= KMAX) {
	    val = k+1;
	    PUT_N_BITS(KBITS, val)
	    kmask = (1u<<k) - 1u;
	    for (j = 0; j < blksz; ++j) {
		val = d[j];
		top = val >> k;
		bottom = val & kmask;
		PUT_FS(top)
		PUT_N_BITS(k, bottom)
	    }
	// High entropy case, k > KMAX
	} else {
	    val = KMAX+2;
	    PUT_N_BITS(KBITS, val)
	    for (j = 0; j < blksz; ++j) {
		val = d[j];
		PUT_N_BITS(16, val);
	    }
	}
    }

    free(d);

    // final flush
    i = 4 - bits2go/8;
    if (i-- > 0) *c++ = buf >> 24;
    if (i-- > 0) *c++ = buf >> 16;
    if (i-- > 0) *c++ = buf >> 8;
    if (i-- > 0) *c++ = buf;

    return (c-out <= bufsz) ? c-out : RICE_ENCODE_OVERRUN;
}
