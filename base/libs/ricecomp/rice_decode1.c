#include "rice.h"

#define KMAX 5		// max number of unencoded low-order bits
#define KBITS 3		// number of bits required to represent K

#define MASK ((1u<<bits2go)-1u)

#define CHECK \
    if (i > nout) return RICE_DECODE_OVERRUN; \
    if (c > in+nin) return RICE_DECODE_TRUNCATED_INPUT;

int rice_decode1(
    const unsigned char *in,	// encoded input byte array
    int nin,			// number of input bytes
    char *out,			// decoded 8-bit integer output array
    int nout,			// number of pixels to decode
    int blksz)			// number of pixels in a coding block
{
    const unsigned char *c;	// pointer into input stream
    unsigned buf;		// bit buffer 
    int bits2go;		// number of bits in buf yet to be decoded 
    int k, i, imax, m;
    unsigned char lastpix, diff, fs;

    // numbers of bits needed to represent 0,1,...,255
    static int bitcount[256] = {
	0,1,2,2,3,3,3,3,4,4,4,4,4,4,4,4,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,
	6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
	7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
	7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
	8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,
	8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,
	8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,
	8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8 };
 
    // get first pixel
    c = in;
    lastpix = *c++;

    buf = *c++;
    bits2go = 8;

    // Main loop
    for (i=0; i<nout; ) {
	// read k
	bits2go -= KBITS;
	if (bits2go < 0) {
	    buf = (buf<<8) + *c++;
	    bits2go += 8;
	}
	k = (buf>>bits2go) - 1;
	buf &= MASK;

	imax = i + blksz;
	if (imax > nout) imax = nout;

	// zero entropy case
	if (k < 0) {
	    while (i < imax)
		out[i++] = lastpix;
	    CHECK

	// normal case
	} else if (k <= KMAX) {
	    while (i < imax) {
		// count zero bits to get fs
		while (buf == 0) {
		    bits2go += 8;
		    buf = *c++;
		}
		fs = bits2go - bitcount[buf];
		bits2go -= fs+1;
		buf ^= 1<<bits2go;	// flip top 1-bit
		bits2go -= k;		// read k bits
		while (bits2go < 0) {
		    buf = (buf<<8) + *c++;
		    bits2go += 8;
		}
		diff = (fs<<k) + (buf>>bits2go);
		buf &= MASK;

		// reverse mapping
		if (diff & 1) diff = ~(diff >> 1);
		else diff >>= 1;

		out[i] = lastpix + diff;
		lastpix = out[i++];
	    }
	    CHECK

	// high entropy case
	} else if (k == KMAX+1) {
	    while (i < imax) {
		m = 8 - bits2go;	// need m more bits (1 <= m <= 8)
		diff = buf << m;
		buf = *c++;
		diff += buf >> bits2go;
		buf &= MASK;

		// reverse mapping
		if (diff & 1) diff = ~(diff >> 1);
		else diff >>= 1;

		out[i] = lastpix + diff;
		lastpix = out[i++];
	    }
	    CHECK
	}
    }

    return (c == in+nin) ? 0 : RICE_DECODE_TRAILING_GARBAGE;
}
