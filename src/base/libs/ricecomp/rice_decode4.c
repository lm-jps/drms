#include "rice.h"

// The 32-bit wide bit buffer must be wide enough to hold the entire
// low-order bits PLUS bits2go (<=7) bits  ==>  K <= 25

#define KMAX 25		// max number of unencoded low-order bits
#define KBITS 5		// number of bits required to represent K

#define MASK ((1u<<bits2go)-1u)

#define CHECK \
    if (i > nout) return RICE_DECODE_OVERRUN; \
    if (c > in+nin) return RICE_DECODE_TRUNCATED_INPUT;

int rice_decode4(
    const unsigned char *in,	// encoded input byte array
    int nin,			// number of input bytes
    int *out,			// decoded 32-bit integer output array
    int nout,			// number of pixels to decode
    int blksz)			// number of pixels in a coding block
{
    const unsigned char *c;	// pointer into input stream
    unsigned buf;		// bit buffer 
    int bits2go;		// number of bits in buf yet to be decoded 
    int k, i, imax, m;
    unsigned lastpix, diff, fs;

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
    lastpix = (unsigned) *c++ << 24; 
    lastpix += (unsigned) *c++ << 16; 
    lastpix += (unsigned) *c++ << 8; 
    lastpix += *c++;

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
		if (bits2go) {
		    m = 32 - bits2go;	// need m more bits (25 <= m < 32)
		    diff = buf << m;
		    m -= 8;
		    diff += (unsigned) (*c++) << m;
		    m -= 8;
		    diff += (unsigned) (*c++) << m;
		    m -= 8;
		    diff += (unsigned) (*c++) << m;
		    buf = *c++;
		    diff += buf >> bits2go;
		    buf &= MASK;
		} else {
		    // m = 32
		    // avoid shifting by 32 bits (undefined C operation)
		    diff = (unsigned) (*c++) << 24;
		    diff += (unsigned) (*c++) << 16;
		    diff += (unsigned) (*c++) << 8;
		    diff += (unsigned) (*c++);
		    buf = 0;
		}

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
