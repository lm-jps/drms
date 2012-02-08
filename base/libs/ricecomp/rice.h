#ifndef _RICE_H_
#define _RICE_H_

#define RICE_ENCODE_OUT_OF_MEMORY	-1
#define RICE_ENCODE_OVERRUN		-2
#define RICE_DECODE_OVERRUN		-3
#define RICE_DECODE_TRUNCATED_INPUT	-4
#define RICE_DECODE_TRAILING_GARBAGE	-5

extern int rice_encode1(const char *in, int nin, unsigned char *out, int bufsz, int blksz);
extern int rice_encode2(const short *in, int nin, unsigned char *out, int bufsz, int blksz);
extern int rice_encode4(const int *in, int nin, unsigned char *out, int bufsz, int blksz);
extern int rice_decode1(const unsigned char *in, int nin, char *out, int nout, int blksz);
extern int rice_decode2(const unsigned char *in, int nin, short *out, int nout, int blksz);
extern int rice_decode4(const unsigned char *in, int nin, int *out, int nout, int blksz);

#endif	// _RICE_H_
