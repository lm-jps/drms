#include <stdio.h>
#include <stdint.h>
#include "adler32.h"

#define MOD_ADLER 65521
/* Compute Adler32 checksum of data in array. 

   To start a new checksum call with chksum=1. To continue a running checksum 
   call with chksum equal to the value returned by the previous call of
   adler32. 
*/


uint32_t adler32sum(uint32_t chksum, int len, const uint8_t *data)
{
  uint32_t a, b;

  a = chksum  & 0xffff;
  b = (chksum >> 16 ) & 0xffff;
  while (len) {
    unsigned int tlen = len > 5550 ? 5550 : len;
    len -= tlen;
    do {
      a += *data++;
      b += a;
    } while (--tlen);
    a = (a & 0xffff) + (a >> 16) * (65536-MOD_ADLER);
    b = (b & 0xffff) + (b >> 16) * (65536-MOD_ADLER);
  }
  /* It can be shown that a <= 0x1013a here, so a single subtract will do. */
  if (a >= MOD_ADLER)
    a -= MOD_ADLER;
  /* It can be shown that b can reach 0xffef1 here. */
  b = (b & 0xffff) + (b >> 16) * (65536-MOD_ADLER);
  if (b >= MOD_ADLER)
    b -= MOD_ADLER;
  return b << 16 | a;
}

