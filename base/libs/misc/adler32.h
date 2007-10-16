#ifndef __ADLER32_H
#define __ADLER32_H
#include <stdio.h>
#include <stdint.h>

uint32_t adler32sum(uint32_t chksum, int len, const uint8_t *data);

#endif
