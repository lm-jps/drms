#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/md5.h>

int main(int argc, char *argv[])
{
    int bs, nr, nw, n;
    unsigned char *buf;
    unsigned char md5[16];
    MD5_CTX c;
    FILE *fp;

    if (argc != 3) {
	fprintf(stderr, "Usage: %s block-size checksum_file\n", argv[0]);
	exit(1);
    }

    bs = atoi(argv[1]);
    if (bs < 0) {
	fprintf(stderr, "%s: illegal block size %d\n", argv[0], bs);
	exit(1);
    }
    bs *= 512;
    buf = (unsigned char *) malloc(bs);
    if (! buf) {
	fprintf(stderr, "%s: out of memory\n", argv[0]);
	exit(1);
    }

    fp = fopen(argv[2], "w");
    if (!fp) {
	fprintf(stderr, "%s: can't checksum file %s\n", argv[0], argv[2]);
	exit(1);
    }

    MD5_Init(&c);
    while (nr = read(0, buf, bs)) {
	if (nr < 0) {
	    fprintf(stderr, "%s: read failed\n", argv[0]);
	    exit(1);
	}
	nw = 0;
	do {
	    n = write(1, &buf[nw], nr - nw);
	    if (n < 0) {
		fprintf(stderr, "%s: write failed\n", argv[0]);
		exit(1);
	    }
	    nw += n;
	} while (nw < nr);
	MD5_Update(&c, buf, n);
    }
    MD5_Final(md5, &c);

    for (n=0; n<16; fprintf(fp, "%02x", md5[n++]))
	;
    fprintf(fp, "\n");

    return 0;
}
