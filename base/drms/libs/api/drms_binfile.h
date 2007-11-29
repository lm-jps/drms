#ifndef __DRMS_BINFILE_H
#define __DRMS_BINFILE_H

int drms_binfile_read(char *filename, int nodata, DRMS_Array_t *ar);
int drms_binfile_write(char *filename, DRMS_Array_t *arr);
int drms_zipfile_read(char *filename, int nodata,DRMS_Array_t *ar);
int drms_zipfile_write(char *filename, DRMS_Array_t *rf);

#endif
