#ifndef DETREND_C99_H_DEF
#define DETREND_C99_H_DEF
extern void sdetrend_discontig( int n, float *data, int *isgood, int degree, 
				int length, int skip, int m, int *sect_last);
extern void ddetrend_discontig( int n, double *data, int *isgood, int degree, 
				int length, int skip, int m, int *sect_last);
extern void cdetrend_discontig( int n, _Complex float *data, int *isgood, 
				int degree, int length, int skip, 
				int m, int *sect_last);
extern void zdetrend_discontig( int n, _Complex double *data, int *isgood, 
				int degree, int length, int skip, 
				int m, int *sect_last);


extern void sdetrend( int n, float *data, int *isgood, int degree, 
		      int length, int skip);
extern void ddetrend( int n, double *data, int *isgood, int degree, 
		      int length, int skip);
extern void cdetrend( int n, _Complex float *data, int *isgood, int degree, 
		      int length, int skip);
extern void zdetrend( int n, _Complex double *data, int *isgood, int degree, 
		      int length, int skip);

#endif
