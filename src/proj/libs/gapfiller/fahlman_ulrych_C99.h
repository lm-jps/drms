#ifndef FAHLMAN_ULRYCH_C99_H_DEF
#define FAHLMAN_ULRYCH_C99_H_DEF

int sfahlman_ulrych(int n, float *data, int *isgood, int minpercentage, 
		    int maxorder, int iterations, int padends, int *order, 
		    float *ar_coeff);
int dfahlman_ulrych(int n, double *data, int *isgood, int minpercentage, 
		    int maxorder, int iterations, int padends, int *order, 
		    double *ar_coeff);
int cfahlman_ulrych(int n, _Complex float *data, int *isgood, 
		    int minpercentage, int maxorder, int iterations, 
		    int padends, int *order, _Complex float *ar_coeff);
int zfahlman_ulrych(int n, _Complex double *data, int *isgood, 
		    int minpercentage, int maxorder, int iterations, 
		    int padends,int *order, _Complex double *ar_coeff);
#endif
