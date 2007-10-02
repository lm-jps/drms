#ifndef REJECT_C99_H_DEF
#define REJECT_C99_H_DEF

void sreject(int n, float *data, int *isgood, float factor);
void dreject(int n, double *data, int *isgood, double factor);
void creject(int n, _Complex float *data, int *isgood, float factor);
void zreject(int n, _Complex double *data, int *isgood, double factor);

#endif
