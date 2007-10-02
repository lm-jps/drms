#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fftw3.h>
#include "fftw_convolve.h"


/****************** 1-D routines *****************/

fftw_convolve_op *dfftw_convolve_1d_prepare(int reva, int na, int nb, 
					    double *a, double *padb, 
					    fftw_complex *padc)
{
  int i,N=na+nb-1;
  fftw_convolve_op *op;
  fftw_plan p;

  /* Allocate arrays and set up FFTW plan. */  
  op = (fftw_convolve_op *) malloc(sizeof(fftw_convolve_op));
  op->na = na;
  op->nb = nb;
  op->N = N;
  op->ma = 1;
  op->mb = 1;
  op->M = 1;
  op->scale = 1.0/op->N;
  op->pada = fftw_malloc(sizeof(double) * 2*(N/2+1));
  if (padb==NULL)
  {
    op->padb = fftw_malloc(sizeof(double) * 2*(N/2+1));
    op->freeb = 1;
  }
  else
  {
    op->padb = padb;
    op->freeb = 0;
  }
  if (padc==NULL)
  {
    op->cc = fftw_malloc(sizeof(fftw_complex) * (N/2+1));
    op->freec = 1;
  }
  else
  {
    op->cc = padc;
    op->freec = 0;
  }
#pragma omp critical
{
  p = fftw_plan_dft_r2c_1d(N, op->pada, (fftw_complex *)(op->pada), 
			   FFTW_MEASURE);
  op->q = fftw_plan_dft_r2c_1d(N, op->padb, (fftw_complex *)(op->padb), 
			       FFTW_MEASURE);
  op->s = fftw_plan_dft_c2r_1d(N, op->cc, (double *)(op->cc), 
			       FFTW_MEASURE);
}

  /* Initialize padded input arrays. */
  if (!reva)
    memcpy(op->pada, a, na*sizeof(double));
  else
    for (i=0; i<na; i++)    
      op->pada[i] = a[na-1-i];
  memset(&(op->pada[na]), 0, (2*(N/2+1)-na)*sizeof(double));

  /* Compute the non-redundant parts of the DFT of a. */
  fftw_execute(p); 

  /* Free FFTW plan after use. */
  fftw_destroy_plan(p);
  

  return op;
}


void dfftw_convolve_1d_execute(fftw_convolve_op *op, int revb,
			       double *b, double *c)
{
  int i;
  int N,nb;
  double scale,*pada, *padb, *rc;
  fftw_complex *cc, *ca, *cb;
  fftw_plan q,s;

  nb = op->nb;
  N = op->N;
  scale = op->scale;
  pada = op->pada;
  padb = op->padb;
  cc = op->cc;
  q = op->q;
  s = op->s;

  /* Initialize padded input arrays. */
  ca = (fftw_complex *) pada;
  cb = (fftw_complex *) padb;
  if (!revb)
    memcpy(padb, b, nb*sizeof(double));
  else
    for (i=0; i<nb; i++)    
      padb[i] = b[nb-1-i];
  memset(&padb[nb], 0, (2*(N/2+1)-nb)*sizeof(double));

  rc = (double *) cc;

  /* Compute the non-redundant parts of the DFT of a and b. */
  fftw_execute(q); 

  /* Complex multiply in Fourier space. */
  for (i=0; i<N/2+1; i++)
  {
    double ra, ia, rb,ib;
    ra = ca[i][0];
    ia = ca[i][1];
    rb = cb[i][0];
    ib = cb[i][1];
    
    cc[i][0] = ra*rb - ia*ib;
    cc[i][1] = ra*ib + ia*rb;
  }
  /* Compute the convolution by inverse DFT. */
  fftw_execute(s); 

  /* Scale. */
  for (i=0; i<N; i++)  
    c[i] = scale*rc[i];
}

void dfftw_convolve_1d_free(fftw_convolve_op *op)
{
  if (op==NULL) 
    return;
  //#pragma omp critical
  //  {
    if (op->q)
    {
      fftw_destroy_plan(op->q);
      op->q = NULL;
    }
    if (op->s)
    {
      fftw_destroy_plan(op->s);
      op->s = NULL;
    }
    //  }
  if (op->pada) {
    fftw_free(op->pada); 
    op->pada = NULL;
  }
  if (op->freeb && op->padb)
  {
    fftw_free(op->padb); 
    op->padb = NULL;
  }
  if (op->freec && op->cc)   
  {
    fftw_free(op->cc);
    op->cc = NULL;
  }
  free(op);
}




void dfftw_convolve_1d(int reva, int na, double *a, int revb, int nb, 
		       double *b, double *c)
{
  int i;
  int N = na+nb-1;
  double scale=1.0/N, *pada, *padb, *rc;
  fftw_complex *cc, *ca, *cb;
  fftw_plan p,q,s;

  /* Allocate arrays and set up FFTW plan. */
  pada = fftw_malloc(sizeof(double) * 2*(N/2+1));
  padb = fftw_malloc(sizeof(double) * 2*(N/2+1));
  cc = fftw_malloc(sizeof(fftw_complex) * (N/2+1));
#pragma omp critical
{
  p = fftw_plan_dft_r2c_1d(N, pada, (fftw_complex *)pada, FFTW_MEASURE);
  q = fftw_plan_dft_r2c_1d(N, padb, (fftw_complex *)padb, FFTW_MEASURE);
  s = fftw_plan_dft_c2r_1d(N, cc, (double *)cc, FFTW_MEASURE);
}
  /* Initialize padded input arrays. */
  ca = (fftw_complex *) pada;
  if (!reva)
    memcpy(pada, a, na*sizeof(double));
  else
    for (i=0; i<na; i++)    
      pada[i] = a[na-1-i];
  memset(&pada[na], 0, (2*(N/2+1)-na)*sizeof(double));

  cb = (fftw_complex *) padb;
  if (!revb)
    memcpy(padb, b, nb*sizeof(double));
  else
    for (i=0; i<nb; i++)    
      padb[i] = b[nb-1-i];
  memset(&padb[nb], 0, (2*(N/2+1)-nb)*sizeof(double));

  rc = (double *) cc;

  /* Compute the non-redundant parts of the DFT of a and b. */
  fftw_execute(p); 
  fftw_execute(q); 

  /* Complex multiply in Fourier space. */
  for (i=0; i<N/2+1; i++)
  {
    double ra, ia, rb,ib;
    ra = ca[i][0];
    ia = ca[i][1];
    rb = cb[i][0];
    ib = cb[i][1];
    
    cc[i][0] = ra*rb - ia*ib;
    cc[i][1] = ra*ib + ia*rb;
  }
  /* Compute the convolution by inverse DFT. */
  fftw_execute(s); 

  /* Scale. */
  for (i=0; i<N; i++)  
    c[i] = scale*rc[i];

  //#pragma omp critical
  //{
  fftw_destroy_plan(p);
  fftw_destroy_plan(q);
  fftw_destroy_plan(s);
  //}
  fftw_free(pada); 
  fftw_free(padb); 
  fftw_free(cc);
}


/****************** 2-D routines *****************/
fftw_convolve_op *dfftw_convolve_2d_prepare(int ma, int na, 
					    int mb, int nb, double *a,
					    double *padb, fftw_complex *padc)
{
  fftw_convolve_op *op;
  double scale;
  int i,j;
  int N = na+nb-1, M = ma+mb-1;
  fftw_plan p;

  /* Allocate arrays and set up FFTW plan. */  
  op = (fftw_convolve_op *) malloc(sizeof(fftw_convolve_op));
  op->na = na;
  op->nb = nb;
  op->N = N;
  op->ma = ma;
  op->mb = mb;
  op->M = M;
  op->pada = fftw_malloc(sizeof(double) * N*2*(M/2+1));
  if (padb==NULL)
  {
    op->padb = fftw_malloc(sizeof(double) * N*2*(M/2+1));
    op->freeb = 1;
  }
  else
  {
    op->padb = padb;  
    op->freeb = 0;
  }
  if (padc==NULL)
  {
    op->cc = fftw_malloc(sizeof(fftw_complex) * N*(M/2+1));
    op->freec = 1;
  }
  else
  {
    op->cc = padc;
    op->freec = 0;
  }
#pragma omp critical
 {
  p = fftw_plan_dft_r2c_2d(N, M, op->pada, (fftw_complex *)(op->pada), 
			       FFTW_MEASURE);
  op->q = fftw_plan_dft_r2c_2d(N, M, op->padb, (fftw_complex *)(op->padb), 
			       FFTW_MEASURE);
  op->s = fftw_plan_dft_c2r_2d(N, M, op->cc, (double *)(op->cc), 
			       FFTW_MEASURE);
 }

  /* Initialize padded input arrays. */
  scale = 1.0/(M*N);
  for (i=0; i<na; i++)
  {
    for (j=0; j<ma; j++)
      op->pada[i*2*(M/2+1)+j] = scale*a[i*ma+j];
    memset(&(op->pada[i*2*(M/2+1)+ma]),0,sizeof(double)*(2*(M/2+1)-ma));
  }      
  for (i=na; i<N; i++) 
    memset(&(op->pada[i*2*(M/2+1)]),0,sizeof(double)*2*(M/2+1));

  op->scale = 1.0;

  /* Compute the non-redundant parts of the DFT of a. */
  fftw_execute(p); 

  /* Free FFTW plan after use. */
  //#pragma omp critical
  fftw_destroy_plan(p);

  return op;
}


void dfftw_convolve_2d_execute(fftw_convolve_op *op, int shape, int reva, 
			       int revb, double *b, double alpha, 
			       double beta, double *c)
{
  int i,j, ii,jj;
  int N, M;
  int na,nb,ma,mb;
  double *pada, *padb, *rc;
  fftw_complex *cc, *ca, *cb;
  fftw_plan q,s;

  na = op->na;
  nb = op->nb;
  N = op->N;
  ma = op->ma;
  mb = op->mb;
  M = op->M;
  pada = op->pada;
  padb = op->padb;
  cc = op->cc;
  q = op->q;
  s = op->s;

  /* Initialize padded input arrays. */
  for (i=0; i<nb; i++)
  {
    if ((!reva && revb) || (reva && !revb)) 
      for (j=0; j<mb; j++)
	padb[i*2*(M/2+1)+j] = b[(nb-1-i)*mb+(mb-1-j)];
    else
      memcpy(&padb[i*2*(M/2+1)], &b[i*mb], mb*sizeof(double));
    memset(&padb[i*2*(M/2+1)+mb],0,sizeof(double)*(2*(M/2+1)-mb));
  }      
  for (i=nb; i<N; i++) 
    memset(&padb[i*2*(M/2+1)],0,sizeof(double)*2*(M/2+1));

  /* Compute the non-redundant parts of the DFT of a and b. */
  fftw_execute(q); 

  /* Complex multiply in Fourier space. */
  ca = (fftw_complex *) pada;
  cb = (fftw_complex *) padb;
  for (j=0; j<N; j++)
    for (i=0; i<(M/2+1); i++)
    {
      double ra, ia, rb,ib;
      ra = ca[j*(M/2+1)+i][0];
      ia = ca[j*(M/2+1)+i][1];
      rb = cb[j*(M/2+1)+i][0];
      ib = cb[j*(M/2+1)+i][1];

      cc[j*(M/2+1)+i][0] = ra*rb - ia*ib;
      cc[j*(M/2+1)+i][1] = ra*ib + ia*rb;
    }

  /* Compute the convolution by inverse DFT. */
  fftw_execute(s); 

  /* Scale and copy requested part of output to array c. */
  rc = (double *) cc;

  switch(shape)
  {
  case FFTW_CONV_SHAPE_FULL:
    if (!reva)
    {
      if (beta==0.0 && alpha==1.0)
	for (j=0; j<N; j++)
	  for (i=0; i<M; i++)  
	    c[j*M+i] = rc[2*j*(M/2+1)+i];
      else if (beta==1.0 && alpha==1.0)
	for (j=0; j<N; j++)
	  for (i=0; i<M; i++)  
	    c[j*M+i] += rc[2*j*(M/2+1)+i];
      else
	for (j=0; j<N; j++)
	  for (i=0; i<M; i++)  
	    c[j*M+i] = alpha*rc[2*j*(M/2+1)+i] + beta*c[j*M+i];
    }
    else
    {
      if (beta==0.0 && alpha==1.0)
	for (j=0; j<N; j++)
	  for (i=0; i<M; i++)  
	    c[j*M+i] = rc[2*(N-j-1)*(M/2+1)+(M-i-1)];
      else if (beta==1.0 && alpha==1.0)
	for (j=0; j<N; j++)
	  for (i=0; i<M; i++)  
	    c[j*M+i] += rc[2*(N-j-1)*(M/2+1)+(M-i-1)];
      else
	for (j=0; j<N; j++)
	  for (i=0; i<M; i++)  
	    c[j*M+i] = alpha*rc[2*(N-j-1)*(M/2+1)+(M-i-1)] + beta*c[j*M+i];
    }
    break;
  case FFTW_CONV_SHAPE_AS_A:
    if (!reva)
    {
      if (beta==0.0 && alpha==1.0)
	for (jj=0, j=nb/2; j<na+nb/2; j++, jj++)
	  for (ii=0, i=mb/2; i<ma+mb/2; i++, ii++)
	    c[jj*ma+ii] = rc[2*j*(M/2+1)+i];
      else if (beta==1.0 && alpha==1.0)
	for (jj=0, j=nb/2; j<na+nb/2; j++, jj++)
	  for (ii=0, i=mb/2; i<ma+mb/2; i++, ii++)
	    c[jj*ma+ii] += rc[2*j*(M/2+1)+i];
      else
	for (jj=0, j=nb/2; j<na+nb/2; j++, jj++)
	  for (ii=0, i=mb/2; i<ma+mb/2; i++, ii++)
	    c[jj*ma+ii] = alpha*rc[2*j*(M/2+1)+i] + beta*c[jj*ma+ii];
    }
    else
    {
      if (beta==0.0 && alpha==1.0)
	for (jj=0, j=nb/2; j<na+nb/2; j++, jj++)
	  for (ii=0, i=mb/2; i<ma+mb/2; i++, ii++)
	    c[jj*ma+ii] = rc[2*(N-j-1)*(M/2+1)+(M-i-1)];
      else if (beta==1.0 && alpha==1.0)
	for (jj=0, j=nb/2; j<na+nb/2; j++, jj++)
	  for (ii=0, i=mb/2; i<ma+mb/2; i++, ii++)
	    c[jj*ma+ii] += rc[2*(N-j-1)*(M/2+1)+(M-i-1)];
      else
	for (jj=0, j=nb/2; j<na+nb/2; j++, jj++)
	  for (ii=0, i=mb/2; i<ma+mb/2; i++, ii++)
	    c[jj*ma+ii] = alpha*rc[2*(N-j-1)*(M/2+1)+(M-i-1)] + beta*c[jj*ma+ii];
    }
    break;
  case FFTW_CONV_SHAPE_AS_B:
    if (!reva)
    {
      if (beta==0.0 && alpha==1.0)
	for (jj=0, j=na/2; j<na/2+nb; j++, jj++)
	  for (ii=0, i=ma/2; i<ma/2+mb; i++, ii++)
	    c[jj*mb+ii] = rc[2*j*(M/2+1)+i];
      else if (beta==1.0 && alpha==1.0)
	for (jj=0, j=na/2; j<na/2+nb; j++, jj++)
	  for (ii=0, i=ma/2; i<ma/2+mb; i++, ii++)
	    c[jj*mb+ii] += rc[2*j*(M/2+1)+i];
      else
	for (jj=0, j=na/2; j<na/2+nb; j++, jj++)
	  for (ii=0, i=ma/2; i<ma/2+mb; i++, ii++)
	    c[jj*mb+ii] = alpha*rc[2*j*(M/2+1)+i] + beta*c[jj*mb+ii];
    }
    else
    {
      if (beta==0.0 && alpha==1.0)
	for (jj=0, j=na/2; j<na/2+nb; j++, jj++)
	  for (ii=0, i=ma/2; i<ma/2+mb; i++, ii++)
	    c[jj*mb+ii] = rc[2*(N-j-1)*(M/2+1)+(M-i-1)];
      else if (beta==1.0 && alpha==1.0)
	for (jj=0, j=na/2; j<na/2+nb; j++, jj++)
	  for (ii=0, i=ma/2; i<ma/2+mb; i++, ii++)
	    c[jj*mb+ii] += rc[2*(N-j-1)*(M/2+1)+(M-i-1)];
      else
	for (jj=0, j=na/2; j<na/2+nb; j++, jj++)
	  for (ii=0, i=ma/2; i<ma/2+mb; i++, ii++)
	    c[jj*mb+ii] = alpha*rc[2*(N-j-1)*(M/2+1)+(M-i-1)] + beta*c[jj*mb+ii];
    }
    break;
  default:
    fprintf(stderr,"%s:%d: ERROR: Unknown shape parameter %d.\n",
	    __FILE__,__LINE__,shape);
    abort();
    break;
  }
}


void dfftw_convolve_2d_free(fftw_convolve_op *op)
{   
  dfftw_convolve_1d_free(op);
}

void dfftw_convolve_2d(int shape, 
		       int reva, int ma, int na, double *a, 
		       int revb, int mb, int nb, double *b, 
		       double alpha, double beta, double *c)
{
  int i,j, ii, jj;
  int N = na+nb-1, M = ma+mb-1;
  double scale=alpha/(N*M), *pada, *padb, *rc;
  fftw_complex *cc, *ca, *cb;
  fftw_plan p,q,s;


  /* Allocate arrays and set up FFTW plan. */
  pada = fftw_malloc(sizeof(double) * N*2*(M/2+1));
  padb = fftw_malloc(sizeof(double) * N*2*(M/2+1));
  cc = fftw_malloc(sizeof(fftw_complex) * N*(M/2+1));
#pragma omp critical
 {
  p = fftw_plan_dft_r2c_2d(N, M, pada, (fftw_complex *)pada, FFTW_MEASURE);
  q = fftw_plan_dft_r2c_2d(N, M, padb, (fftw_complex *)padb, FFTW_MEASURE);
  s = fftw_plan_dft_c2r_2d(N, M, cc, (double *)cc, FFTW_MEASURE);
 }
  /* Initialize padded input arrays. */
  for (i=0; i<na; i++)
  {
    if (!reva) 
      memcpy(&pada[i*2*(M/2+1)], &a[i*ma], ma*sizeof(double));
    else
      for (j=0; j<ma; j++)
	pada[i*2*(M/2+1)+j] = a[(na-1-i)*ma+(ma-1-j)];
    memset(&pada[i*2*(M/2+1)+ma],0,sizeof(double)*(2*(M/2+1)-ma));
  }      
  for (i=na; i<N; i++) 
    memset(&pada[i*2*(M/2+1)],0,sizeof(double)*2*(M/2+1));

  for (i=0; i<nb; i++)
  {
    if (!revb) 
      memcpy(&padb[i*2*(M/2+1)], &b[i*mb], mb*sizeof(double));
    else
      for (j=0; j<mb; j++)
	padb[i*2*(M/2+1)+j] = b[(nb-1-i)*mb+(mb-1-j)];
    memset(&padb[i*2*(M/2+1)+mb],0,sizeof(double)*(2*(M/2+1)-mb));
  }      
  for (i=nb; i<N; i++) 
    memset(&padb[i*2*(M/2+1)],0,sizeof(double)*2*(M/2+1));


  /* Compute the non-redundant parts of the DFT of a and b. */
  fftw_execute(p); 
  fftw_execute(q); 

  /* Complex multiply in Fourier space. */
  ca = (fftw_complex *) pada;
  cb = (fftw_complex *) padb;
  for (j=0; j<N; j++)
    for (i=0; i<(M/2+1); i++)
    {
      double ra, ia, rb,ib;
      ra = ca[j*(M/2+1)+i][0];
      ia = ca[j*(M/2+1)+i][1];
      rb = cb[j*(M/2+1)+i][0];
      ib = cb[j*(M/2+1)+i][1];

      cc[j*(M/2+1)+i][0] = ra*rb - ia*ib;
      cc[j*(M/2+1)+i][1] = ra*ib + ia*rb;
    }

  /* Compute the convolution by inverse DFT. */
  fftw_execute(s); 

  /* Scale and copy requested part of output to array c. */
  rc = (double *) cc;
  switch(shape)
  {
  case FFTW_CONV_SHAPE_FULL:
    for (j=0; j<N; j++)
      for (i=0; i<M; i++)  
	c[j*M+i] = scale*rc[2*j*(M/2+1)+i];
    break;
  case FFTW_CONV_SHAPE_AS_A:
    for (jj=0, j=nb/2; j<na+nb/2; j++, jj++)
      for (ii=0, i=mb/2; i<ma+mb/2; i++, ii++)
	c[jj*ma+ii] = scale*rc[2*j*(M/2+1)+i];
    break;
  case FFTW_CONV_SHAPE_AS_B:
    for (jj=0, j=na/2; j<na/2+nb; j++, jj++)
      for (ii=0, i=ma/2; i<ma/2+mb; i++, ii++)
	c[jj*mb+ii] = scale*rc[2*j*(M/2+1)+i];
    break;
  default:
    fprintf(stderr,"%s:%d: ERROR: Unknown shape parameter %d.\n",
	    __FILE__,__LINE__,shape);
    abort();
    break;
  }

  //#pragma omp critical
 {    
  fftw_destroy_plan(p);
  fftw_destroy_plan(q);
  fftw_destroy_plan(s);
 }
  fftw_free(pada); 
  fftw_free(padb); 
  fftw_free(cc); 
}

/* Double precision FORTRAN interface */
/* 1-D */
void dfftw_corr_1d_(int *na, double *a, int *nb, double *b, double *c)
{
  dfftw_corr_1d(*na, a, *nb, b, c);
}

void dfftw_conv_1d_(int *na, double *a, int *nb, double *b, double *c)
{
  dfftw_conv_1d(*na, a, *nb, b, c);
}

void dfftw_convolve_1d_prepare_(fftw_convolve_op **op, int *reva, 
				int *na, int *nb, double *a,
				double **padb, fftw_complex **padc)
{
  *op = dfftw_convolve_1d_prepare(*reva, *na, *nb, a, *padb, *padc);
}
  
void dfftw_convolve_1d_execute_(fftw_convolve_op **op, int *revb, 
				double *b, double *c)
{
  dfftw_convolve_1d_execute(*op, *revb, b, c);
}

void dfftw_convolve_1d_free_(fftw_convolve_op **op)
{
  dfftw_convolve_1d_free(*op);
}

void dfftw_convolve_1d_(int *reva, int *na, double *a, int *revb, int *nb, 
			double *b, double *c)
{
  dfftw_convolve_1d(*reva, *na, a, *revb, *nb, b, c);
}

/* 2-D */
void dfftw_corr_2d_(int *shape, int *ma, int *na, double *a, int *mb, int *nb, 
		    double *b, double *c)
{
  dfftw_corr_2d(*shape, *ma, *na, a, *mb, *nb, b, c);
}

void dfftw_conv_2d_(int *shape, int *ma, int *na, double *a, int *mb, int *nb, 
		    double *b, double *c)
{
  dfftw_conv_2d(*shape, *ma, *na, a, *mb, *nb, b, c);
}

void dfftw_convolve_2d_prepare_(fftw_convolve_op **op, int *ma, 
				int *na, int *mb, int *nb, double *a,
				double **padb, fftw_complex **padc)
{
  *op = dfftw_convolve_2d_prepare(*ma, *na, *mb, *nb, a, *padb, *padc);
}
  
void dfftw_convolve_2d_execute_(fftw_convolve_op **op, int *shape, 
				int *reva, int *revb, 
				double *b, double *alpha, double *beta,
				double *c)
{
  dfftw_convolve_2d_execute(*op, *shape, *reva, *revb, b, *alpha, *beta, c);
}

void dfftw_convolve_2d_free_(fftw_convolve_op **op)
{
  dfftw_convolve_2d_free(*op);
}

void dfftw_convolve_2d_(int *shape, 
			int *reva, int *ma, int *na, double *a, 
			int *revb, int *mb, int *nb, double *b, 
			double *alpha, double *beta, double *c)
{
  dfftw_convolve_2d(*shape, *reva, *ma, *na, a, 
		    *revb, *mb, *nb, b, *alpha, *beta, c);
}
