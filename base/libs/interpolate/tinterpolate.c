// Based on coeffj.c
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <math.h>
#include <mkl.h>
#include "tinterpolate.h"
#include "drms_defs.h"

#define minval(x,y) (((x) < (y)) ? (x) : (y))

int tinterpolate(
  int nsample, // Number of input times
  double *tsample, // Input times
  double tint, // Target time
  int nconst, // Number of polynomial terms exactly reproduced
  float **images, // Pointer array to input images
  unsigned char **masks, // Pointer array to input masks. 0 is good, 1 is missing
  float *image_out, // Interpolated image
  int nx, // Number of points in dimension adjacent in memory
  int ny, // Number of points in dimension not adjacent in memory
  int nlead, // Leading dimension of arrays. nlead>=nx
  int method, // Interpolation method
  char **filenamep // Pointer to name of file to read covariance from.
                   // Set to actual file used if method > 0.
)
{
  const unsigned char maskgood=0; // Value of good entries in masks
  int malign=32;
  int ncor=5000;
  //double dta=1.0; // Time between samples of input autocorrelation
  int i,j,isample;
  FILE *fileptr;
  double ti,dt,dtx;
  double *acor; // Input autocorrelation
  double *a0,*a,*rh0,*coeffd;
  double *a1b,bta1b,*a1r,bta1r,c;
  int idt;
  int info;
  char uplo[] = "U";
  int ione = 1;
  float *coeff;
  int nmasks/*,smask*/,imask,imask1,ngood;
  int *wgood;
  int *smasks;
  float *coeffs,*cp;
  float *sums,*ip;
  double t0,t1,t2;
  unsigned char *mp;
  //int ib, nblock;
  char *filename;

  if ((nsample < 1) || (nsample > 20)) {
// Code breaks at 31 or 32 due to the use of summed masks
// Also, memory usage and efficiency drops due to all masks calculated for >20
    printf("tinterpolate: must have 1<=nsample<=20\n");
    return 1;
  }
  t0=dsecnd();
  switch (method) {
  case 0:
    filename=strdup(*filenamep);
    break;
  case 1:
    filename=strdup(DEFS_MKPATH("/data/acort_hr12.txt"));
    break;
  default:
    printf("Unknown method in init_fill.\n");
    return 1;
    break;
  }
  fileptr = fopen (filename,"r");
  if (fileptr==NULL) {
    printf("File not found in init_fill.\n");
    return 1;
  }
  if (method != 0) {
    if (filenamep != NULL) {
      *filenamep=strdup(filename);
    }
  }

// Read autocorrelation
  acor=(double *)(MKL_malloc(ncor*sizeof(double),malign));
//fread ((char*)(acor),sizeof(double),ncor,fileptr);
  for (i=0;i<ncor;i++) fscanf(fileptr,"%lf",acor+i);
  fclose(fileptr);
  t0=dsecnd()-t0;

  t1=dsecnd();
  a0=(double *)(MKL_malloc(nsample*nsample*sizeof(double),malign));
  a=(double *)(MKL_malloc(nsample*nsample*sizeof(double),malign));
  rh0=(double *)(MKL_malloc(nsample*sizeof(double),malign));
  coeffd=(double *)(MKL_malloc(nsample*sizeof(double),malign));
  coeff=(float *)(MKL_malloc(nsample*sizeof(float),malign));
  a1b=(double *)(MKL_malloc(nsample*sizeof(double),malign));
  a1r=(double *)(MKL_malloc(nsample*sizeof(double),malign));

// nmasks=2^nsample
  nmasks=1;
  for (i=0;i<nsample;i++) nmasks=2*nmasks;
  coeffs=(float *)(MKL_malloc(nsample*nmasks*sizeof(float),malign));
  wgood=(int *)(MKL_malloc(nsample*sizeof(int),malign));

  for (i=0;i<nsample;i++) {
    ti=tsample[i];
    for (j=0;j<nsample;j++) {
      dt=fabs(tsample[j]-ti);
      idt=(float)floor(dt);
      dtx=dt-idt;
      a0[i*nsample+j]=(1.0-dtx)*acor[idt]+dtx*acor[idt+1];
    }
    dt=fabs(tint-ti);
    idt=(float)floor(dt);
    dtx=dt-idt;
    rh0[i]=(1.0-dtx)*acor[idt]+dtx*acor[idt+1];
  }

// For the time being calculate weights for all possible masks
// Could be done implicitly if nsample becomes large
// For 4096x4096 images interpolation dominates to around nsample=20
for (imask=0;imask<nmasks;imask++) {
    cp=coeffs+nsample*imask;
    imask1=imask;
    ngood=0;
// Find good and bad given mask
    for (i=0;i<nsample;i++) {
      if ((imask1&1)==0) { // Good data
        wgood[ngood]=i;
        ngood=ngood+1;
      }
      imask1=imask1/2;
      cp[i]=0.0f; // Zero all coefficients
    } // for i
    if (ngood>0) {
// Pick out relevant parts of a0 and rh0
      for (i=0;i<ngood;i++) {
        for (j=0;j<ngood;j++) a[ngood*i+j]=a0[nsample*wgood[i]+wgood[j]];
        coeffd[i]=rh0[wgood[i]];
      }
      dpotrf(uplo,&ngood,a,&ngood,&info); // Cholesky decomposition
      if (nconst==0) {
        dpotrs(uplo,&ngood,&ione,a,&ngood,coeffd,&ngood,&info);
      }
      else {
        for (i=0;i<ngood;i++) a1b[i]=1.;
        dpotrs(uplo,&ngood,&ione,a,&ngood,a1b,&ngood,&info);
        bta1b=0.;
        for (i=0;i<ngood;i++) bta1b=bta1b+a1b[i];
        for (i=0;i<ngood;i++) a1r[i]=rh0[wgood[i]];
        dpotrs(uplo,&ngood,&ione,a,&ngood,a1r,&ngood,&info);
        bta1r=0.0;
        for (i=0;i<ngood;i++) bta1r=bta1r+a1r[i];
        c=(bta1r-1.)/bta1b;
        for (i=0;i<ngood;i++) coeffd[i]=a1r[i]-c*a1b[i];
      } // nconst
      for (i=0;i<ngood;i++) cp[wgood[i]]=(float)coeffd[i];
    } // if ngood
  } // for imask
  t1=dsecnd()-t1;

  t2=dsecnd();
// Now do actual interpolation
// Could play include games here, but for the time being not
/*
// Inner loop over time
  for (j=0;j<ny;j++) {
    for (i=0;i<nx;i++) {
      ix=i+nlead*j; // Index into array
// First encode mask in single integer
      smask=0;
      for (isample=nsample-1;isample>=0;isample--) {
        if (masks[isample][ix]!=maskgood) smask=2*smask+1; else smask=2*smask;
      }
      sum=0.0f;
      if (smask==0) { // All are there
        for (isample=0;isample<nsample;isample++) sum=sum+coeffs[isample]*images[isample][ix];
      }
//    else if (smask != (nmasks-1)) { // Some, but not all, are missing.
      else { // Some are missing.
// Could keep wgood and index instead
        cp=coeffs+nsample*smask;
        for (isample=0;isample<nsample;isample++) {
// This if statement is costly but needed to avoid accessing NaNs
          if (masks[isample][ix]==maskgood) {
            sum=sum+cp[isample]*images[isample][ix];
          }
        }
      } // if (smask)
      image_out[ix]=sum;
    } // for i
  } // for j
*/

// Alternate version with inner loop over x
  smasks=(int *)(MKL_malloc(nx*sizeof(int),malign));
  sums=(float *)(MKL_malloc(nx*sizeof(float),malign));
  for (j=0;j<ny;j++) {
    for (i=0;i<nx;i++) smasks[i]=0;
    for (isample=nsample-1;isample>=0;isample--) {
      mp=masks[isample]+nlead*j;
      for (i=0;i<nx;i++) {
        if (mp[i]!=maskgood)
          smasks[i]=2*smasks[i]+1;
        else
          smasks[i]=2*smasks[i];
      }
    }
    for (i=0;i<nx;i++) sums[i]=0.0f;
    for (isample=0;isample<nsample;isample++) {
      mp=masks[isample]+nlead*j;
      cp=coeffs+isample;
      ip=images[isample]+nlead*j;
      for (i=0;i<nx;i++) {
        if (mp[i]==maskgood) {
          sums[i]=sums[i]+cp[nsample*smasks[i]]*ip[i];
        }
      } // for i
    } // for isample
    ip=image_out+nlead*j;
    for (i=0;i<nx;i++) ip[i]=sums[i];
//  memcpy(ip,sums,nx*sizeof(float));
  } // for j
  MKL_free(smasks);
  MKL_free(sums);

/*
nblock=1024;
// Alternate version with inner loop over x and blocks
  smasks=(int *)(MKL_malloc(nblock*sizeof(int),malign));
  sums=(float *)(MKL_malloc(nblock*sizeof(float),malign));
  for (j=0;j<ny;j++) {
    for (ib=0;ib<nx;ib=ib+nblock) {
      for (i=0;i<minval(nblock,nx-ib);i++) smasks[i]=0;
      for (isample=nsample-1;isample>=0;isample--) {
        mp=masks[isample]+ib;
        for (i=0;i<minval(nblock,nx-ib);i++) {
          if (mp[i]!=maskgood)
            smasks[i]=2*smasks[i]+1;
          else
            smasks[i]=2*smasks[i];
        }
      }
      for (i=0;i<minval(nblock,nx-ib);i++) sums[i]=0.0f;
      for (isample=0;isample<nsample;isample++) {
        mp=masks[isample]+ib;
        cp=coeffs+isample;
        ip=images[isample]+nlead*j+ib;
        for (i=0;i<minval(nblock,nx-ib);i++) {
          if (mp[i]==maskgood) {
            sums[i]=sums[i]+cp[nsample*smasks[i]]*ip[i];
          }
        } // for i
      } // for isample
      ip=image_out+nlead*j+ib;
      for (i=0;i<minval(nblock,nx-ib);i++) ip[i]=sums[i];
    } // for ib
  } // for j
  MKL_free(smasks);
  MKL_free(sums);
*/

  t2=dsecnd()-t2;
  printf("%d %f %f %f\n",nsample,t0,t1,t2);

  MKL_free(acor);
  MKL_free(a0);
  MKL_free(a);
  MKL_free(rh0);
  MKL_free(coeffd);
  MKL_free(coeff);
  MKL_free(a1b);
  MKL_free(a1r);
  MKL_free(coeffs);
  MKL_free(wgood);
  free (filename);
  return 0;
}


