#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <math.h>
#include <mkl.h>
#include <omp.h>
#include "finterpolate.h"
#include "drms_defs.h"

#define minval(x,y) (((x) < (y)) ? (x) : (y))
#define maxval(x,y) (((x) < (y)) ? (y) : (x))
#define fint_wiener 1
#define fint_linear 2
#define fint_cubic_conv 3
#define corfile1 DEFS_MKPATH("/data/acor1d_80x100_double.txt")

double sinc(double x)
{
if (fabs(x) < (1.e-10))
  return 1.;
else
  return sin(M_PI*x)/(M_PI*x);
}

int init_finterpolate_wiener_old(
  struct fint_struct *pars,
  int order,
  int edgemode, // 0 to go as far as you can with symmetric kernel
                // Otherwise go further (as set by extrapolate)
  float extrapolate, // How far to extrapolate
  int minorder, // Minimum order to use when approaching edge or beyond
  int nconst // Number of polynomial constraints
             // 0 None, 1 for norm, 2 for linear preserved, etc.
)
{
  int status;
  int table=0;
  char *filename = corfile1;

  status=init_finterpolate_wiener(pars,order,edgemode,extrapolate,minorder,nconst,table,&filename);

  return status;
}

int init_finterpolate_wiener(
  struct fint_struct *pars,
  int order,
  int edgemode, // 0 to go as far as you can with symmetric kernel
                // Otherwise go further (as set by extrapolate)
  float extrapolate, // How far to extrapolate
  int minorder, // Minimum order to use when approaching edge or beyond
  int nconst, // Number of polynomial constraints
             // 0 None, 1 for norm, 2 for linear preserved, etc.
  int cortable, // Which of the hardcoded tables to use. 
                // 0 To use table pointed to by filenamep
  char **filenamep // Pointer to name of file to read covariance from.
                 // Set to actual file used if cortable>0
)
{
  const int malign=32;
  const int nsubin0=100;
  const int maxoff=40;
  int i,j,k;
  int ishift,offset,order2,nextra,nshifts,shift0;
  FILE *fileptr;
  double *acor,*a,*rh,*a1b,*a1r,*coeffd;
  double bta1b,bta1r,c;
  double *b,*rhc,*bta1b1,*help;
  double xc,xp,sum;
  int info;
  char uplo[] = "U";
  int ione = 1;
  double pmin,regul;
  int minorder1,curorder,imin,imax,icent,is0,i0,nrh;

  pars->method=fint_wiener;
  pars->kersx=NULL;

  if ((order%2) != 0) {
    printf("Order must be even!\n");
    return 2;
  }
  if ((minorder%2) != 0) {
    printf("Minorder must be even!\n");
    return 2;
  }
  if (nconst < 0) {
    printf("Number of constraints must be non-negative!\n");
    return 2;
  }
  order2=order/2;
  if (edgemode==0) { // Only do area with symmetric set of points available
    nshifts=nsubin0+1;
    shift0=0;
    if (extrapolate!=0) {
      printf("Warning: Can't extrapolate for edgemode==0.\n");
      extrapolate=0.0f;
    }
  }
  nextra=maxval(0,extrapolate)*nsubin0+2;
  shift0=(order2-1)*nsubin0+nextra;
  nshifts=(order-1)*nsubin0+2*nextra+1;
  icent=(nshifts-1)/2; // Center of range. icent=shift0+nsubin0/2
  pars->edgemode=edgemode;
  pars->extrapolate=extrapolate;
  pars->order=order;
  pars->nshifts=nshifts;
  pars->shift0=shift0;
  pars->fover=(float)nsubin0;
  pars->nsub=nsubin0+1;
  pars->kersx=(float *)(MKL_malloc(nshifts*order*sizeof(float),malign));

  regul=1/400./400.;
  pmin=regul;

  if ((cortable < 0) || (cortable > 1)) {
    printf("Illegal cortable passed to finterpolate!\n");
    return 1;
  }
  if (cortable == 0) {
    pars->filename=strdup(*filenamep);
  }
  if (cortable == 1) {
    pars->filename=strdup(corfile1);
  }
  fileptr = fopen (pars->filename, "r");
  if (fileptr == NULL) {
    printf("File not found in finterpolate!\n");
    return 2;
  }
  if (cortable != 0) {
    if (filenamep != NULL) {
      *filenamep=strdup(pars->filename);
    }
  }
  acor=(double *)(MKL_malloc((nsubin0*maxoff+1)*sizeof(double),malign));
  for (i=0;i<nsubin0*maxoff+1;i++) fscanf(fileptr,"%lf",acor+i);
  fclose(fileptr);

  nrh=maxval(1,nconst);
  a=(double *)(MKL_malloc(order*order*sizeof(double),malign));
  rh=(double *)(MKL_malloc(nrh*order*sizeof(double),malign));
  a1r=(double *)(MKL_malloc(nrh*order*sizeof(double),malign));
  a1b=(double *)(MKL_malloc(nrh*order*sizeof(double),malign));
  coeffd=(double *)(MKL_malloc(order*sizeof(double),malign));
  b=(double *)(MKL_malloc(nrh*order*sizeof(double),malign));
  rhc=(double *)(MKL_malloc(nrh*sizeof(double),malign));
  help=(double *)(MKL_malloc(nrh*sizeof(double),malign));
  bta1b1=(double *)(MKL_malloc(nrh*nrh*sizeof(double),malign));
  minorder1=minorder;
  if (edgemode == 0) minorder1=order; // Effectively ignore minorder
  for (curorder=minorder1;curorder<=order;curorder=curorder+2) {
    for (i=0;i<curorder;i++) {
      for (j=i;j<curorder;j++) {
        offset=nsubin0*(j-i);
        a[i*curorder+j]=acor[offset];
        a[j*curorder+i]=acor[offset];
      }
      a[i*curorder+i]=a[i*curorder+i]+regul;
    }
    dpotrf(uplo,&curorder,a,&curorder,&info); // Cholesky decomposition

    imin=icent-(order-curorder+1)*nsubin0/2;
    imax=icent+(order-curorder+1)*nsubin0/2;
    if (curorder == minorder) { // Extrapolate using minorder
      imin=0;
      imax=nshifts-1;
    }
    for (ishift=imin;ishift<=imax;ishift++) {
// Find shift equivalent of first pixel to be used for interpolation
// First find remainder
      is0=(ishift-shift0+nsubin0*2*order) % nsubin0;
// Then find pixel
      is0=ishift-is0-nsubin0*(curorder/2-1);
// Then limit to range of valid pixels
      is0=maxval(is0,icent-(order-1)*nsubin0/2);
      is0=minval(is0,icent+(order-1)*nsubin0/2-(curorder-1)*nsubin0);
      i0=(is0-icent+(order-1)*nsubin0/2)/nsubin0;
//printf("%d %d %d %d %d %d\n",order,imin,imax,ishift,is0,i0);
      for (i=0;i<curorder;i++) {
        offset=ishift-(shift0+nsubin0*(i+i0-order/2+1));
        if (offset<0) offset=-offset;
        rh[i]=acor[offset]+pmin*sinc((offset+0.0)/nsubin0);
      } // i

      switch (nconst) {
      case 0: // No constraints
        for (i=0;i<curorder;i++) coeffd[i]=rh[i];
        dpotrs(uplo,&curorder,&ione,a,&curorder,coeffd,&curorder,&info);
        break;
      case 1: // Unit norm. Some tricks applied.
        for (i=0;i<curorder;i++) a1b[i]=1.;
        dpotrs(uplo,&curorder,&ione,a,&curorder,a1b,&curorder,&info);
        bta1b=0.;
        for (i=0;i<curorder;i++) bta1b=bta1b+a1b[i];
        for (i=0;i<curorder;i++) a1r[i]=rh[i];
        dpotrs(uplo,&curorder,&ione,a,&curorder,a1r,&curorder,&info);
        bta1r=0.0;
        for (i=0;i<curorder;i++) bta1r=bta1r+a1r[i];
        c=(bta1r-1.)/bta1b;
        for (i=0;i<curorder;i++) coeffd[i]=a1r[i]-c*a1b[i];
        break;
      default: // General case
        for (i=0;i<curorder;i++) {
          offset=ishift-(shift0+nsubin0*(i+i0-order/2+1));
          xc=(offset+0.0)/nsubin0;
          xp=1.0;
          for (j=0;j<nconst;j++) {
            b[j*curorder+i]=xp;
            xp=xp*xc;
          }
        }
        rhc[0]=1;
        for (j=1;j<nconst;j++) rhc[j]=0;
        for (i=0;i<curorder*nconst;i++) a1b[i]=b[i];
        dpotrs(uplo,&curorder,&nconst,a,&curorder,a1b,&curorder,&info);
        for (i=0;i<nconst;i++) {
          for (j=0;j<nconst;j++) {
            sum=0.0;
            for (k=0;k<curorder;k++) 
              sum=sum+a1b[i*curorder+k]*b[j*curorder+k];
            bta1b1[i*nconst+j]=sum;
          }
        }
        dpotrf(uplo,&nconst,bta1b1,&nconst,&info); // Cholesky decomposition
        for (i=0;i<nconst;i++) {
          sum=0.0;
          for (k=0;k<curorder;k++) sum=sum+a1b[i*curorder+k]*rh[k];
          help[i]=sum-rhc[i];
        }
        dpotrs(uplo,&nconst,&ione,bta1b1,&nconst,help,&nconst,&info);
        for (i=0;i<curorder;i++) a1r[i]=rh[i];
        dpotrs(uplo,&curorder,&ione,a,&curorder,a1r,&curorder,&info);
        for (i=0;i<curorder;i++) {
          sum=0.0;
          for (k=0;k<nconst;k++) sum=sum+a1b[k*curorder+i]*help[k];
          coeffd[i]=a1r[i]-sum;
        }
        break;
      }

// Copy to output array
// First zero unused entries
      for (i=0;i<order;i++) pars->kersx[ishift*order+i]=0.0f;
      for (i=0;i<curorder;i++) pars->kersx[ishift*order+i+i0]=(float)coeffd[i];
//    for (i=0;i<curorder;i++) printf(" %f",coeffd[i]);printf("\n");
    } // ishift
  } // order
//for (ishift=0;ishift<nshifts;ishift++) {
//  for (i=0;i<order;i++) printf(" %f",pars->kersx[ishift*order+i]);
//  printf("\n");
//}

  MKL_free(acor);
  MKL_free(a);
  MKL_free(a1r);
  MKL_free(a1b);
  MKL_free(rh);
  MKL_free(coeffd);
  MKL_free(b);
  MKL_free(rhc);
  MKL_free(help);
  MKL_free(bta1b1);

  return 0;
}

int init_finterpolate_linear(
  struct fint_struct *pars,
  float extrapolate // How far to extrapolate
)
{

  pars->method=fint_linear;
  pars->extrapolate=extrapolate;

  return 0;
}

int init_finterpolate_cubic_conv(
  struct fint_struct *pars,
  int edgemode, // 0 to go as far as you can with symmetric kernel
                // Otherwise go further (as set by extrapolate)
  float extrapolate // How far to extrapolate
)
{

  pars->method=fint_cubic_conv;
  pars->edgemode=edgemode;
  pars->extrapolate=extrapolate;

  return 0;
}

int free_finterpolate(
  struct fint_struct *pars
)
{
  if (pars->method==fint_wiener) {
// Check for null if called after error in init.
    if (pars->kersx!=NULL) {
      MKL_free (pars->kersx);
      free (pars->filename);
    }
  }

  return 0;
}

int winterpolate(
struct fint_struct *pars,
float *image_in,
float *xin,
float *yin,
float *image_out,
int nxin,
int nyin,
int nleadin,
int nx,
int ny,
int nlead
)

{
  int i,j,i1,j1;
  int order2;
  float *xker;
  const int ione = 1;
  int malign=32;
  int ixin,iyin,ixin1,iyin1;
  float *ixins,*iyins,*ixin1s,*iyin1s;
  float fxin1,fyin1,fxin2,fyin2;
  float *fxins,*fyins,*xinp,*yinp,*fxin1s,*fyin1s;
  float *xk1,*xk2,*yk1,*yk2;
  float *imp;
  float sum,sum1;
  float *kersx;
  int ixmax,iymax;
  float xmax,ymax;
  int order,shift0,edgemode;
  float extrapolate;
  float x,y,help;

  order=pars->order;
  kersx=pars->kersx;
  shift0=pars->shift0;
  edgemode=pars->edgemode;
  extrapolate=pars->extrapolate;

  order2=order/2;
  ixmax=nxin-order2; // Max index for first element of kernel
  iymax=nyin-order2; // Max index for first element of kernel
  xmax=(float)ixmax;
  ymax=(float)iymax;

#pragma omp parallel default(none)\
private(ixins,iyins,fxins,fyins,ixin1s,iyin1s,fxin1s,fyin1s,xker) \
private (i,j,xinp,yinp,ixin,iyin,ixin1,iyin1,fxin1,fyin1,fxin2,fyin2,imp) \
private (xk1,xk2,yk1,yk2,sum,sum1,i1,j1,x,y,help) \
shared (pars,nlead,nx,ny,xin,yin,nleadin,nxin,nyin,image_in,image_out,order) \
shared (malign,order2,kersx,ixmax,iymax,xmax,ymax,shift0,edgemode,extrapolate)
{ // Needed to define parallel region
    ixins=(float *)(MKL_malloc(nx*sizeof(int),malign));
    iyins=(float *)(MKL_malloc(nx*sizeof(int),malign));
    fxins=(float *)(MKL_malloc(nx*sizeof(float),malign));
    fyins=(float *)(MKL_malloc(nx*sizeof(float),malign));
    ixin1s=(float *)(MKL_malloc(nx*sizeof(float),malign));
    iyin1s=(float *)(MKL_malloc(nx*sizeof(float),malign));
    fxin1s=(float *)(MKL_malloc(nx*sizeof(float),malign));
    fyin1s=(float *)(MKL_malloc(nx*sizeof(float),malign));

    xker=(float *)(MKL_malloc(order*sizeof(float),malign));

// This awful code basically hardcodes the common cases and allows the
// compiler to optimize the code for a significant speed improvement
    switch(order) {
    case 2:
#define OOO1 2
#define OOO2 1
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 4:
#define OOO1 4
#define OOO2 2
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 6:
#define OOO1 6
#define OOO2 3
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 8:
#define OOO1 8
#define OOO2 4
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 10:
#define OOO1 10
#define OOO2 5
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 12:
#define OOO1 12
#define OOO2 6
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 14:
#define OOO1 14
#define OOO2 7
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 16:
#define OOO1 16
#define OOO2 8
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 18:
#define OOO1 18
#define OOO2 9
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 20:
#define OOO1 20
#define OOO2 10
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 22:
#define OOO1 22
#define OOO2 11
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 24:
#define OOO1 24
#define OOO2 12
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 26:
#define OOO1 26
#define OOO2 13
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 28:
#define OOO1 28
#define OOO2 14
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 30:
#define OOO1 30
#define OOO2 15
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    case 32:
#define OOO1 32
#define OOO2 16
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
// If not hardcoded do general calculation
    default:
#define OOO1 order
#define OOO2 order2
#include "finterpolate.include"
#undef OOO1
#undef OOO2
      break;
    }

    MKL_free(xker);
    MKL_free(ixins);
    MKL_free(iyins);
    MKL_free(fxins);
    MKL_free(fyins);
    MKL_free(ixin1s);
    MKL_free(iyin1s);
    MKL_free(fxin1s);
    MKL_free(fyin1s);
  } // End of parallel region

  return 0;
}

int linterpolate(
float *image_in,
float *xin,
float *yin,
float *image_out,
int nxin,
int nyin,
int nleadin,
int nx,
int ny,
int nlead,
float extrapolate,
float fillval
)

{
  int i,j;
  int malign=32;
  int ixin,iyin;
  float *ixins,*iyins;
  float fxin1,fyin1,fxin2,fyin2;
  float *xinp,*yinp;
  float *imp;
  float xmin,xmax,ymin,ymax;

  xmin=-extrapolate;
  xmax=nxin-1.0f+extrapolate;
  ymin=-extrapolate;
  ymax=nyin-1.0f+extrapolate;

#pragma omp parallel default(none) \
private(ixins,iyins,i,j,xinp,yinp,ixin,iyin,fxin1,fyin1,fxin2,fyin2,imp) \
shared(image_in,xin,yin,image_out,extrapolate,fillval) \
shared(nx,ny,nlead,nxin,nyin,nleadin) \
shared(malign,xmin,xmax,ymin,ymax)
  { // Needed to define parallel region
    ixins=(float *)(MKL_malloc(nx*sizeof(int),malign));
    iyins=(float *)(MKL_malloc(nx*sizeof(int),malign));

#pragma omp for
    for (j=0; j<ny; j++) {
      xinp=xin+j*nlead;
      vsFloor(nx,xinp,ixins);
      yinp=yin+j*nlead;
      vsFloor(nx,yinp,iyins);
      for (i=0; i<nx; i++) {
        ixin=(int)ixins[i]; // Integer pixel to interpolate to
        iyin=(int)iyins[i]; // Integer pixel to interpolate to
        if ((ixin>=0) && (ixin <(nxin-1)) && (iyin>=0) && (iyin <(nyin-1))) {
// Normal case

          fxin1=xinp[i]-ixin;
          fyin1=yinp[i]-iyin;
          fxin2=1.0f-fxin1;
          fyin2=1.0f-fyin1;
    
          imp=image_in+ixin+nleadin*iyin;

/* Brute force addition */
          image_out[i+nlead*j]=fyin2*(fxin2*imp[0]+fxin1*imp[1])+
                               fyin1*(fxin2*imp[nleadin]+fxin1*imp[nleadin+1]);
        } // if
        else {
          if ((xinp[i]>=xmin) && (xinp[i]<=xmax) && (yinp[i]>=ymin) && (yinp[i]<=ymax)) {
// Extrapolating or point exactly at edge
// Move ixin and iyin inside of array
            ixin=maxval(0,minval(ixin,nxin-2));
            iyin=maxval(0,minval(iyin,nyin-2));

            fxin1=xinp[i]-ixin;
            fyin1=yinp[i]-iyin;
            fxin2=1.0f-fxin1;
            fyin2=1.0f-fyin1;
      
            imp=image_in+ixin+nleadin*iyin;
    
    /* Brute force addition */
            image_out[i+nlead*j]=fyin2*(fxin2*imp[0]+fxin1*imp[1])+
                                 fyin1*(fxin2*imp[nleadin]+fxin1*imp[nleadin+1]);
          }
          else {
// Outside of array
            image_out[i+nlead*j]=fillval;
          }
        }
      } // i=
    } //j=

    MKL_free(ixins);
    MKL_free(iyins);
  } // End of parallel region

  return 0;
}

int ccinterpolate( // Cubic convolution interpolation
float *image_in,
float *xin,
float *yin,
float *image_out,
int nxin,
int nyin,
int nleadin,
int nx,
int ny,
int nlead,
int edgemode,
float extrapolate,
float fillval
)

{
  int i,j,i1;
  float xker[4],yker[4];
  int malign=32;
  int ixin,iyin;
  float *ixins,*iyins;
  float *xinp,*yinp;
  float *imp;
  float sum;
  float s,s2;
  float xmin,xmax,ymin,ymax;

  if (edgemode == 0) {
    xmin=1.0f;
    xmax=nxin-2.0f;
    ymin=1.0f;
    ymax=nyin-2.0f;
  }
  else {
    xmin=-extrapolate;
    xmax=nxin-1.0f+extrapolate;
    ymin=-extrapolate;
    ymax=nyin-1.0f+extrapolate;
  }

#pragma omp parallel default(none) \
private(ixins,iyins,i,j,xinp,yinp,ixin,iyin,imp,s,s2,xker,yker,sum,i1) \
shared(image_in,xin,yin,image_out) \
shared(nxin,nyin,nleadin,nx,ny,nlead,extrapolate,fillval) \
shared(malign,xmin,xmax,ymin,ymax)
  { // Needed to define parallel region
    ixins=(float *)(MKL_malloc(nx*sizeof(int),malign));
    iyins=(float *)(MKL_malloc(nx*sizeof(int),malign));

#pragma omp for 
    for (j=0; j<ny; j++) {
      xinp=xin+j*nlead;
      vsFloor(nx,xinp,ixins);
      yinp=yin+j*nlead;
      vsFloor(nx,yinp,iyins);
      for (i=0; i<nx; i++) {
        ixin=(int)ixins[i]; // Integer pixel to interpolate to
        iyin=(int)iyins[i]; // Integer pixel to interpolate to
        if ((ixin>=1) && (ixin <(nxin-2)) && (iyin>=1) && (iyin <(nyin-2))) {

// RML kernel calculation based on fc3kernel in winterpolate_old.c
// Kernels are unnormalized and need to be divided by 2 

          s=xinp[i]-ixin;
          s2 = s*s;
          xker[0] = -s*(1.0f - s*(2.0f - s));
          xker[1] = 2.0f - s2*(5.0f - 3.0f*s);
          xker[2] = s*(1.0f + s*(4.0f - 3.0f*s));
          xker[3] = -s2*(1.0f-s);

          s=yinp[i]-iyin;
          s2 = s*s;
          yker[0] = -s*(1.0f - s*(2.0f - s));
          yker[1] = 2.0f - s2*(5.0f - 3.0f*s);
          yker[2] = s*(1.0f + s*(4.0f - 3.0f*s));
          yker[3] = -s2*(1.0f-s);
    
          imp=image_in+ixin-1+nleadin*(iyin-1);

/* Brute force addition */
          sum=0.0f;
          for (i1=0; i1<4; i1++) {
            sum=sum+yker[i1]*(xker[0]*imp[0]+xker[1]*imp[1]+xker[2]*imp[2]+xker[3]*imp[3]);
            imp=imp+nleadin;
          }
          image_out[i+nlead*j]=sum/4;
        } // if
        else {
          if ((xinp[i]>=xmin) && (xinp[i]<=xmax) && (yinp[i]>=ymin) && (yinp[i]<=ymax)) {
// Extrapolating or point exactly at edge
// Move ixin and iyin inside of array
            ixin=maxval(1,minval(ixin,nxin-3));
            iyin=maxval(1,minval(iyin,nyin-3));
// RML kernel calculation based on fc3kernel in winterpolate_old.c
// Kernels are unnormalized and need to be divided by 2 

            s=xinp[i]-ixin;
            s2 = s*s;
            xker[0] = -s*(1.0f - s*(2.0f - s));
            xker[1] = 2.0f - s2*(5.0f - 3.0f*s);
            xker[2] = s*(1.0f + s*(4.0f - 3.0f*s));
            xker[3] = -s2*(1.0f-s);
    
            s=yinp[i]-iyin;
            s2 = s*s;
            yker[0] = -s*(1.0f - s*(2.0f - s));
            yker[1] = 2.0f - s2*(5.0f - 3.0f*s);
            yker[2] = s*(1.0f + s*(4.0f - 3.0f*s));
            yker[3] = -s2*(1.0f-s);
      
            imp=image_in+ixin-1+nleadin*(iyin-1);
    
    /* Brute force addition */
            sum=0.0f;
            for (i1=0; i1<4; i1++) {
              sum=sum+yker[i1]*(xker[0]*imp[0]+xker[1]*imp[1]+xker[2]*imp[2]+xker[3]*imp[3]);
              imp=imp+nleadin;
            }
            image_out[i+nlead*j]=sum/4;
          }
          else {
// Outside of array
            image_out[i+nlead*j]=fillval;
          }
        }
// Ought to do something here.
      } // i=
    } //j=

    MKL_free(ixins);
    MKL_free(iyins);
  } // End of parallel region

  return 0;
}

int finterpolate(
struct fint_struct *pars,
float *image_in,
float *xin,
float *yin,
float *image_out,
int nxin,
int nyin,
int nleadin,
int nx,
int ny,
int nlead,
float fillval
)
{
  int status;

  switch (pars->method) {
  case fint_wiener:
    status=winterpolate(pars,image_in,xin,yin,image_out,nxin,nyin,nleadin,nx,ny,nlead);
    break;

  case fint_linear:
    status=linterpolate(image_in,xin,yin,image_out,nxin,nyin,nleadin,nx,ny,nlead,pars->extrapolate,fillval);
    break;

  case fint_cubic_conv:
    status=ccinterpolate(image_in,xin,yin,image_out,nxin,nyin,nleadin,nx,ny,nlead,pars->edgemode,pars->extrapolate,fillval);
    break;

  default:
    status=1;
    break;
  }

return status;

}

