#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <math.h>
#include <mkl.h>
#include <omp.h>
#include "cholesky_down.h"
#include "gapfill.h"
#include "drms_defs.h"

#define minval(x,y) (((x) < (y)) ? (x) : (y))
#define maxval(x,y) (((x) < (y)) ? (y) : (x))

struct work_struct {
 double *a,*rh,*a1b,*a1r,*a1x;
 int *wgood,*wbad;
};

void malloc_work(
// Malloc work space for each thread
  int nsample,
  struct work_struct *work
)
{
  int malign=32;

  work->wgood=(int *)(MKL_malloc(nsample*sizeof(int),malign));
  work->wbad=(int *)(MKL_malloc(nsample*sizeof(int),malign));
  work->a=(double *)(MKL_malloc(nsample*nsample*sizeof(double),malign));
  work->rh=(double *)(MKL_malloc(nsample*sizeof(double),malign));
  work->a1b=(double *)(MKL_malloc(nsample*sizeof(double),malign));
  work->a1r=(double *)(MKL_malloc(nsample*sizeof(double),malign));
  work->a1x=(double *)(MKL_malloc(2*nsample*sizeof(double),malign));
}

void free_work(
// Free the workspace
  int nsample,
  struct work_struct *work
)
{
  MKL_free(work->wgood);
  MKL_free(work->wbad);
  MKL_free(work->a);
  MKL_free(work->rh);
  MKL_free(work->a1b);
  MKL_free(work->a1r);
  MKL_free(work->a1x);
}

int init_fill(
  int method, // Interpolation method
  double pnoise, // Level of photon noise for trade-off
  int order, // Interpolation order. Generally odd.
  int targetx, // Target point in x (normally (order-1)/2)
  int targety, // Target point in y (normally (order-1)/2)
  struct fill_struct *pars, // Structure to save setup information etc.
  char **filenamep // Pointer to name of file to read covariance from.
                   // Set to actual file used if method > 0.
)
{
  double *acor;
  int nx=80;
  int malign=32;
  double t0,t1;
  FILE *fileptr;
  int i,j;
  int *offx,*offy,ox,oy;
  int nsample,inoise;
  double regul,pmin;
  double *a0,*rh0,*a0t,*rh0t;
  int info;
  char uplo[] = "U";

  pars->order=order;

  t0=dsecnd();
  switch (method) {
  case 0:
    pars->filename=strdup(*filenamep);
    break;
  case 8:
    pars->filename=strdup(DEFS_MKPATH("/data/acor1_08_80.txt"));
    break;
  case 9:
    pars->filename=strdup(DEFS_MKPATH("/data/acor1_09_80.txt"));
    break;
  case 10:
    pars->filename=strdup(DEFS_MKPATH("/data/acor1_10_80.txt"));
    break;
  case 11:
    pars->filename=strdup(DEFS_MKPATH("/data/acor1_11_80.txt"));
    break;
  default:
    printf("Unknown method in init_fill.\n");
    return 1;
    break;
  }
  fileptr = fopen (pars->filename,"r");
  if (fileptr==NULL) {
    printf("File not found in init_fill.\n");
    return 1;
  }
  if (method != 0) {
    if (filenamep != NULL) {
      *filenamep=strdup(pars->filename);
    }
  }
  acor=(double *)(MKL_malloc(nx*nx*sizeof(double),malign));
//fread ((char*)acor,sizeof(double),nx*nx,fileptr);
  for (i=0;i<nx*nx;i++) fscanf(fileptr,"%lf",acor+i);
  fclose(fileptr);
  t0=dsecnd()-t0;
  printf("x0 %f\n",t0);

  regul=pnoise*pnoise;
  inoise=1;
  pmin=regul*inoise;

  nsample=order*order;
  t1=dsecnd();
  offx=(int *)(MKL_malloc(nsample*sizeof(int),malign));
  offy=(int *)(MKL_malloc(nsample*sizeof(int),malign));
  for (i=0;i<nsample;i++) {
    offx[i]=(i%order)-targetx;
    offy[i]=(i/order)-targety;
  }
// Set up complete matrix
  pars->a0=(double *)(MKL_malloc(nsample*nsample*sizeof(double),malign));
  pars->a00=(double *)(MKL_malloc(nsample*nsample*sizeof(double),malign));
  pars->a0t=(double *)(MKL_malloc(nsample*nsample*sizeof(double),malign));
  a0=pars->a0;
  a0t=pars->a0t;
  for (i=0;i<nsample;i++) {
    for (j=0;j<nsample;j++) {
      ox=((offx[j]-offx[i])+nx) % nx;
      oy=((offy[j]-offy[i])+nx) % nx;
      a0[i+nsample*j]=acor[ox+nx*oy];
      a0t[i+nsample*j]=acor[ox+nx*oy];
    }
    a0[i+nsample*i]=a0[i+nsample*i]+regul;
  }
// save copy of matrix
  memcpy(pars->a00,a0,nsample*nsample*sizeof(double));
  pars->acort00=acor[0];
  pars->rh0=(double *)(MKL_malloc(nsample*sizeof(double),malign));
  pars->rh0t=(double *)(MKL_malloc(nsample*sizeof(double),malign));
  rh0=pars->rh0;
  rh0t=pars->rh0t;
  for (i=0;i<nsample;i++) {
    rh0[i]=acor[(-offx[i]+nx) % nx+nx*((-offy[i]+nx) % nx)];
    rh0t[i]=acor[(-offx[i]+nx) % nx+nx*((-offy[i]+nx) % nx)];
    if ((offx[i]==0) && (offy[i]==0)) rh0[i]=rh0[i]+pmin;
  }
// Decompose complete matrix
  dpotrf(uplo,&nsample,pars->a0,&nsample,&info); // Cholesky decomposition

  MKL_free(acor);
  MKL_free(offx);
  MKL_free(offy);

// Variables used in further calculations. Avoids multiple mallocs.
//pars->wgood=(int *)(MKL_malloc(nsample*sizeof(int),malign));
//pars->wbad=(int *)(MKL_malloc(nsample*sizeof(int),malign));

  pars->hashmod=8191; // Hash table size
  pars->hashcount=(int *)(MKL_malloc(pars->hashmod*sizeof(int),malign));
  pars->hashtable=(struct fill_hash_struct **)(MKL_malloc(pars->hashmod*sizeof(struct fill_hash_struct *),malign));
  pars->locks=(omp_lock_t *)(MKL_malloc(pars->hashmod*sizeof(omp_lock_t),malign));
  pars->complocks=(omp_lock_t *)(MKL_malloc(pars->hashmod*sizeof(omp_lock_t),malign));

  for (i=0;i<pars->hashmod;i++) {
    pars->hashcount[i]=0;
// Initialize to null pointers
    pars->hashtable[i]=(struct fill_hash_struct *)NULL;
    omp_init_lock(&(pars->locks[i]));
    omp_init_lock(&(pars->complocks[i]));
  }
  pars->ndiff=0;

  t1=dsecnd()-t1;
  printf("x1 %f\n",t1);

  return 0;
}

int free_fill(
struct fill_struct *pars
)
{
  int i;
  struct fill_hash_struct *ptr,*oldptr;
  
  printf("Hashmod: %d\n",pars->hashmod);
  printf("Ndiff: %d\n",pars->ndiff);
/*
  for (i=0;i<pars->hashmod;i++) {
    printf("%d %d\n",i,pars->hashcount[i]);
  }
  for (i=0;i<pars->hashmod;i++) {
    ptr=(pars->hashtable[i]);
    while (ptr != NULL) {
      printf("%d %d %d\n",i,ptr->nbad,ptr->nhit);
      ptr=ptr->next;
    }
  }
*/
  MKL_free(pars->a0);
  MKL_free(pars->a00);
  MKL_free(pars->a0t);
  MKL_free(pars->rh0);
  MKL_free(pars->rh0t);

  MKL_free(pars->locks);
  MKL_free(pars->complocks);

// Now free hash table and contents

  MKL_free(pars->hashcount);
  for (i=0;i<pars->hashmod;i++) {
    ptr=(pars->hashtable[i]);
    while (ptr != NULL) {
      oldptr=ptr;
      MKL_free(ptr->wbad);
      MKL_free(ptr->coeff);
      ptr=ptr->next;
      MKL_free(oldptr);
    }
  }
  MKL_free(pars->hashtable);
  free(pars->filename);

  return 0; // Currently no error conditions

}

int find_entry(
  struct fill_struct *pars,
  int hash0,
  int hash,
  int nbad,
  int *wbad,
  struct fill_hash_struct **ptr_out
)
{
  int found;
  int ident,i;
  struct fill_hash_struct *ptr;
  
  found=0;
  *ptr_out=NULL;

// Prevent update while starting traverse
  omp_set_lock(&(pars->locks[hash]));
  ptr=pars->hashtable[hash];
  omp_unset_lock(&(pars->locks[hash]));

  while (ptr!=NULL) { // Not at end of table
    if ((hash0==ptr->hash0) && (nbad==ptr->nbad)) {
// Original hash and number of elements is correct, so there is hope
      ident=1;
      for (i=0;i<nbad;i++) {
        if (wbad[i] != ptr->wbad[i]) {
          ident=0;
          break; // Out of for loop. Known not to match.
        }
      }
      if (ident != 0) { // Success
        ptr->nhit++;
        found=1;
        *ptr_out=ptr;
        break; // Out of while loop. No need to test the rest.
      } // if
    } // if
    ptr=ptr->next;
  } // while

  return found;
} // find_entry

double fill_point(
struct fill_struct *pars,
int *mask,
double *vals,
float *cnorm,
float *ierror,
struct work_struct *work
)
{
  double t1,t2,t3,t4,t5,t6,t7,t8/*,t9*/;
  int nsample;
  int ngood,nbad;
  int *wgood,*wbad;
  int i,info,j;
  char uplo[] = "U";
  int ione = 1;
  int itwo = 2;
  int malign=32;
  double *a,*a0,*rh0,/* *rh,*a1b,*/bta1b,/**a1r,*/bta1r,c,*coeff,*a0t,*a00,*a1x;
  float fillval;
  int hash0,hash,found;
  struct fill_hash_struct *ptr;
  double ierror2; // interpolation error squared
  int unlocked;
  int nleft;
  double tdown,tbrute;
  double sum;

  t1=0.0;t2=0.0;t3=0.0;t4=0.0;t5=0.0;t6=0.0;t7=0.0;t8=0.0;//t9=0.0;

  t1=dsecnd();

  nsample=(pars->order)*(pars->order);
  
// Make work copies
  a0=pars->a0;
  a00=pars->a00;
  a0t=pars->a0t;
  rh0=pars->rh0;

  wgood=work->wgood;
  wbad=work->wbad;

  ngood=0;
  nbad=0;
  hash0=0;
  for (i=0;i<nsample;i++) {
    if (mask[i]==0) {
      wgood[ngood]=i;
      ngood=ngood+1;
    }
    if (mask[i]!=0) {
      hash0=hash0+i;
      wbad[nbad]=i;
      nbad=nbad+1;
    }
  }
  hash=hash0%pars->hashmod;

  unlocked=0;

  found=find_entry(pars,hash0,hash,nbad,wbad,&ptr);

  if (found == 0) {
    unlocked=omp_test_lock(&(pars->complocks[hash])); // Capture lock status
    if (unlocked==0) omp_set_lock(&(pars->complocks[hash]));

// Rerun search. Someone may have updated while waiting for lock
    found=find_entry(pars,hash0,hash,nbad,wbad,&ptr);
    if (found!=0) {
// Clear lock
      omp_unset_lock(&(pars->complocks[hash]));
      found=2; // Indicate collision
    }
  }

  t1=dsecnd()-t1;

  if (found == 0) { // Got to calculate new coefficients

//printf("New %d %d\n",omp_get_thread_num(),hash);
// malloc new entry
//for (i=0;i<nsample;i++) printf(" %d",mask[i]);
//printf("\n");
    ptr=(struct fill_hash_struct *)(MKL_malloc(sizeof(struct fill_hash_struct),malign));
    ptr->next=pars->hashtable[hash]; // Point to next element
// Set other variables
    ptr->nbad=nbad;
    ptr->hash0=hash0;
    ptr->nhit=1;
    ptr->wbad=(int *)(MKL_malloc(nbad*sizeof(int),malign));
    for (i=0;i<nbad;i++) ptr->wbad[i]=wbad[i];
    coeff=(double *)(MKL_malloc(ngood*sizeof(double),malign));
    pars->hashcount[hash]++; // Increment count for this hash
#pragma omp atomic
    pars->ndiff++; // Increment count for total
    ptr->coeff=coeff; // Point to new coefficients

// Point to work arrays
    a=work->a;
    //rh=work->rh;
    //a1b=work->a1b;
    //a1r=work->a1r;
    a1x=work->a1x;

// Calculate new coefficients
// First estimate time for each option
    tdown=0.0;
    nleft=nsample;
    for (i=nsample-1;i>=0;i--) {
      if (mask[i]!=0) {
        tdown=tdown+(nleft-i-1)*(nleft-i-1);
        nleft=nleft-1;
      }
    }
    tdown=3.9e-9*tdown;
    tbrute=4e-11*(ngood+0.0)*(ngood+0.0)*(ngood+0.0);

// Constants in if statement based on timings
//  if (nbad < (1.50*sqrt(nsample)-6)) {
    if (tdown<tbrute) {
      t2=dsecnd();
// Get new decomposition by downsampling old one
// Copy decomposed matrix.
// This dominates dcholesky_down when only removing a few points.
// Only need to copy relevant part of matrix
//    for (i=0;i<nsample;i++) for (j=0;j<=i;j++) a[i*nsample+j]=a0[i*nsample+j];
      dlacpy(uplo,&nsample,&nsample,a0,&nsample,a,&nsample);
      t2=dsecnd()-t2;
      t3=dsecnd();

// Get decomposition of reduced matrix
      dcholesky_down_mask(uplo, &nsample, a, &nsample, mask, &info);
      t3=dsecnd()-t3;
    } // if downsampling
    else {
      t4=dsecnd();
// Calculate brute force from original matrix
// Pick good points
      for (i=0;i<ngood;i++) for (j=0;j<=i;j++) a[i*nsample+j]=a00[wgood[i]*nsample+wgood[j]];
      t4=dsecnd()-t4;
      t5=dsecnd();

// Get decomposition of reduced matrix
      dpotrf(uplo,&ngood,a,&nsample,&info); // Cholesky decomposition
      t5=dsecnd()-t5;
    } // Brute force

    t6=dsecnd();

/*
    for (i=0;i<ngood;i++) a1b[i]=1.;
    for (i=0;i<ngood;i++) a1r[i]=rh0[wgood[i]];
    dpotrs(uplo,&ngood,&ione,a,&nsample,a1b,&nsample,&info);
    dpotrs(uplo,&ngood,&ione,a,&nsample,a1r,&nsample,&info);
    bta1b=0.;
    for (i=0;i<ngood;i++) bta1b=bta1b+a1b[i];
    bta1r=0.0;
    for (i=0;i<ngood;i++) bta1r=bta1r+a1r[i];
    c=(bta1r-1.)/bta1b;
    for (i=0;i<ngood;i++) coeff[i]=a1r[i]-c*a1b[i];
    ptr->cnorm=dnrm2(&ngood,coeff,&ione);
*/

// Combine a1b and a1r and make single call to dpotrs
    for (i=0;i<ngood;i++) a1x[i]=1.;
    for (i=0;i<ngood;i++) a1x[i+nsample]=rh0[wgood[i]];
    dpotrs(uplo,&ngood,&itwo,a,&nsample,a1x,&nsample,&info);
    bta1b=0.;
    for (i=0;i<ngood;i++) bta1b=bta1b+a1x[i];
    bta1r=0.0;
    for (i=0;i<ngood;i++) bta1r=bta1r+a1x[i+nsample];
    c=(bta1r-1.)/bta1b;
    for (i=0;i<ngood;i++) coeff[i]=a1x[i+nsample]-c*a1x[i];
    ptr->cnorm=(float)dnrm2(&ngood,coeff,&ione);

    t6=dsecnd()-t6;
    t7=dsecnd();

// Now get interpolation error
    ierror2=0.0;
    for (i=0;i<ngood;i++) ierror2=ierror2+coeff[i]*rh0[wgood[i]];
    ierror2=pars->acort00-2*ierror2;
// Now calculate Sum_wood[i,j] coeff_i A0t_ij coeff_j
    for (j=0;j<ngood;j++) {
/*
      sum=0.0;
      for (i=0;i<ngood;i++) {
        sum=sum+a0t[nsample*wgood[j]+wgood[i]]*coeff[i];
      }
      ierror2=ierror2+sum*coeff[j];
*/
// a0t is symmetric, so this is more efficient
      sum=0.0;
      for (i=0;i<j;i++) {
        sum=sum+a0t[nsample*wgood[j]+wgood[i]]*coeff[i];
      }
      ierror2=ierror2+coeff[j]*(2*sum+coeff[j]*a0t[nsample*wgood[j]+wgood[j]]);
    }
    ptr->ierror=(float)sqrt(ierror2/pars->acort00);

// Stick new entry at beginning of linked list
// Prevent others from starting to traverse
    omp_set_lock(&(pars->locks[hash]));
    pars->hashtable[hash]=ptr;
#pragma omp flush
    omp_unset_lock(&(pars->locks[hash]));

// Done with computation
    omp_unset_lock(&(pars->complocks[hash]));

    t7=dsecnd()-t7;

// Could store coefficients in the other 7 rotations of the mask.
// Beware that the time for cholesky_down depends strongly on which
// one is calculated. See timep.pro for calculation (t2s is a good predictor).
  } // End of calculating new coefficients

  t8=dsecnd();
  fillval=(float)ddot(&ngood,ptr->coeff,&ione,vals,&ione);
  *cnorm=ptr->cnorm;
  *ierror=ptr->ierror;

  t8=dsecnd()-t8;

//printf("%d %d %d %d %d %f %f %f %f %f %f %f %f\n",hash,unlocked,found,omp_get_thread_num(),nbad,t1,t2,t3,t4,t5,t6,t7,t8);
  return fillval;
}

int fgap_fill(
  struct fill_struct *pars,
  float *im,
  int nx,
  int ny,
  int nlead,
  unsigned char *mask,
  float *cnorm,
  float *ierror
)
{
  const unsigned char maskgood=0; // Value of good entries in masks
  const unsigned char maskfill=1; // Value of entries to be filled
  const unsigned char masknofill=2; // Value of entries not to be filled
  int malign=32;
//const int ione = 1;
  int i,j,i1,j1,i0,j0,k;
  int order,order2;
  int ngood,nfill;
  double *im1;
  int *mask1;
  float cnorm1,ierror1;
  int nsample;
  struct work_struct work;
  //double t0;
  
  order=pars->order;
  order2=order/2;
  nsample=order*order;
  
/*
  int *fcount;
  fcount=(int *)(MKL_malloc(ny*sizeof(int),malign));
  t0=dsecnd();
#pragma omp parallel for private (i,j,nfill)
  for (j=0; j<ny; j++) {
    nfill=0;
    for (i=0; i<nx; i++) {
      if (mask[j*nlead+i]==maskfill) nfill++;
    }
    fcount[j]=nfill;
  }
  printf("Count: %f\n",dsecnd()-t0);
  MKL_free(fcount);
*/

nfill=0;
#pragma omp parallel default(none) \
  private (im1,mask1,i,i0,i1,j,j0,j1,k,cnorm1,ierror1,ngood/*,t0*/)     \
shared(pars,im,nx,ny,nlead,mask,cnorm,ierror) \
shared(malign,order,order2) \
private(work) shared(nsample) \
reduction(+:nfill)
{ // Needed to define parallel region

  im1=(double *)(MKL_malloc(order*order*sizeof(double),malign));
  mask1=(int *)(MKL_malloc(order*order*sizeof(int),malign));

  malloc_work(nsample,&work);
//printf("malloc %d %lx %lx\n",omp_get_thread_num(),im1,mask1);

//#pragma omp for schedule(dynamic,1)
//for (j=0; j<ny; j++) {
//  for (i=0; i<nx; i++) {
#pragma omp for schedule(dynamic,256)
  for (k=0;k<nx*ny;k++) {{
        //t0=dsecnd();
    i=k%nx;
    j=k/nx;
      j0=j-order2;
      if (mask[j*nlead+i]==maskgood) {
        cnorm1=1.0f; // Random noise is 1
        ierror1=0.0f; // Interpolation error is 0
      }
      if (mask[j*nlead+i]==maskfill) { // Fill if mask=1. Skip for mask>1.
        i0=i-order2;
        ngood=0;
        for (j1=j0;j1<(j0+order);j1++) {
          for (i1=i0;i1<(i0+order);i1++) {
            if ((i1>=0) && (i1<nx) && (j1>=0) && (j1<ny)) {
              mask1[i1-i0+(j1-j0)*order]=mask[i1+j1*nlead];
              if (mask[j1*nlead+i1]==maskgood) {
                im1[ngood]=im[i1+j1*nlead];
                ngood=ngood+1;
              }
            } // Inside original image
            else { // Outside original image
              mask1[i1-i0+(j1-j0)*order]=1; // Point is missing
            }
          }
        }
        if (ngood != 0) { // Have at least one good point
           im[j*nlead+i]=(float)fill_point(pars,mask1,im1,&cnorm1,&ierror1,&work);
        } 
        else { // Otherwise quietly leave unchanged, except set errors
          cnorm1=0.0f;
          ierror1=1.0f;
        }
        nfill=nfill+1;
      } // end if mask==1
      if (mask[j*nlead+i]==masknofill) { // Don't fill.
        cnorm1=0.0f;
        ierror1=1.0f;
      }
      cnorm[j*nlead+i]=cnorm1;
      ierror[j*nlead+i]=ierror1;
//    cnorm[j*nlead+i]=dsecnd()-t0;
//    ierror[j*nlead+i]=omp_get_thread_num();
    } // for i
  } // for j
//printf("free %d %lx %lx\n",omp_get_thread_num(),im1,mask1);
  MKL_free(im1);
  MKL_free(mask1);
  free_work(nsample,&work);
//printf("done %d %lx %lx\n",omp_get_thread_num(),im1,mask1);
} // End of parallel region
  printf("nfill %d\n",nfill);

  
  return 0; // Current no error conditions
}
