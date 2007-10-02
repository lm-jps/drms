#include "ctypes_def.h"

#if TYPE == FLOAT 
   #define DETREND sdetrend
   #define DETREND_DISCONTIG sdetrend_discontig
   #define LEGENDRE_POLY slegendre_poly
   #define ORTHO_POLY sortho_poly
#elif TYPE == DOUBLE 
   #define DETREND ddetrend
   #define DETREND_DISCONTIG ddetrend_discontig
   #define LEGENDRE_POLY dlegendre_poly
   #define ORTHO_POLY dortho_poly
#endif

//#define DEBUG


static void LEGENDRE_POLY(int degree, int length, RTYPE **poly);
static void ORTHO_POLY(int degree, int length, RTYPE **poly, int *isgood);


void DETREND_DISCONTIG( int n, RTYPE *data, int *isgood, int degree, 
			 int length, int skip, int m, int *sect)
{
#ifndef NDEBUG
  int verbose=1;
#else
  int verbose=0;
#endif
  int i,first,last;
  // Seperately detrend m sections of data separated by 
  // discontinuities.
  // The first section begins on data[0], 
  // the m-1 first sections end on data[sect_last[i]], i=0,1,...,m-2,
  // and the last section ends on data[n-1].

  if (m==0)
    DETREND(n,data,isgood,degree,length,skip);
  else
  {
    for (i=0; i<m; i++)
    {
      first = sect[2*i];
      last = sect[2*i+1];
      //      if (verbose)
      printf("Detrending [%d:%d]\n",first,last);
      DETREND(last-first+1,&data[first],&isgood[first],degree,length,skip);
    }
  }
}
		


void DETREND( int n, RTYPE *data, int *isgood, int degree, 
	      int length, int skip)
{
#ifndef NDEBUG
  int verbose=1;
#else
  int verbose=0;
#endif
  int first,i,d, overlap, start,end;
  RTYPE **legendre_poly, **gap_poly, *fit, gamma, *apodizer;
#ifdef DEBUG
  char name[20];
  static int count=0;
  FILE *fh;
#endif

  if (verbose)
    printf("%s: n=%d, length=%d, skip=%d, degree=%d\n",
	   __func__,n,length,skip,degree);

  fit = (RTYPE *)calloc(n,sizeof(RTYPE));
  // Set up Legendre polynomials
  legendre_poly = (RTYPE **)malloc((degree+1)*sizeof(RTYPE *));
  gap_poly = (RTYPE **)malloc((degree+1)*sizeof(RTYPE *));
  for (i=0; i<=degree; i++)
  {
    legendre_poly[i] = (RTYPE *)malloc(2*length*sizeof(RTYPE));
    gap_poly[i] = (RTYPE *)malloc(2*length*sizeof(RTYPE));
  }
  LEGENDRE_POLY(degree,length, legendre_poly);

  // Set up apodization to stitch together individual fits.
  overlap = length-skip;
  apodizer = (RTYPE *)malloc(overlap*sizeof(RTYPE));
  for (i=0;i<overlap;i++)
  {
    apodizer[i] = cos(M_PI/2*((RTYPE) i+1)/(overlap+1));
    apodizer[i] *= apodizer[i];
  }

  for (first=0; first<=n-length; first+=skip)
  {
    // Possibly grow the last interval to match n.
    if (first+2*length>n )
    {
      length = n-first;
      LEGENDRE_POLY(degree, length, legendre_poly);
    }

    start = first;
    while (start<first+length && !isgood[start])
      start++;
    if (start==first+length)
    {
      /* An empty interval. fill with zeros and skip. */
      for (i=first; i<first+length; i++)
	fit[i] = 0;
      continue;
    }

    end = first+length-1;
    while (end>start && !isgood[end])
      end--;
    end++;

    //    printf("first = %d, start = %d, end = %d.\n",first,start,end);
    if ((end-start) < length/2)
    {
      if (verbose)
	printf("special treatment of [%d:%d], length = %d\n",start,end,length);
      LEGENDRE_POLY(degree, (end-start), legendre_poly);
      // Modify polynomials to be orthonormal wrt. 
      // the local gap structure.
      for (i=0; i<=degree; i++)
	memcpy(gap_poly[i],legendre_poly[i],(end-start)*sizeof(RTYPE));
      ORTHO_POLY(degree,(end-start), gap_poly, &isgood[start]);
      for (i=start; i<end; i++)
	fit[i] = 0;
      for (d=0;d<=degree;d++)
      {
	gamma = 0;
	for (i=start; i<end; i++)
	  if (isgood[i])
	    gamma += gap_poly[d][i-start]*data[i];
	for (i=start; i<end; i++)
	  fit[i] += gamma*gap_poly[d][i-start];
      }
      LEGENDRE_POLY(degree, length, legendre_poly);      
    }
    else
    {
      if (verbose)
	printf("normal treatment [%d:%d]\n",first,first+length-1);
      // Modify polynomials to be orthonormal wrt. 
      // the local gap structure.
      for (i=0; i<=degree; i++)
	memcpy(gap_poly[i],legendre_poly[i],length*sizeof(RTYPE));
      ORTHO_POLY(degree,length, gap_poly, &isgood[first]);

      // Subtract polynomial components one by one.
      if (first!=0)
      {
	for (i=first; i<first+overlap; i++)
	{
	  RTYPE alpha = apodizer[i-first];	  
	  fit[i] = alpha*fit[i];
	}
      }
      for (d=0;d<=degree;d++)
      {
	gamma = 0;
	for (i=first; i<first+length; i++)
	  if (isgood[i])
	    gamma += gap_poly[d][i-first]*data[i];
	if (first!=0)
	  for (i=first; i<first+overlap; i++)
	  {
	    RTYPE alpha = apodizer[i-first];	  
	    fit[i] += (1-alpha)*gamma*gap_poly[d][i-first];
	  }
	else
	  i=first;
	for (; i<first+length; i++)
	  fit[i] += gamma*gap_poly[d][i-first];
      } 
    }
  }    

  for (i=0; i<n; i++)
    if (!isgood[i])      
      fit[i] = 0;
  
  for (i=0; i<n; i++)
    if (isgood[i])      
      data[i] -= fit[i];
    else
      data[i] = 0;

#ifdef DEBUG
  printf("writing debug file #%d\n",count);
  sprintf(name,"detrend_debug_%d.out",count++);
  fh = fopen(name,"w");
  assert(fh);
  for (i=0; i<n; i++)
    fprintf(fh,"%e %e\n",fit[i],data[i]);
  fclose(fh);
#endif

  for (i=0; i<=degree; i++)
  {
    free(legendre_poly[i]);
    free(gap_poly[i]);
  }
  free(legendre_poly);
  free(gap_poly);
  free(fit);  
  free(apodizer);
}


static void LEGENDRE_POLY(int degree, int length, RTYPE **poly)
{
  int d,i;
 
  // Set up Legendre polynomials using recurrence.
  for (i=0;i<(length+1)/2; i++)
  {
    poly[0][i] = 1.0;
    poly[1][i] = -1.0+(2.0*(i+1))/(length+1);
  }  
  for (d=1; d<degree; d++)
    for (i=0; i<(length+1)/2; i++)
      poly[d+1][i] = ((2*d+1)*poly[1][i]*poly[d][i]-d*poly[d-1][i])/(d+1);

  // Set up symmetric / anti-symmetric part.
  for (d=1; d<=degree; d+=2)
    for (i=0; i<length/2; i++)
      poly[d][length-i-1] = -poly[d][i];
  for (d=0; d<=degree; d+=2)
    for (i=0; i<length/2; i++)
      poly[d][length-i-1] = poly[d][i];
}


static void ORTHO_POLY(int degree, int length, RTYPE **poly, int *isgood)
{
  int d,i,j;
  RTYPE norm;
 
  // Normalize wrt. inner product on good part.
  for (d=0; d<=degree; d++)
  {
    norm = 0.0;
    for (i=0; i<length; i++)
      if (isgood[i])
	norm +=  poly[d][i]*poly[d][i];
    norm = sqrt(norm);
    for (i=0; i<length; i++)
      poly[d][i] /= norm;
  }

  // Orthogonalize  wrt. inner product on good part.
  for (d=0; d<=degree; d++)    
    for (j=d-1; j>=0; j--)
    {
      norm = 0;
      for (i=0; i<length; i++)
	if (isgood[i])
	  norm +=  poly[j][i]*poly[d][i];
      for (i=0; i<length; i++)
	poly[d][i] -=  norm*poly[j][i];
    }

  // Normalize again wrt. inner product on good part.
  for (d=0; d<=degree; d++)
  {
    norm = 0.0;
    for (i=0; i<length; i++)
      if (isgood[i])
	norm +=  poly[d][i]*poly[d][i];
    norm = sqrt(norm);
    for (i=0; i<length; i++)
      poly[d][i] /= norm;
  }

}


#undef DETREND
#undef DETREND_DISCONTIG
#undef LEGENDRE_POLY
#undef ORTHO_POLY
#include "ctypes_undef.h"
