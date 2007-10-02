#include "ctypes_def.h"

#if TYPE == FLOAT 
   #define GAPSTRUCTURE sgapstructure
   #define BURG smulti_burg
   #define FAHLMAN_ULRYCH sfahlman_ulrych
   #define FILL_GAPS sfill_gaps
   #define TOEPLITZ_MULT stoeplitz_mult
   #define PRECONDITION sprecondition
   #define PCG spcg
   #define LEVINSON slevinson
   #define EPSILON FLT_EPSILON
#elif TYPE == DOUBLE 
   #define GAPSTRUCTURE dgapstructure
   #define BURG dmulti_burg
   #define FAHLMAN_ULRYCH dfahlman_ulrych
   #define FILL_GAPS dfill_gaps
   #define TOEPLITZ_MULT dtoeplitz_mult
   #define PRECONDITION dprecondition
   #define PCG dpcg
   #define LEVINSON dlevinson
   #define EPSILON DBL_EPSILON
#elif TYPE == COMPLEXFLOAT
   #define GAPSTRUCTURE cgapstructure
   #define BURG cmulti_burg
   #define FAHLMAN_ULRYCH cfahlman_ulrych
   #define FILL_GAPS cfill_gaps
   #define TOEPLITZ_MULT ctoeplitz_mult
   #define PRECONDITION cprecondition
   #define PCG cpcg
   #define LEVINSON clevinson
   #define EPSILON FLT_EPSILON
#elif TYPE == COMPLEXDOUBLE
   #define GAPSTRUCTURE zgapstructure
   #define BURG zmulti_burg
   #define FAHLMAN_ULRYCH zfahlman_ulrych
   #define FILL_GAPS zfill_gaps
   #define TOEPLITZ_MULT ztoeplitz_mult
   #define PRECONDITION zprecondition
   #define PCG zpcg
   #define LEVINSON zlevinson
   #define EPSILON DBL_EPSILON
#endif

#define PCG_MAXIT n_gap
#define PCG_TOL 1e-6

static int GAPSTRUCTURE(int n, CTYPE *data, int *isgood, 
			gapped_timeseries *ts, int **data_length, 
			CTYPE ***good_data, int minpercentage);
static void FILL_GAPS(int n, CTYPE *data, int *isgood, int order, CTYPE *ar_coeff);
static void TOEPLITZ_MULT(int n, CTYPE *x, CTYPE *y, void **data);
static void PRECONDITION(int n, CTYPE *x, CTYPE *y, void **data);

//static void identity(int n, CTYPE *x, CTYPE *y, void **data);



int FAHLMAN_ULRYCH(int n, CTYPE *data_in, int *isgood_in, int minpercentage, 
		   int maxorder, int iterations, int padends, 
		   int *order, CTYPE *ar_coeff_in)
{
#ifndef NDEBUG
  int verbose=1;
#else
  int verbose=0;
#endif
  int i,j, first, last, iter, effective_order;
  RTYPE *err    ;
  gapped_timeseries ts,tsm;
  CTYPE **good_data;
  int *isgood;
  CTYPE *data, nold;
  int *data_length;
  int *model_gaps, oldorder;
  int model_order;
  CTYPE *ar_coeff;

  
  if (ar_coeff_in)
    ar_coeff = ar_coeff_in;
  else
    ar_coeff = malloc(maxorder*sizeof(CTYPE));

  assert(minpercentage>0 && minpercentage<=100);
  // Execute the Fahlman-Ulrych algorithm: 
  // FOR i=1 TO iterations DO
  //   Step 1. Compute AR model using known sample values.
  //   Step 2. For the unknown samples, compute predicted values that
  //           minimize the forward and backward prediction errors in 
  //           the least squares sense.
  // END FOR

  if (padends)
  {
    nold = n;
    n = nold + 2*maxorder;
    data = calloc(n,sizeof(CTYPE));
    isgood = calloc(n,sizeof(int));
    memcpy(&data[maxorder],data_in,nold*sizeof(CTYPE));
    memcpy(&isgood[maxorder],isgood_in,nold*sizeof(int));
  }
  else
  {
    isgood = isgood_in;
    data = data_in;
  }

  oldorder = -1;
  model_order = GAPSTRUCTURE(n, data, isgood, &ts, &data_length, 
			     &good_data, minpercentage);
  tsm = ts;
  model_gaps = malloc(n*sizeof(int));
  memcpy(model_gaps, isgood, n*sizeof(int));
  for (iter=0; iter<iterations; iter++)
  {
    // Analyze gapstructure.
    if (iter>0)
      model_order = GAPSTRUCTURE(n, data, model_gaps, &tsm, &data_length, 
			    &good_data, minpercentage);


    model_order = min( model_order,maxorder);

    // Sort gaps by length and determine the largest AR model order
    // such that "minpercentage" percent of the data will be included
    // in the estimation of the AR coefficients.
    //    model_order =  maxorder(&ts,data_length,minpercentage);    
    err  = (RTYPE *) malloc((model_order+1)*sizeof(RTYPE));

    if (verbose)
    {
      printf("Determining AR(%d) model...",model_order);  
      fflush(stdout);
    }

    effective_order = BURG(tsm.m_data, data_length, good_data, model_order, 
			   ar_coeff, err);

    if (verbose)
    {
      printf("done\n");
      printf("Specified order = %d, Effective order = %d.\n", 
	     model_order, effective_order);
      //      printf("Filling gaps...\n");  
      fflush(stdout);
    }

    model_order = effective_order;
    if (ts.first_is_good)
    {
      i = 1; 
      first = max(0,ts.data_int[0].last-(model_order)+1); 
      last=-1;
    }
    else    
    {
      i = 0; 
      first = 0; 
      last=-1;
    }
    memcpy(model_gaps, isgood, n*sizeof(int));
    while ( i<ts.m_data && last<n-1)
    {
      // move the right-hand end of the interval until we find a
      // data section longer than the model order, i.e. which decouples
      // the unknowns in the gaps on either side.
      while (i<ts.m_data && 
	     (ts.data_int[i].last-ts.data_int[i].first+1)<(model_order) &&
	     (i==0 || (ts.data_int[i].first-ts.data_int[i-1].last-1)<model_order)
	     )
      {	i++; }


      if (i!=0 && (ts.data_int[i].first-ts.data_int[i-1].last)>=model_order)
	last = ts.data_int[i-1].last+(model_order)-1;
      else if ( i>=ts.m_data-1 )
	last = n-1;
      else
	last = ts.data_int[i].first+(model_order)-1;
      
      if (verbose)
	printf("Filling missing data in [ %d : %d ].\n",first,last);

      FILL_GAPS(last-first+1, &data[first], &isgood[first], 
		(model_order), ar_coeff);
    
      first = ts.data_int[i].last-(model_order)+1;
      i++;
    }
    // Mark filled gaps shorter than effective order as good.
    for(i=0; i<ts.m_gap; i++)
    {
      if ( ts.gap_int[i].last-ts.gap_int[i].first+1 <= model_order)
	for (j = ts.gap_int[i].first; j<=ts.gap_int[i].last; j++)
	  model_gaps[j] = 1;
    }
    if (verbose)
      printf("done\n");
    if (iter>0)
    {
      free(tsm.gap_int);  free(tsm.data_int);
    }
    free(data_length);  free(good_data);
    free(err); 

    if (oldorder==model_order || tsm.m_gap==0)
      break;

    oldorder = model_order;
  }
  free(ts.gap_int);  free(ts.data_int);
  memcpy(isgood,model_gaps,n*sizeof(int));
  for (i=0;i<n;i++)
    if (model_gaps[i]==0)
      data[i] = 0;

  free(model_gaps);  


  if (padends)
  {
    memcpy(data_in, &data[maxorder],nold*sizeof(CTYPE));
    memcpy(isgood_in,&isgood[maxorder],nold*sizeof(int));
    free(data);
    free(isgood);
  }

  if (ar_coeff_in==NULL)
    free(ar_coeff);

  if (order)
    *order = model_order;

  return 0;
}


static void FILL_GAPS(int n, CTYPE *data, int *isgood, int order, 
		      CTYPE *ar_coeff)
{
  int iter, idx, i, shift, *gap_idx, *gap_idx_inv, *block;
  int n_gap, n_data, n_blocks, maxblock;
  CTYPE *rhs, *rhs2, *data_gap, *gamma2, *scratch;
  RTYPE rnorm, tol;
  void *aarg[3], *marg[3];
  CTYPE *gamma;


  //  for (i=0; i<10; i++)
  //  printf("data[%d] = %f%+fi.\n",i,creal(data[i]),cimag(data[i]));

  // Compute gamma containing the first row of T^H*T.
  gamma = (CTYPE *)malloc((order+1)*sizeof(CTYPE));
  rhs = (CTYPE *)malloc(2*(n-order)*sizeof(CTYPE));
  for (shift = 0; shift<=order; shift++)
  {
    gamma[shift] = 0;
    for (i=0; (i+shift)<=order; i++) 
      gamma[shift] += 2*CONJ(ar_coeff[i+shift])*ar_coeff[i];
    //     printf("gamma[%d] = %f%+fi\n",shift,creal(gamma[shift]),cimag(gamma[shift]));
  }
  // for (i=0; i<=order; i++)
  //  printf("ar_coeff[%d] = %f%+fi.\n",i,creal(ar_coeff[i]),cimag(ar_coeff[i]));
  

  // Find gaps.
  gap_idx = (int*)calloc(n,sizeof(int));
  gap_idx_inv = (int*)calloc(n,sizeof(int));
  block = (int*)calloc(n,sizeof(int));
  n_data = 0;
  n_gap = 0;
  n_blocks = 0;
  maxblock = 0;
  for (i=0; i<n; i++)
  {
    if (isgood[i]) 
    {
      n_data++;
      if (i>0 && !isgood[i-1])
	n_blocks++;
    }
    else
    {
      // printf("gap at %4d\n",i);
      gap_idx[i] = n_gap;
      gap_idx_inv[n_gap] = i;
      n_gap++;
      block[n_blocks]++;
      if (block[n_blocks] > maxblock)
	maxblock = block[n_blocks];
    }
  }

  /* Set up right-hand side for block Toeplitz system. */
  for (shift=0; (shift+order)<n; shift++)
  {
    rhs[shift] = 0;
    rhs[shift+(n-order)] = 0;
    for (i=0; i<=order; i++)
    {
      if (isgood[i+shift])
      {
	rhs[shift] -= ar_coeff[order-i]*data[i+shift];
	rhs[shift+(n-order)] -= CONJ(ar_coeff[i])*data[i+shift];	
      }
    }
  }
  
  rhs2 = (CTYPE *)calloc(n_gap,sizeof(CTYPE));	 
  for (shift=0; (shift+order)<n; shift++)
  {
    for (i=0; i<=order; i++)
    {
      if ( !isgood[i+shift] )
      {
	idx = gap_idx[i+shift];
	//	assert(idx>=0 && idx < n_gap);
	rhs2[idx] += CONJ(ar_coeff[order-i])*rhs[shift];
	rhs2[idx] += ar_coeff[i]*rhs[shift+(n-order)];
	
      }
    }
  }
  
  //  printf("n_gap = %d\n",n_gap);
  //for (i=0; i<n_gap; i++)
  //  printf("rhs2[%d] = %f%+fi.\n",i,creal(rhs2[i]),cimag(rhs2[i]));
  

  data_gap = (CTYPE*)calloc(n_gap,sizeof(CTYPE));
  if (n_gap == 1)
    data_gap[0] = rhs2[0] / gamma[0];
  else
  {
    // Prepare PCG inputs defining the matrix.
    aarg[0] = (void *) &order;
    aarg[1] = (void *) gap_idx_inv;
    aarg[2] = (void *) gamma;

    // Prepare PCG inputs defining the preconditioner.
    gamma2 = (CTYPE *) calloc(maxblock,sizeof(CTYPE));
    scratch = (CTYPE *) calloc(maxblock,sizeof(CTYPE));
    for (i=0; i<=min(order,maxblock-1); i++)
      gamma2[i] = gamma[i];
    marg[0] = (void *) block;
    marg[1] = (void *) gamma2;
    marg[2] = (void *) scratch;
  
    // Solve normal equations using PCG with a block Toeplitz preconditioner.
    memset(data_gap, 0, n_gap*sizeof(CTYPE));
    tol = sqrt(order)*PCG_TOL;
    iter = PCG(n_gap, PCG_MAXIT+2, tol, TOEPLITZ_MULT, 
	       PRECONDITION, rhs2, data_gap, &rnorm, aarg, marg);
    if (rnorm>tol)
      fprintf(stderr, "Warning: PCG did not converge. "
	      "After %d iterations (maxit=%d) the relative"
	      " residual was %e (tol=%e)\n",
	      iter, PCG_MAXIT+2, rnorm,tol);
 
    /*    printf("n_gap=%3d, maxblock=%3d, iter=%2d, rnorm=%e\n", 
	  n_gap, maxblock, iter,rnorm); */
    //    for (i=0; i<n_gap; i++)
    //printf("x[%d] = %f%+fi\n",i,creal(data_gap[i]),cimag(data_gap[i]));
    free(scratch);  free(gamma2); 
  }
  // Fill gap the original time series with extrapolated data.
  for (i=0; i<n_gap; i++)
    data[gap_idx_inv[i]] = data_gap[i];

  free(block); free(gap_idx); free(gap_idx_inv);  
  free(data_gap); free(gamma); free(rhs); free(rhs2);
}



static int GAPSTRUCTURE(int n, CTYPE *data, int *isgood, gapped_timeseries *ts,
		  int **data_length, CTYPE ***good_data, int minpercentage)
{
#ifndef NDEBUG
  int verbose=1;
#else
  int verbose=0;
#endif
  int i,j,first;

 // Count number of good and bad points and
  // number of good and bad intervals.
  ts->n_data = 0;  ts->m_data = 0;
  ts->n_gap = 0;   ts->m_gap = 0;
  for (i=0; i<n-1; i++)
  {
    assert(isgood[i]==1 || isgood[i]==0);
    if (isgood[i]==1)
    {
      ts->n_data++;
      if (i==0)
      {
	ts->first_is_good = 1;
	ts->m_data++;
      }
      if (isgood[i+1]==0)
	ts->m_gap++;
    }
    else
    {
      ts->n_gap++;
      if (i==0)
      {
	ts->first_is_good = 0;
	ts->m_gap++;
      }
      if (isgood[i+1]==1)
	ts->m_data++;
    }
  }
  if (isgood[n-1]==1)
      ts->n_data++;
  else    
      ts->n_gap++;


  if (verbose)
    printf("n_data = %d, m_data = %d\nn_gap = %d, m_gap = %d\n",
	   ts->n_data,ts->m_data,ts->n_gap,ts->m_gap);

  assert((isgood[0]==0 && isgood[n-1]==0 && ts->m_gap == ts->m_data+1) ||
	 (isgood[0]==1 && isgood[n-1]==1 && ts->m_gap == ts->m_data-1) ||
	 (isgood[0]!=isgood[n-1] && ts->m_gap == ts->m_data));


  // Allocate space for data pointers and AR coefficients. 
  *good_data  = (CTYPE **) malloc(ts->m_data*sizeof(CTYPE *));
  *data_length = (int *) malloc(ts->m_data*sizeof(int));
  ts->gap_int = (interval *) malloc(ts->m_gap*sizeof(interval));
  ts->data_int = (interval *) malloc(ts->m_data*sizeof(interval));
  

  // Scan again and set up data structures describing gap structure. 
  j = 0;
  first=0;
  if (isgood[0]==1)
  {
    first=0;
    ts->data_int[0].first = 0;
    (*good_data)[0] = &data[0];
    j++;
  }
  for (i=0; i<n-1; i++)
  {
    // first of an interval
    if (isgood[i]==0 && isgood[i+1]==1)
    {
      first = i+1;
      ts->data_int[j].first = first;
      (*good_data)[j] = &(data[i+1]);
      //      printf("first[%d] = %d\n",j,i+1);
      j++;
    }
    // end of an interval
    if (isgood[i]==1 && isgood[i+1]==0)
    {
      (*data_length)[j-1] = i-first+1;
      ts->data_int[j-1].last = i;
    }
  }
  if (isgood[n-1]==1)
  {
    (*data_length)[j-1] = n-1-first+1;
    ts->data_int[j-1].last = n-1;
  }
  if (ts->first_is_good)
  {
    for(j=0; j<ts->m_data;j++)
    {
      if (j<ts->m_gap)
	  ts->gap_int[j].first = ts->data_int[j].last+1;
      if (j>0)
	  ts->gap_int[j-1].last = ts->data_int[j].first-1;
    }
    if (ts->m_data==ts->m_gap)
      ts->gap_int[ts->m_gap-1].last = n-1;
  }
  else
  {
    ts->gap_int[0].first = 0;
    for(j=0; j<ts->m_data;j++)
    {
      if (j<ts->m_gap)
	  ts->gap_int[j].last = ts->data_int[j].first-1;
      if (j<ts->m_gap-1)
	  ts->gap_int[j+1].first = ts->data_int[j].last+1;
    }
    if (ts->m_gap==ts->m_data+1)
      ts->gap_int[ts->m_gap-1].last = n-1;
  }
  return maxorder(ts, *data_length, minpercentage);
}



// Matrix-vector multiply with symmetric Toeplitz matrix.
// y = T(r) x

static void TOEPLITZ_MULT(int n, CTYPE *x, CTYPE *y, void **data)
{
  int i,j,order,idx,ii;
  int *gap_idx_inv;
  CTYPE *r;

  order = *((int *) data[0]);
  gap_idx_inv = (int *) data[1];
  r = (CTYPE *) data[2];

  for (i=0; i<n; i++)
    y[i] = r[0]*x[i];

  for (i=0; i<n; i++)
  {
    ii = gap_idx_inv[i];
    for (j=i+1; j<n; j++)
    {
      idx = ii-gap_idx_inv[j];
      if (idx <= order && idx >= -order)
      {
	if (idx>=0)
	{
	  y[i] += CONJ(r[idx])*x[j];
	  y[j] += r[idx]*x[i];
	}
	else
	{
	  y[i] += r[-idx]*x[j];
	  y[j] += CONJ(r[-idx])*x[i];
	}
      }
    }
  }
}

// Solve M y = x;
static void PRECONDITION(int n, CTYPE *b, CTYPE *x, void **data)
{
  int i,first,*block;
  CTYPE *gamma, *scratch;
  block = (int *) data[0];
  gamma = (CTYPE *) data[1];
  scratch = (CTYPE *) data[2];
  

  for(i=0, first = 0; first<n; first+=block[i], i++)
  {
    memcpy(scratch,&b[first],block[i]*sizeof(CTYPE));
    LEVINSON(block[i],gamma,scratch,&x[first]);
  }
}


#undef GAPSTRUCTURE
#undef FAHLMAN_ULRYCH 
#undef BURG
#undef FILL_GAPS
#undef TOEPLITZ_MULT
#undef PRECONDITION
#undef PCG
#undef LEVINSON
#undef EPSILON
#include "ctypes_undef.h"
