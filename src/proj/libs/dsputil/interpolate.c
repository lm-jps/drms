#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>
#include "interpolate.h"



/* Macro to aid branch prediction when using the GCC compiler. */
#ifdef __GNUC__
#define likely(a) __builtin_expect((a), 1)
#define unlikely(a) __builtin_expect((a), 0)
#else
#define likely(a)   (a)
#define unlikely(a) (a)
#endif

/* Static declarations of interpolation routines. */
static inline void dc3kernel (double u[4], double s);
static inline void fc3kernel (float u[4], float s);
static inline void dc4kernel (double u[6], double s);
static inline void fc4kernel (float u[6], float s);
static inline float fc3convolve(int ix, int iy, int nx, int ny, float *f, 
				float *ux, float *uy, float fillvalue);
static inline double dc3convolve(int ix, int iy, int nx, int ny, double *f, 
				 double *ux, double *uy, double fillvalue);
static inline double dc4convolve(int ix, int iy, int nx, int ny, double *f, 
				 double *ux, double *uy, double fillvalue);
static inline float fc4convolve(int ix, int iy, int nx, int ny, float *f, 
				float *ux, float *uy, float fillvalue);



/************* High level interpolation routines *****************/

/* Interpolate to get the value of the nx-by-ny image stored
   in f at the point (x,y). If (x,y) is outside [0:nx-1]x[0:ny-1]
   the value "fillvalue is returned. The interpolation order 
   can be either 3 or 4. 3rd or 4th order convolutional 
   spline interpolation is used. */
float fcint(int order, int nx, int ny, float *f,
	   float x, float y, float fillvalue)
{
  float ux[6], uy[6];
  int ix, iy;

  if ( unlikely(x < 0.0f || x > (float)(nx-1) ||
		y < 0.0f || y > (float)(ny-1)) )
    return fillvalue;
  else 
  {	
    iy = (int)y;
    ix = (int)x;
    switch(order)
    {
    case 3:
      fc3kernel (uy,  y - (float)iy); 
      fc3kernel (ux,  x - (float)ix);
      return fc3convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
      break;
    case 4:
      fc4kernel (uy,  y - (float)iy); 
      fc4kernel (ux,  x - (float)ix);
      return fc4convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
      break;
    default:
      abort();
    }
  }
}


/* Interpolate to get the value of the nx-by-ny image stored
   in f at the point (x,y). If (x,y) is outside [0:nx-1]x[0:ny-1]
   the value "fillvalue is returned. The interpolation order 
   can be either 3 or 4. 3rd or 4th order convolutional 
   spline interpolation is used. */
double dcint(int order, int nx, int ny, double *f,
	   double x, double y, double fillvalue)
{
  double ux[6], uy[6];
  int ix, iy;

  if ( unlikely(x < 0.0 || x > (double)(nx-1) ||
		y < 0.0 || y > (double)(ny-1)) )
    return fillvalue;
  else 
  {	
    iy = (int)y;
    ix = (int)x;
    switch(order)
    {
    case 3:
      dc3kernel (uy,  y - (double)iy); 
      dc3kernel (ux,  x - (double)ix);
      return dc3convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
      break;
    case 4:
      dc4kernel (uy,  y - (double)iy); 
      dc4kernel (ux,  x - (double)ix);
      return dc4convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
      break;
    default:
      abort();
    }
  }
}



/* Interpolate multiple target points at a time. The interpolation
   coordinates are input in X and Y and the interpolated values are
   returned in g. */
void fcint_vect(int order, int nx, int ny, float *f, float *g,
		 int N, float *X, float *Y, float fillvalue)
{
  int i;
  float x, y;
  float ux[6], uy[6];
  int ix, iy;

  for (i=0; i<N; i++)
  {
    x = X[i];
    y = Y[i];
    
    if ( unlikely(x < 0.0f || x > (float)(nx-1) ||
		  y < 0.0f || y > (float)(ny-1)) )
      g[i] = fillvalue;
    else 
    {	
      iy = (int)y;
      ix = (int)x;
      switch(order)
      {
      case 3:
	fc3kernel (uy,  y - (float)iy); 
	fc3kernel (ux,  x - (float)ix);
	g[i] = fc3convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	break;
      case 4:
	fc4kernel (uy,  y - (float)iy); 
	fc4kernel (ux,  x - (float)ix);
	g[i] = fc4convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	break;
      default:
	abort();
      }
    }
  }
}


/* Interpolate multiple target points at a time. The interpolation
   coordinates are input in X and Y and the interpolated values are
   returned in g. */
void dcint_vect(int order, int nx, int ny, double *f, double *g,
		  int N, double *X, double *Y, double fillvalue)
{
  int i;
  double x, y;
  double ux[6], uy[6];
  int ix, iy;

  for (i=0; i<N; i++)
  {
    x = X[i];
    y = Y[i];
    
    if ( unlikely(x < 0.0 || x > (double)(nx-1) ||
		  y < 0.0 || y > (double)(ny-1)) )
      g[i] = fillvalue;
    else 
    {	
      iy = (int)y;
      ix = (int)x;
      switch(order)
      {
      case 3:
	dc3kernel (uy,  y - (double)iy); 
	dc3kernel (ux,  x - (double)ix);
	g[i] = dc3convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	break;
      case 4:
	dc4kernel (uy,  y - (double)iy); 
	dc4kernel (ux,  x - (double)ix);
	g[i] = dc4convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	break;
      default:
	abort();
      }
    }
  }
}

/******* High level image transformation routines. ***********/


/* Shift image by (dx,dy). */
void fshift(int order, int nx, int ny, float *f, float *g,
	     float dx, float dy, float fillvalue)
{
  int i,j;
  float x, y;
  float ux[6], uy[6];
  int ix, iy;


  y = dy;
  iy = (int)y;
  x = dx;
  ix = (int)x;
  switch(order)
  {
  case 3:
    fc3kernel (uy,  dy - iy); 
    fc3kernel (ux,  dx - ix);
    break;
  case 4:
    fc4kernel (uy,  dy - iy); 
    fc4kernel (ux,  dx - ix);
    break;
  default:
    abort();
  }

  y = dy;
  iy = (int)y;
  for (i=0; i<ny; i++)
  {
    x = dx;
    ix = (int)x;
    if ( unlikely( y < 0.0f || y > (float)(ny-1)) )
      for (j=0; j<nx; j++)
	g[i*nx+j] = fillvalue;
    
    for (j=0; j<nx; j++)
    {
      if ( unlikely(x < 0.0f || x > (float)(nx-1)) )      
	g[i*nx+j] = fillvalue;
      else
      {	  
	switch(order)
	{
	case 3:
	  g[i*nx+j] = fc3convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	  break;
	case 4:
	  g[i*nx+j] = fc4convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	  break;
	default:
	  abort();
	}
      }
      x += 1.0f;
      ix++;
    }
    y += 1.0f;    
    iy++;
  }
}



/* Shift image by (dx,dy). */
void dshift(int order, int nx, int ny, double *f, double *g,
	     double dx, double dy, double fillvalue)
{
  int i,j;
  double x, y;
  double ux[6], uy[6];
  int ix, iy;


  switch(order)
  {
  case 3:
    dc3kernel (uy,  dy); 
    dc3kernel (ux,  dx);
    break;
  case 4:
    dc4kernel (uy,  dy); 
    dc4kernel (ux,  dx);
    break;
  default:
    abort();
  }

  y = dy;
  iy = (int)y;
  for (i=0; i<ny; i++)
  {
    x = dx;
    ix = (int)x;
    if ( unlikely( y < 0.0f || y > (double)(ny-1)) )
      for (j=0; j<nx; j++)
	g[i*nx+j] = fillvalue;
    
    for (j=0; j<nx; j++)
    {
      if ( unlikely(x < 0.0f || x > (double)(nx-1)) )      
	g[i*nx+j] = fillvalue;
      else
      {	  
	switch(order)
	{
	case 3:
	  g[i*nx+j] = dc3convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	  break;
	case 4:
	  g[i*nx+j] = dc4convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	  break;
	default:
	  abort();
	}
      }
      x += 1.0;
      ix++;
    }
    y += 1.0;    
    iy++;
  }
}


/* Resample an image of size (nx,ny) in the input array f to
   the size (new_nx,new_ny) output array g. */
void fscale(int order, int nx, int ny, float *f,
	    int new_nx, int new_ny, float *g)
{
  int i,j;
  float x, y;
  float ux[6], uy[6], stepx, stepy;
  int ix, iy;

  stepx = ((float) (nx-1))/(new_nx-1);
  stepy = ((float) (ny-1))/(new_ny-1);
  for (i=0; i<new_ny; i++)
  {
    y = stepy*i;
    for (j=0; j<new_nx; j++)
    {
      x = stepx*j;
      iy = (int)y;
      ix = (int)x;
      switch(order)
      {
      case 3:
	fc3kernel (uy,  y - (float)iy); 
	fc3kernel (ux,  x - (float)ix);
	g[i*new_nx+j] = fc3convolve(ix,iy,nx,ny,f,ux,uy,0.0f);
	break;
      case 4:
	fc4kernel (uy,  y - (float)iy); 
	fc4kernel (ux,  x - (float)ix);
	g[i*new_nx+j] = fc4convolve(ix,iy,nx,ny,f,ux,uy,0.0f);
	break;
      default:
	abort();
      }
    }
  }
}




/* Resample an image of size (nx,ny) in the input array f to
   the size (new_nx,new_ny) output array g. */
void dscale(int order, int nx, int ny, double *f,
	    int new_nx, int new_ny, double *g)
{
  int i,j;
  double x, y;
  double ux[6], uy[6], stepx, stepy;
  int ix, iy;

  stepx = ((double) (nx-1))/(new_nx-1);
  stepy = ((double) (ny-1))/(new_ny-1);
  for (i=0; i<new_ny; i++)
  {
    y = stepy*i;
    for (j=0; j<new_nx; j++)
    {
      x = stepx*j;
      iy = (int)y;
      ix = (int)x;
      switch(order)
      {
      case 3:
	dc3kernel (uy,  y - (double)iy); 
	dc3kernel (ux,  x - (double)ix);
	g[i*new_nx+j] = dc3convolve(ix,iy,nx,ny,f,ux,uy,0.0);
	break;
      case 4:
	dc4kernel (uy,  y - (double)iy); 
	dc4kernel (ux,  x - (double)ix);
	g[i*new_nx+j] = dc4convolve(ix,iy,nx,ny,f,ux,uy,0.0);
	break;
      default:
	abort();
      }
    }
  }
}


/* Remap the image in f by an affine coordinate transformation. */
void faffine(int order, int nx, int ny, float *f, float *g,
	     float a11, float a12, float a21, float a22, 
	     float dx, float dy, float fillvalue)
{
  int i,j;
  float x0 ,y0, x, y;
  float ux[6], uy[6];
  int ix, iy;

  //  printf("order %d\n",order);
  x0 = dx;
  y0 = dy;    
  for (i=0; i<ny; i++)
  {
  //  x0 = a12*(float)i + dx;
  //  y0 = a22*(float)i + dy;    
    x = x0;
    y = y0;
    for (j=0; j<nx; j++)
    {
        x = x0 + a11*(float)j;
        y = y0 + a21*(float)j;

      if ( unlikely(x < 0.0f || x > (float)(nx-1) ||
		    y < 0.0f || y > (float)(ny-1)) )
	g[i*nx+j] = fillvalue;
      else
      {	  
	iy = (int)y;
	ix = (int)x;
	switch(order)
	{
	case 3:
	  fc3kernel (uy,  y - (float)iy); 
	  fc3kernel (ux,  x - (float)ix);
	  g[i*nx+j] = fc3convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	  break;
	case 4:
	  fc4kernel (uy,  y - (float)iy); 
	  fc4kernel (ux,  x - (float)ix);
	  g[i*nx+j] = fc4convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	  break;
	default:
	  abort();
	}
      }
      x += a11;
      y += a21;    
    }
    x0 += a12;
    y0 += a22;    
  }
}

/* Remap the image in f by an affine coordinate transformation. */
void daffine(int order, int nx, int ny, double *f, double *g,
	     double a11, double a12, double a21, double a22, 
	     double dx, double dy, double fillvalue)
{
  int i,j;
  double x0 ,y0, x, y;
  double ux[6], uy[6];
  int ix, iy;

  for (i=0; i<ny; i++)
  {
    x0 = a12*(double)i + dx;
    y0 = a22*(double)i + dy;    
    for (j=0; j<nx; j++)
    {
      x = x0 + a11*(double)j;
      y = y0 + a21*(double)j;

      if ( unlikely(x < 0.0 || x > (double)(nx-1) ||
		    y < 0.0 || y > (double)(ny-1)) )
	g[i*nx+j] = fillvalue;
      else
      {	  
	iy = (int)y;
	ix = (int)x;
	switch(order)
	{
	case 3:
	  dc3kernel (uy,  y - (double)iy); 
	  dc3kernel (ux,  x - (double)ix);
	  g[i*nx+j] = dc3convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	  break;
	case 4:
	  dc4kernel (uy,  y - (double)iy); 
	  dc4kernel (ux,  x - (double)ix);
	  g[i*nx+j] = dc4convolve(ix,iy,nx,ny,f,ux,uy,fillvalue);
	  break;
	default:
	  abort();
	}
      }
    }
  }
}

/* ========= optimized interpolation kernel and convolution routines ======= */

/* Third order cubic convolution kernel 
   using sample pattern
   x1 < x2 < x < x3 < x4 .

   These kernels are unnormalized, so the convolution sum 
   must be multiplied by a factor of 1/2 (for each dimension).  
*/
static inline void dc3kernel (double u[4], double s)
{
  double s2;
  s2 = s*s;
  u[0] = -s*(1.0 - s*(2.0 - s));
  u[1] = 2.0 - s2*(5.0 - 3.0*s);
  u[2] = s*(1.0 + s*(4.0 - 3.0*s));
  u[3] = -s2*(1.0-s);
}

static inline void fc3kernel (float u[4], float s)
{
  float s2;
  s2 = s*s;
  u[0] = -s*(1.0f - s*(2.0f - s));
  u[1] = 2.0f - s2*(5.0f - 3.0f*s);
  u[2] = s*(1.0f + s*(4.0f - 3.0f*s));
  u[3] = -s2*(1.0f-s);
}


/* Fourth order cubic convolution kernel 
   using sample pattern
   x1 < x2 < x3 < x < x4 < x5 < x6 

   These kernels are unnormalized, so the convolution sum 
   must be multiplied by a factor of 1/12 (for each dimension).
*/
static inline void dc4kernel (double u[6], double s)
{
  double s2;

  s2 = s * s;
  u[0] = s*(1.0 + s*(-2.0 + s));
  u[1] = s*(-8.0 + s*(15.0 -7.0*s));
  u[2] = 12.0 + s2*(-28.0 + 16.0*s);
  u[3] = s*(8.0 + s*(20.0 - 16.0*s));
  u[4] = s*(-1.0 + s*(-6.0 + 7.0*s));
  u[5] = s2*(1.0 - s);
}

static inline void fc4kernel (float u[6], float s)
{
  float s2;

  s2 = s * s;
  u[0] = s*(1.0f + s*(-2.0f + s));
  u[1] = s*(-8.0f + s*(15.0f -7.0f*s));
  u[2] = 12.0f + s2*(-28.0f + 16.0f*s);
  u[3] = s*(8.0f + s*(20.0f - 16.0f*s));
  u[4] = s*(-1.0f + s*(-6.0f + 7.0f*s));
  u[5] = s2*(1.0f - s);
}


/* Optimized 4x4 convolution routines. Uses 3rd order extrapolation
   at boundaries to interpolate between the first two samples. 
*/

static inline float fc3convolve(int ix, int iy, int nx, int ny, float *f, 
				float *ux, float *uy, float fillvalue)
{
  float *fc;
  float t1,t2,t3;

  fc = &f[iy*nx + ix];
  if ( unlikely(ix == 0) )
  {
    if ( unlikely(iy == 0) )
    {
      t1 = (3.0f*fc[0] - 3.0f*fc[nx] + fc[2*nx]);
      t2 = (3.0f*fc[1] - 3.0f*fc[nx+1] + fc[2*nx+1]);
      t3 = (3.0f*fc[2] - 3.0f*fc[nx+2] + fc[2*nx+2]);
      return
	0.25f*( 
	       (t1*ux[1] + t2*ux[2] + 
		t3*ux[3] + (3.0f*t1 - 3.0f*t2 + t3)*ux[0]) * uy[0] +
	       ((3.0f*fc[0] - 3.0f*fc[1] + fc[2])*ux[0] + fc[0]*ux[1] +
		fc[1]*ux[2] + fc[2]*ux[3]) * uy[1] +
	       ((3.0f*fc[nx] - 3.0f*fc[nx+1] + fc[nx+2])*ux[0] + fc[nx]*ux[1] +
		fc[nx+1]*ux[2] + fc[nx+2]*ux[3]) * uy[2] +
	       ((3.0f*fc[2*nx] - 3.0f*fc[2*nx+1] + fc[2*nx+2])*ux[0] + fc[2*nx]*ux[1] + 
		fc[2*nx+1]*ux[2] + fc[2*nx+2]*ux[3]) * uy[3]
	       );
    }
    else if ( unlikely(iy >= ny-2) )
    {
      if (iy == ny-1)
      {
	fc = &fc[-nx];
	uy[0] = 0.0f;
	uy[1] = 0.0f;
	uy[2] = 2.0f;
	uy[3] = 0.0f;
      }
      
      t1 = (fc[  -nx] - 3.0f*fc[0] + 3.0f*fc[nx]);	            
      t2 = (fc[-nx+1] - 3.0f*fc[1] + 3.0f*fc[nx+1]); 	            
      t3 = (fc[-nx+2] - 3.0f*fc[2] + 3.0f*fc[nx+2]); 	            
      return 
	0.25f*( 
	       ((3.0f*fc[-nx] - 3.0f*fc[-nx+1] + fc[-nx+2])*ux[0] + fc[-nx]*ux[1] +
		fc[-nx+1]*ux[2] + fc[-nx+2]*ux[3]) * uy[0] +
	       ((3.0f*fc[  0] - 3.0f*fc[    1] + fc[    2])*ux[0] + fc[  0]*ux[1] +
		fc[    1]*ux[2] + fc[    2]*ux[3]) * uy[1] +
	       ((3.0f*fc[ nx] - 3.0f*fc[ nx+1] + fc[ nx+2])*ux[0] + fc[ nx]*ux[1] +
		fc[ nx+1]*ux[2] + fc[ nx+2]*ux[3]) * uy[2] +
	       ((3.0f*t1-3.0f*t2+t3)                       *ux[0] +      t1*ux[1] + 
		t2       *ux[2] + t3       *ux[3]) * uy[3]
	       );
    }
    else
    {
      return
	0.25f*( 
	       ((3.0f*fc[-nx] - 3.0f*fc[-nx+1] + fc[-nx+2])*ux[0] + fc[-nx  ]*ux[1] + 
		fc[-nx+1]*ux[2] + fc[-nx+2]*ux[3]) * uy[0] + 
	       ((3.0f*fc[0] - 3.0f*fc[1] + fc[2])*ux[0] + fc[0]*ux[1] + 
		fc[1]*ux[2] + fc[2]*ux[3]) * uy[1] + 
	       ((3.0f*fc[nx] - 3.0f*fc[nx+1] + fc[nx+2])*ux[0] + fc[nx  ]*ux[1] +
		fc[nx+1]*ux[2] + fc[nx+2]*ux[3]) * uy[2] +
	       ((3.0f*fc[2*nx] - 3.0f*fc[2*nx+1] + fc[2*nx+2])*ux[0] + fc[2*nx  ]*ux[1] +
		fc[2*nx+1]*ux[2] + fc[2*nx+2]*ux[3]) * uy[3]
	       );
    }
  }
  else if ( unlikely(ix >= nx-2) )
  {
    if (ix == nx-1)
    {
      fc = &fc[-1];
      ux[0] = 0.0f;
      ux[1] = 0.0f;
      ux[2] = 2.0f;
      ux[3] = 0.0f;
    }
    if ( unlikely(iy == 0) )
    {
      t1 = (3.0f*fc[-1] - 3.0f*fc[nx-1] + fc[2*nx-1]);
      t2 = (3.0f*fc[ 0] - 3.0f*fc[nx] + fc[2*nx]);
      t3 = (3.0f*fc[ 1] - 3.0f*fc[nx+1] + fc[2*nx+1]);
      return
	0.25f*( 
	       (t1*ux[0] + t2*ux[1] +
		t3*ux[2] + (3.0f*t3 - 3.0f*t2 + t1)*ux[3]) * uy[0] +
	       (fc[    -1]*ux[0] + fc[0]*ux[1] + 
		fc[     1]*ux[2]  + (3.0f*fc[1] - 3.0f*fc[0] + fc[-1])*ux[3]) * uy[1] +
	       (fc[  nx-1]*ux[0] + fc[nx]*ux[1] +
		fc[  nx+1]*ux[2] + (3.0f*fc[nx+1] - 3.0f*fc[nx] + fc[nx-1])*ux[3]) * uy[2] +
	       (fc[2*nx-1]*ux[0] + fc[2*nx]*ux[1] +
		fc[2*nx+1]*ux[2] + (3.0f*fc[2*nx+1]-3.0f*fc[2*nx] + fc[2*nx-1])*ux[3]) * uy[3]
	       );
    }
    else if ( unlikely(iy >= ny-2) )
    {
      if (iy == ny-1)
      {
	fc = &fc[-nx];
	uy[0] = 0.0f;
	uy[1] = 0.0f;
	uy[2] = 2.0f;
	uy[3] = 0.0f;
      }
      t1 = (fc[-nx-1] - 3.0f*fc[-1] + 3.0f*fc[nx-1]);
      t2 = (fc[-nx] - 3.0f*fc[0] + 3.0f*fc[nx]);
      t3 = (fc[-nx+1] - 3.0f*fc[1] + 3.0f*fc[nx+1]);
      return
	0.25f*( 
	       (fc[-nx-1]*ux[0] + fc[-nx  ]*ux[1] + 
		fc[-nx+1]*ux[2] + (3.0f*fc[-nx+1] - 3.0f*fc[-nx] + fc[-nx-1])*ux[3]) * uy[0] + 
	       (fc[-1]*ux[0] + fc[0]*ux[1] +
		fc[1]*ux[2] + (3.0f*fc[1] - 3.0f*fc[0] + fc[-1])*ux[3]) * uy[1] + 
	       (fc[nx-1]*ux[0] + fc[nx]*ux[1] + 
		fc[nx+1]*ux[2] + (3.0f*fc[nx+1] - 3.0f*fc[nx] + fc[nx-1])*ux[3]) * uy[2] + 
	       (t1*ux[0] + t2*ux[1] + 
		t3*ux[2] + (3.0f*t3 - 3.0f*t2 + t1)*ux[3])*uy[3]
	       );
    }
    else
    {
      return
	0.25f*( 
	       (fc[-nx-1]*ux[0] + fc[-nx]*ux[1] + 
		fc[-nx+1]*ux[2] + (3.0f*fc[-nx+1] - 3.0f*fc[-nx] + fc[-nx-1])*ux[3]) * uy[0] +
	       (fc[-1]*ux[0] + fc[0 ]*ux[1] + 
		fc[1 ]*ux[2] + (3.0f*fc[1] - 3.0f*fc[0] + fc[-1])*ux[3]) * uy[1] + 
	       (fc[nx-1]*ux[0] + fc[nx  ]*ux[1] + 
		fc[nx+1]*ux[2] + (3.0f*fc[nx+1] - 3.0f*fc[nx] + fc[nx-1])*ux[3]) * uy[2] + 
	       (fc[2*nx-1]*ux[0] + fc[2*nx  ]*ux[1] + 
		fc[2*nx+1]*ux[2] + (3.0f*fc[2*nx+1]-3.0f*fc[2*nx] + fc[2*nx-1])*ux[3]) * uy[3]
	       );
    }
  }
  else
  {
    if ( unlikely(iy == 0) )
    {
      return	    
	0.25f*( 
	       ((3.0f*fc[-1] - 3.0f*fc[nx-1] + fc[2*nx-1])*ux[0] + 
		(3.0f*fc[0] - 3.0f*fc[nx] + fc[2*nx])*ux[1] + 
		(3.0f*fc[1] - 3.0f*fc[nx+1] + fc[2*nx+1])*ux[2] + 
		(3.0f*fc[2] - 3.0f*fc[nx+2] + fc[2*nx+2])*ux[3] ) * uy[0] +
	       (fc[-1]*ux[0] + fc[0 ]*ux[1] + 
		fc[1 ]*ux[2] + fc[2 ]*ux[3]) * uy[1] + 
	       (fc[nx-1]*ux[0] + fc[nx  ]*ux[1] + 
		fc[nx+1]*ux[2] + fc[nx+2]*ux[3]) * uy[2] + 
	       (fc[2*nx-1]*ux[0] + fc[2*nx  ]*ux[1] + 
		fc[2*nx+1]*ux[2] + fc[2*nx+2]*ux[3]) * uy[3] 
	       );
    }
    else if ( unlikely(iy >= ny-2) )
    {
      if (iy == ny-1)
      {
	fc = &fc[-nx];
	uy[0] = 0.0f;
	uy[1] = 0.0f;
	uy[2] = 2.0f;
	uy[3] = 0.0f;
      }
      return
	0.25f*( 
	       (fc[-nx-1]*ux[0] + fc[-nx  ]*ux[1] + 
		fc[-nx+1]*ux[2] + fc[-nx+2]*ux[3]) * uy[0] + 
	       (fc[-1]*ux[0] + fc[0 ]*ux[1] + 
		fc[1 ]*ux[2] + fc[2 ]*ux[3]) * uy[1] + 
	       (fc[nx-1]*ux[0] + fc[nx  ]*ux[1] + 
		fc[nx+1]*ux[2] + fc[nx+2]*ux[3]) * uy[2] + 
	       ((fc[-nx-1] - 3.0f*fc[-1] + 3.0f*fc[nx-1])*ux[0] + 
		(fc[-nx] - 3.0f*fc[0] + 3.0f*fc[nx])*ux[1] + 
		(fc[-nx+1] - 3.0f*fc[1] + 3.0f*fc[nx+1])*ux[2] + 
		(fc[-nx+2] - 3.0f*fc[2] + 3.0f*fc[nx+2])*ux[3])*uy[3]
	       );
    }
    else
    {
      return
	0.25f*( 
	       (fc[ -nx-1]*ux[0] + fc[ -nx  ]*ux[1] +
		fc[ -nx+1]*ux[2] + fc[ -nx+2]*ux[3])*uy[0] +
	       (fc[    -1]*ux[0] + fc[     0]*ux[1]+
		fc[     1]*ux[2] + fc[     2]*ux[3])*uy[1] +
	       (fc[  nx-1]*ux[0] + fc[  nx  ]*ux[1] +
		fc[  nx+1]*ux[2] + fc[  nx+2]*ux[3])*uy[2] +
	       (fc[2*nx-1]*ux[0] + fc[2*nx  ]*ux[1] +
		fc[2*nx+1]*ux[2] + fc[2*nx+2]*ux[3])*uy[3]
	       );
    }
  }
}





static inline double dc3convolve(int ix, int iy, int nx, int ny, double *f, 
				 double *ux, double *uy, double fillvalue)
{
  double *fc;
  double t1,t2,t3;

  fc = &f[iy*nx + ix];
  if ( unlikely(ix == 0) )
  {
    if ( unlikely(iy == 0) )
    {
      t1 = (3.0*fc[0] - 3.0*fc[nx] + fc[2*nx]);
      t2 = (3.0*fc[1] - 3.0*fc[nx+1] + fc[2*nx+1]);
      t3 = (3.0*fc[2] - 3.0*fc[nx+2] + fc[2*nx+2]);
      return
	0.25*( 
	       (t1*ux[1] + t2*ux[2] + 
		t3*ux[3] + (3.0*t1 - 3.0*t2 + t3)*ux[0]) * uy[0] +
	       ((3.0*fc[0] - 3.0*fc[1] + fc[2])*ux[0] + fc[0]*ux[1] +
		fc[1]*ux[2] + fc[2]*ux[3]) * uy[1] +
	       ((3.0*fc[nx] - 3.0*fc[nx+1] + fc[nx+2])*ux[0] + fc[nx]*ux[1] +
		fc[nx+1]*ux[2] + fc[nx+2]*ux[3]) * uy[2] +
	       ((3.0*fc[2*nx] - 3.0*fc[2*nx+1] + fc[2*nx+2])*ux[0] + fc[2*nx]*ux[1] + 
		fc[2*nx+1]*ux[2] + fc[2*nx+2]*ux[3]) * uy[3]
	       );
    }
    else if ( unlikely(iy >= ny-2) )
    {
      if (iy == ny-1)
      {
	fc = &fc[-nx];
	uy[0] = 0.0;
	uy[1] = 0.0;
	uy[2] = 2.0;
	uy[3] = 0.0;
      }

      t1 = (fc[  -nx] - 3.0*fc[0] + 3.0*fc[nx]);	            
      t2 = (fc[-nx+1] - 3.0*fc[1] + 3.0*fc[nx+1]); 	            
      t3 = (fc[-nx+2] - 3.0*fc[2] + 3.0*fc[nx+2]); 	            
      return
	0.25*( 
	       ((3.0*fc[-nx] - 3.0*fc[-nx+1] + fc[-nx+2])*ux[0] + fc[-nx]*ux[1] +
		fc[-nx+1]*ux[2] + fc[-nx+2]*ux[3]) * uy[0] +
	       ((3.0*fc[0] - 3.0*fc[1] + fc[2])*ux[0] + fc[0]*ux[1] +
		fc[1]*ux[2] + fc[2]*ux[3]) * uy[1] +
	       ((3.0*fc[nx] - 3.0*fc[nx+1] + fc[nx+2])*ux[0] + fc[nx]*ux[1] +
		fc[nx+1]*ux[2] + fc[nx+2]*ux[3]) * uy[2] +
	       (t1*ux[1] + t2*ux[2] +
		t3*ux[3] + (3.0*t1-3.0*t2+t3)*ux[0]) * uy[3]
	       );
    }
    else
    {
      return
	0.25*( 
	       ((3.0*fc[-nx] - 3.0*fc[-nx+1] + fc[-nx+2])*ux[0] + fc[-nx  ]*ux[1] + 
		fc[-nx+1]*ux[2] + fc[-nx+2]*ux[3]) * uy[0] + 
	       ((3.0*fc[0] - 3.0*fc[1] + fc[2])*ux[0] + fc[0]*ux[1] + 
		fc[1]*ux[2] + fc[2]*ux[3]) * uy[1] + 
	       ((3.0*fc[nx] - 3.0*fc[nx+1] + fc[nx+2])*ux[0] + fc[nx  ]*ux[1] +
		fc[nx+1]*ux[2] + fc[nx+2]*ux[3]) * uy[2] +
	       ((3.0*fc[2*nx] - 3.0*fc[2*nx+1] + fc[2*nx+2])*ux[0] + fc[2*nx  ]*ux[1] +
		fc[2*nx+1]*ux[2] + fc[2*nx+2]*ux[3]) * uy[3]
	       );
    }
  }
  else if ( unlikely(ix >= nx-2) )
  {
    if (ix == nx-1)
    {
      fc = &fc[-1];
      ux[0] = 0.0;
      ux[1] = 0.0;
      ux[2] = 2.0;
      ux[3] = 0.0;
    }
    if ( unlikely(iy == 0) )
    {
      t1 = (3.0*fc[-1] - 3.0*fc[nx-1] + fc[2*nx-1]);
      t2 = (3.0*fc[0] - 3.0*fc[nx] + fc[2*nx]);
      t3 = (3.0*fc[1] - 3.0*fc[nx+1] + fc[2*nx+1]);
      return
	0.25*( 
	       (t1*ux[0] + t2*ux[1] +
		t3*ux[2] + (3.0*t3 - 3.0*t2 + t1)*ux[3]) * uy[0] +
	       (fc[-1]*ux[0] + fc[0]*ux[1] + 
		fc[1]*ux[2]  + (3.0*fc[1] - 3.0*fc[0] + fc[-1])*ux[3]) * uy[1] +
	       (fc[nx-1]*ux[0] + fc[nx]*ux[1] +
		fc[nx+1]*ux[2] + (3.0*fc[nx+1] - 3.0*fc[nx] + fc[nx-1])*ux[3]) * uy[2] +
	       (fc[2*nx-1]*ux[0] + fc[2*nx]*ux[1] +
		fc[2*nx+1]*ux[2] + (3.0*fc[2*nx+1]-3.0*fc[2*nx] + fc[2*nx-1])*ux[3]) * uy[3]
	       );
    }
    else if ( unlikely(iy >= ny-2) )
    {
      if (iy == ny-1)
      {
	fc = &fc[-nx];
	uy[0] = 0.0;
	uy[1] = 0.0;
	uy[2] = 2.0;
	uy[3] = 0.0;
      }

      t1 = (fc[-nx-1] - 3.0*fc[-1] + 3.0*fc[nx-1]);
      t2 = (fc[-nx] - 3.0*fc[0] + 3.0*fc[nx]);
      t3 = (fc[-nx+1] - 3.0*fc[1] + 3.0*fc[nx+1]);
      return
	0.25*( 
	       (fc[-nx-1]*ux[0] + fc[-nx  ]*ux[1] + 
		fc[-nx+1]*ux[2] + (3.0*fc[-nx+1] - 3.0*fc[-nx] + fc[-nx-1])*ux[3]) * uy[0] + 
	       (fc[-1]*ux[0] + fc[0]*ux[1] +
		fc[1]*ux[2] + (3.0*fc[1] - 3.0*fc[0] + fc[-1])*ux[3]) * uy[1] + 
	       (fc[nx-1]*ux[0] + fc[nx]*ux[1] + 
		fc[nx+1]*ux[2] + (3.0*fc[nx+1] - 3.0*fc[nx] + fc[nx-1])*ux[3]) * uy[2] + 
	       (t1*ux[0] + t2*ux[1] + 
		t3*ux[2] + (3.0*t3 - 3.0*t2 + t1)*ux[3])*uy[3]
	       );
    }
    else
    {
      return
	0.25*( 
	       (fc[-nx-1]*ux[0] + fc[-nx]*ux[1] + 
		fc[-nx+1]*ux[2] + (3.0*fc[-nx+1] - 3.0*fc[-nx] + fc[-nx-1])*ux[3]) * uy[0] +
	       (fc[-1]*ux[0] + fc[0 ]*ux[1] + 
		fc[1 ]*ux[2] + (3.0*fc[1] - 3.0*fc[0] + fc[-1])*ux[3]) * uy[1] + 
	       (fc[nx-1]*ux[0] + fc[nx  ]*ux[1] + 
		fc[nx+1]*ux[2] + (3.0*fc[nx+1] - 3.0*fc[nx] + fc[nx-1])*ux[3]) * uy[2] + 
	       (fc[2*nx-1]*ux[0] + fc[2*nx  ]*ux[1] + 
		fc[2*nx+1]*ux[2] + (3.0*fc[2*nx+1]-3.0*fc[2*nx] + fc[2*nx-1])*ux[3]) * uy[3]
	       );
    }
  }
  else
  {
    if ( unlikely(iy == 0) )
    {
      return	    
	0.25*( 
	       ((3.0*fc[-1] - 3.0*fc[nx-1] + fc[2*nx-1])*ux[0] + 
		(3.0*fc[0] - 3.0*fc[nx] + fc[2*nx])*ux[1] + 
		(3.0*fc[1] - 3.0*fc[nx+1] + fc[2*nx+1])*ux[2] + 
		(3.0*fc[2] - 3.0*fc[nx+2] + fc[2*nx+2])*ux[3] ) * uy[0] +
	       (fc[-1]*ux[0] + fc[0 ]*ux[1] + 
		fc[1 ]*ux[2] + fc[2 ]*ux[3]) * uy[1] + 
	       (fc[nx-1]*ux[0] + fc[nx  ]*ux[1] + 
		fc[nx+1]*ux[2] + fc[nx+2]*ux[3]) * uy[2] + 
	       (fc[2*nx-1]*ux[0] + fc[2*nx  ]*ux[1] + 
		fc[2*nx+1]*ux[2] + fc[2*nx+2]*ux[3]) * uy[3] 
	       );
    }
    else if ( unlikely(iy >= ny-2) )
    {
      if (iy == ny-1)
      {
	fc = &fc[-nx];
	uy[0] = 0.0;
	uy[1] = 0.0;
	uy[2] = 2.0;
	uy[3] = 0.0;
      }
      return
	0.25*( 
	       (fc[-nx-1]*ux[0] + fc[-nx  ]*ux[1] + 
		fc[-nx+1]*ux[2] + fc[-nx+2]*ux[3]) * uy[0] + 
	       (fc[-1]*ux[0] + fc[0 ]*ux[1] + 
		fc[1 ]*ux[2] + fc[2 ]*ux[3]) * uy[1] + 
	       (fc[nx-1]*ux[0] + fc[nx  ]*ux[1] + 
		fc[nx+1]*ux[2] + fc[nx+2]*ux[3]) * uy[2] + 
	       ((fc[-nx-1] - 3.0*fc[-1] + 3.0*fc[nx-1])*ux[0] + 
		(fc[-nx] - 3.0*fc[0] + 3.0*fc[nx])*ux[1] + 
		(fc[-nx+1] - 3.0*fc[1] + 3.0*fc[nx+1])*ux[2] + 
		(fc[-nx+2] - 3.0*fc[2] + 3.0*fc[nx+2])*ux[3])*uy[3]
	       );
    }
    else
    {
      return
	0.25*( 
	       (fc[ -nx-1]*ux[0] + fc[ -nx  ]*ux[1] +
		fc[ -nx+1]*ux[2] + fc[ -nx+2]*ux[3])*uy[0] +
	       (fc[    -1]*ux[0] + fc[     0]*ux[1]+
		fc[     1]*ux[2] + fc[     2]*ux[3])*uy[1] +
	       (fc[  nx-1]*ux[0] + fc[  nx  ]*ux[1] +
		fc[  nx+1]*ux[2] + fc[  nx+2]*ux[3])*uy[2] +
	       (fc[2*nx-1]*ux[0] + fc[2*nx  ]*ux[1] +
		fc[2*nx+1]*ux[2] + fc[2*nx+2]*ux[3])*uy[3]
	       );
    }
  }
}




/* Optimized 6x6 convolution routines. This version returns zero
   if the target location is withing 2 pixels from the boundary.
*/
static inline double dc4convolve(int ix, int iy, int nx, int ny, double *f, 
				 double *ux, double *uy, double fillvalue)
{
  double *fc;

  if ( unlikely(ix < 2 || ix >= (nx-3) ||
		iy < 2 || iy >= (ny-3)) )
    return fillvalue;
  else
  {	  
    fc = &f[iy*nx + ix];
    return
      (1.0/144.0)*( 
		   (fc[-2*nx-2]*ux[0] + fc[-2*nx-1]*ux[1] +
		    fc[-2*nx  ]*ux[2] + fc[-2*nx+1]*ux[3] +
		    fc[-2*nx+2]*ux[4] + fc[-2*nx+3]*ux[5]) * uy[0] +
		   (fc[  -nx-2]*ux[0] + fc[  -nx-1]*ux[1] +
		    fc[  -nx  ]*ux[2] + fc[  -nx+1]*ux[3] +
		    fc[  -nx+2]*ux[4] + fc[  -nx+3]*ux[5]) * uy[1] +
		   (fc[     -2]*ux[0] + fc[     -1]*ux[1] +
		    fc[      0]*ux[2] + fc[      1]*ux[3] +
		    fc[      2]*ux[4] + fc[      3]*ux[5]) * uy[2] +
		   (fc[   nx-2]*ux[0] + fc[   nx-1]*ux[1] +
		    fc[   nx  ]*ux[2] + fc[   nx+1]*ux[3] +
		    fc[   nx+2]*ux[4] + fc[   nx+3]*ux[5]) * uy[3] +
		   (fc[ 2*nx-2]*ux[0] + fc[ 2*nx-1]*ux[1] +
		    fc[ 2*nx  ]*ux[2] + fc[ 2*nx+1]*ux[3] +
		    fc[ 2*nx+2]*ux[4] + fc[ 2*nx+3]*ux[5]) * uy[4] +
		   (fc[ 3*nx-2]*ux[0] + fc[ 3*nx-1]*ux[1] +
		    fc[ 3*nx  ]*ux[2] + fc[ 3*nx+1]*ux[3] +
		    fc[ 3*nx+2]*ux[4] + fc[ 3*nx+3]*ux[5]) * uy[5]
		   );
  }
}


static inline float fc4convolve(int ix, int iy, int nx, int ny, float *f, 
				 float *ux, float *uy, float fillvalue)
{
  float *fc;

  if ( unlikely(ix < 2 || ix >= (nx-3) ||
		iy < 2 || iy >= (ny-3)) )
    return fillvalue;
  else
  {	  
    fc = &f[iy*nx + ix];
    return
      (1.0f/144.0f)*( 
		   (fc[-2*nx-2]*ux[0] + fc[-2*nx-1]*ux[1] +
		    fc[-2*nx  ]*ux[2] + fc[-2*nx+1]*ux[3] +
		    fc[-2*nx+2]*ux[4] + fc[-2*nx+3]*ux[5]) * uy[0] +
		   (fc[  -nx-2]*ux[0] + fc[  -nx-1]*ux[1] +
		    fc[  -nx  ]*ux[2] + fc[  -nx+1]*ux[3] +
		    fc[  -nx+2]*ux[4] + fc[  -nx+3]*ux[5]) * uy[1] +
		   (fc[     -2]*ux[0] + fc[     -1]*ux[1] +
		    fc[      0]*ux[2] + fc[      1]*ux[3] +
		    fc[      2]*ux[4] + fc[      3]*ux[5]) * uy[2] +
		   (fc[   nx-2]*ux[0] + fc[   nx-1]*ux[1] +
		    fc[   nx  ]*ux[2] + fc[   nx+1]*ux[3] +
		    fc[   nx+2]*ux[4] + fc[   nx+3]*ux[5]) * uy[3] +
		   (fc[ 2*nx-2]*ux[0] + fc[ 2*nx-1]*ux[1] +
		    fc[ 2*nx  ]*ux[2] + fc[ 2*nx+1]*ux[3] +
		    fc[ 2*nx+2]*ux[4] + fc[ 2*nx+3]*ux[5]) * uy[4] +
		   (fc[ 3*nx-2]*ux[0] + fc[ 3*nx-1]*ux[1] +
		    fc[ 3*nx  ]*ux[2] + fc[ 3*nx+1]*ux[3] +
		    fc[ 3*nx+2]*ux[4] + fc[ 3*nx+3]*ux[5]) * uy[5]
		   );
  }
}

/* ==== end of optimized interpolation kernel and convolution routines ==== */


