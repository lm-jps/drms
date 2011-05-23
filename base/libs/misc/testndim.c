#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "timer.h"
#include "ndim.h"
#include "xassert.h"


#define N1 3
#define N2 2
#define N3 5
#define N4 3

int x[N1][N2][N3][N4];
int dims[4]={N1,N2,N3,N4};
int start[4]={0,0,0,0}, end[4]={N1-1,N2-1,N3-1,N4-1};
int start1[4]={0,0,0,0}, end1[4]={N1-1,N2-1,N3-1,N4-1};

void print(int x[N1][N2][N3][N4])
{
  int i0,i1,i2,i3;

  for (i3=0; i3<N1; i3++)
  {
    for (i2=0; i2<N2; i2++)
    {
      printf("x[%d][%d][*][*] = \n",i3,i2);
      for (i1=0; i1<N3; i1++)
      {
	for (i0=0; i0<N4; i0++)
	  printf("%04d ",x[i3][i2][i1][i0]);
	printf("\n");
      }
      printf("\n");
    }
    printf("\n");
  }
  printf("\n");
}

int main()
{
  int N;
  int i,i0,i1,i2,i3, total, val;
  int *y;
  int perm[4] = {1,0,2,3};

  N =  N1*N2*N3*N4;
  StartTimer(1);
  for (i3=0; i3<N1; i3++)
    for (i2=0; i2<N2; i2++)
      for (i1=0; i1<N3; i1++)
	for (i0=0; i0<N4; i0++)
	  x[i3][i2][i1][i0] = i0 + 10*i1 + 100*i2 + 1000*i3;
  printf("time spent generating data = %f\n",StopTimer(1));
  y = malloc(N*sizeof(int));
  XASSERT(y);
  memset(y, 0, N*sizeof(int));
  StartTimer(1);
  ndim_unpack(sizeof(int), 4, dims, start, end, 
	      (unsigned char *) x, (unsigned char *) y);
  printf("time spent unpacking data = %f\n",StopTimer(1));

  printf("y = \n");
  if (N<1000)
  {
    total = end[0]-start[0]+1;
    for(i=1; i<4; i++)
      total *= (end[i]-start[i]+1); 
    for(i=0; i<total; i++)
      printf("%04d\n",y[i]);
  }

  
  StartTimer(1);
  memset(x, 0, N*sizeof(int));
  ndim_pack(sizeof(int), 4, dims, start1, end1, 
	      (unsigned char *) y, (unsigned char *) x);
  printf("time spent packing data = %f\n",StopTimer(1));

  if (N<1000)
    print(x);

  
  memset(y, 0, N*sizeof(int));
  ndim_permute(sizeof(int), 4, dims, perm,
	    (unsigned char *) x, (unsigned char *) y);

  if (N<1000)
    print(y);
  return 0;
}
