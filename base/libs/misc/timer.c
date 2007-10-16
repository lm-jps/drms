#include "timer.h"
#include <stdio.h>
#include <sys/time.h>
#define N 100
#include "xmem.h"

int maxtimer=0;
static struct timeval first[N],second[N];

//static struct timezone tzp[N];
static int nexttimer = 0;


/* FORTRAN Interface routines: */
void starttimer_(int *n)
{
  StartTimer(*n);
}

float stoptimer_(int *n)
{
  return StopTimer(*n);
}

void StartTimer(int n)
{
  if (n<1 || n > N) 
    fprintf(stderr,"StartTimer: Timer number should be between 1 and %d\n",N);
  else {
    n = n-1; 
    gettimeofday (&first[n], NULL);
  }
}

float StopTimer(int n)
{
  if (n<1 || n > N) 
    fprintf(stderr,"StopTimer: Timer number should be between 1 and %d\n",N);
  else {
    n = n-1; 
    gettimeofday (&second[n], NULL);
    if (first[n].tv_usec > second[n].tv_usec) {  
      second[n].tv_usec += 1000000;
      second[n].tv_sec--;
    }
  }
  return (float) (second[n].tv_sec-first[n].tv_sec) +  
    (float) (second[n].tv_usec-first[n].tv_usec)/1000000.0;
}


/* Wall clock timer routines */
void PushTimer(void)
{
  StartTimer(++nexttimer);
}

float PopTimer(void)
{
  return StopTimer(nexttimer--);
}
