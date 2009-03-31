#include "timer.h"
#include <stdio.h>
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

/* There are a lot of shortcomings of the original functions.
 * They rely upon globals that can't be shared very well by 
 * different blocks of code. They don't work with threads at all.
 * Also, with the original functions, you must not
 * call PPopTimer(). You can't time a function that gets 
 * repeatedly called, saving the timer between calls.
 * Also, the original functions only work if you always call PushTimer/PopTimer every time
 * you want to time a block of code. But typically, this is not desired.
 * What you want to do is to start a timer, then check on the elapsed time
 * at many times and locations after starting the timer - you don't want to
 * continually create and destroy the timer.
 */
TIMER_t *CreateTimer()
{
   TIMER_t *timer = NULL;

   timer = malloc(sizeof(TIMER_t));

   if (timer)
   {
      gettimeofday(&(timer->first), NULL);
   }

   return timer;
}

float GetElapsedTime(TIMER_t *timer)
{
   float seconds = -1;

   if (timer)
   {
      gettimeofday(&(timer->second), NULL);
      
      if (timer->first.tv_usec > timer->second.tv_usec) 
      {  
         timer->second.tv_usec += 1000000;
         timer->second.tv_sec--;
      }

      seconds = (float)(timer->second.tv_sec - timer->first.tv_sec) +  
        (float)(timer->second.tv_usec - timer->first.tv_usec) / 1000000.0;
   }

   return seconds;
}

void DestroyTimer(TIMER_t **timer)
{
   if (timer && *timer)
   {
      free(*timer);
      *timer = NULL;
   }
}
