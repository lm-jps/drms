#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include "tdsignals.h"

struct td_sthreadarg_struct
{
  unsigned int seconds;
  void (*shandler)(int, pthread_mutex_t *);
  pthread_mutex_t *mutex;
};

typedef struct td_sthreadarg_struct td_sthreadarg_t;

static td_sthreadarg_t gStdarg;

static void *td_sigthreadfxn(void *arg)
{
   sigset_t bsigs;
   sigset_t rsigs;
   int siggot;
   int err = 0;
   td_sthreadarg_t *stdarg = (td_sthreadarg_t *)arg;
   unsigned int seconds = stdarg->seconds;
   void (*shandler)(int, pthread_mutex_t*) = stdarg->shandler;
   pthread_mutex_t *mutex = stdarg->mutex;

   /* block all signals */
   sigfillset(&bsigs);
   pthread_sigmask(SIG_BLOCK, &bsigs, NULL);

   /* create a set of signals that includes only SIGALRM */
   sigemptyset(&rsigs);
   sigaddset(&rsigs, SIGALRM);

   /* Block here if another signal thread is processing an alarm.  This ensures
    * that the callback functions provided as td_alarm() arguments get executed 
    * in the order in which their encompassing td_alarm() calls were called. */
   pthread_mutex_lock(mutex);
   /* Will block until SIGALRM is received. */
   err = sigwait(&rsigs, &siggot);
   pthread_mutex_unlock(mutex);

   if (!err)
   {
      /* sleep for <seconds> seconds */
      sleep(seconds);

      /* Call function callback, with SIGALRM argument */
      (*shandler)(SIGALRM, mutex);
   }
   else
   {
      fprintf(stderr, "sigwait() error in thread %lld.\n", (long long)pthread_self());      
   }

   return arg;
}

/* td_alarm runs is the "main" thread. 
 *
 * seconds - minimum number of seconds before shandler is called
 * shandler - function callback that is called BY THE SIGNAL THREAD once <seconds> seconds 
 *     have elapsed.
 * returns 0 if success, 1 if failure
 */
int td_createalarm(unsigned int seconds, 
                   void (*shandler)(int, pthread_mutex_t *), 
                   pthread_mutex_t *mutex, 
                   pthread_t *alrmtd)
{
   int status = 0;
   int ret = 0;
   pthread_t sigthread = 0;

   /* Ensure that the main thread doesn't handle the SIGALRM signal */
   sigset_t rsigs;
   sigemptyset(&rsigs);
   sigaddset(&rsigs, SIGALRM); 
   pthread_sigmask(SIG_BLOCK, &rsigs, NULL);

   /* Create a thread to handle the SIGALRM signal. */
   gStdarg.seconds = seconds;
   gStdarg.shandler = shandler;
   gStdarg.mutex = mutex;
   if((status = pthread_create(&sigthread, NULL, &td_sigthreadfxn, (void *)&gStdarg)))
   {
      fprintf(stderr, "Thread creation failed: %d\n", status);          
      ret = 1;
   }
   else
   {
      /* now send SIGALRM signal - signal thread will catch the signal, then sleep for 
       * <seconds> seconds, then it will call the main thread callback. */
      pthread_kill(sigthread, SIGALRM);
   }

   if (!ret && alrmtd)
   {
      *alrmtd = sigthread;
   }

   return ret;
}

/* Must call to before freeing mutex.  If you call pthread_mutex_destory() before all threads
 * have exited, then you might accidentally destroy the mutex and then a thread tries to 
 * use the mutex. */
void td_destroyalarm(td_alarm_t *alarm)
{
   if (alarm)
   {
      /* Wait until signal thread has terminated. */
      pthread_join(*((pthread_t *)alarm), NULL);
      *alarm = 0;
   }
}
