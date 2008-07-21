#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include "tdsignals.h"
#include "dsqueue.h"

struct td_sthreadarg_struct
{
  unsigned int seconds;
  void (*shandler)(int, pthread_mutex_t *);
  pthread_mutex_t *mutex;
};

typedef struct td_sthreadarg_struct td_sthreadarg_t;

static td_sthreadarg_t gStdarg;
static Queue_t *queue = NULL;
static int gArgsConsumed = 0;

static void *td_sigthreadfxn(void *arg)
{
   sigset_t bsigs;
   sigset_t rsigs;
   int siggot;
   int err = 0;
   td_sthreadarg_t *stdarg = NULL;
   unsigned int seconds = 0;
   void (*shandler)(int, pthread_mutex_t*) = NULL;
   pthread_mutex_t *mutex = NULL;
   pthread_t fronttd = 0;
   pthread_t selftd = 0;
   QueueNode_t *top = NULL;
   QueueNode_t *thistd = NULL;

   selftd = pthread_self();

   /* Block here if another signal thread is processing an alarm.  Only one 
    * thread at a time can wait for a given signal with sigwait(). */
   stdarg = (td_sthreadarg_t *)arg;
   seconds = stdarg->seconds;
   shandler = stdarg->shandler;
   mutex = stdarg->mutex;

   /* Pushd signal thread onto fifo queue.  These will be processed in the
    * order in which they were received.*/
   pthread_mutex_lock(mutex);
   queue_queue(queue, (void *)(&selftd));
   pthread_mutex_unlock(mutex);

   gArgsConsumed = 1;

   /* block all signals */
   sigfillset(&bsigs);
   pthread_sigmask(SIG_BLOCK, &bsigs, NULL);

   /* wait until it is this thread's turn. */
   while (1)
   {
      pthread_mutex_lock(mutex);
      thistd = queue_find(queue, &selftd);
      
      if (!thistd)
      {
         /* this alarm was destroyed - return */
         pthread_mutex_unlock(mutex);
         return arg;
      }

      top = queue_front(queue);

      if (!top)
      {
         pthread_mutex_unlock(mutex);
         sched_yield();
         continue;
      }

      fronttd = *((pthread_t *)(top->data));
      if (fronttd != selftd)
      {
         pthread_mutex_unlock(mutex);
         // sleep(1);
         sched_yield();
      }
      else
      {
         pthread_mutex_unlock(mutex);
         break;
      }
   }

   /* create a set of signals that includes only SIGALRM */
   sigemptyset(&rsigs);
   sigaddset(&rsigs, SIGALRM);

   /* Will block until SIGALRM is received. */
   err = sigwait(&rsigs, &siggot);

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

   /* Create queue. */
   if (!queue)
   {
      queue = queue_create(sizeof(pthread_t));
   }

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
   
   while (1)
   {
      pthread_mutex_lock(mutex);
      if (gArgsConsumed)
      {
         gArgsConsumed = 0;
         pthread_mutex_unlock(mutex);
         sched_yield();
         break;
      }
      else
      {
         pthread_mutex_unlock(mutex);   
      }
   }

   if (!ret && alrmtd)
   {
      pthread_mutex_lock(mutex);
      *alrmtd = sigthread;
      pthread_mutex_unlock(mutex);
   }

   return ret;
}

/* Must call to before freeing mutex.  If you call pthread_mutex_destory() before all threads
 * have exited, then you might accidentally destroy the mutex and then a thread tries to 
 * use the mutex. */
void td_destroyalarm(td_alarm_t *alarm, pthread_mutex_t *mutex)
{
   int err = 0;

   if (alarm && *alarm)
   {
      pthread_mutex_lock(mutex);
      QueueNode_t *node = queue_remove(queue, alarm);

      if (node)
      {
         queue_freenode(&node);
      }
      else
      {
         err = 1;
      }

      pthread_mutex_unlock(mutex);

      if (err)
      {
         fprintf(stderr, "Invalid alarm '%lld'\n.", (long long)*alarm);
      }
      else
      {
         /* Wait until signal thread has terminated. */
         pthread_join(*((pthread_t *)alarm), NULL);
         *alarm = 0;
      }

      pthread_mutex_lock(mutex);

      if (queue_empty(queue))
      {
         queue_destroy(&queue);
      }

      pthread_mutex_unlock(mutex);
   }
}
