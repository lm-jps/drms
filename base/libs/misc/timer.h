#ifndef __TIMER_H_DEF
#define __TIMER_H_DEF

#include <sys/time.h>

#define TIME(code) { PushTimer(); code; printf("Time spent executing '"#code"' = %f\n",PopTimer()); } 

void StartTimer(int n);
float StopTimer(int n);
void PushTimer(void);
float PopTimer(void);

struct TIMER_struct
{
  struct timeval first;
  struct timeval second;
};

typedef struct TIMER_struct TIMER_t;

TIMER_t *CreateTimer();
float GetElapsedTime(TIMER_t *timer);
void ResetTimer(TIMER_t *timer);
void DestroyTimer(TIMER_t **timer);

#endif
