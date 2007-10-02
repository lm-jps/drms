#ifndef __TIMER_H_DEF
#define __TIMER_H_DEF

#define TIME(code) { PushTimer(); code; printf("Time spent executing '"#code"' = %f\n",PopTimer()); } 

void StartTimer(int n);
float StopTimer(int n);
void PushTimer(void);
float PopTimer(void);

#endif
