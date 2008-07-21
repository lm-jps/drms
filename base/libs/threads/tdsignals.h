#ifndef _TDSIGNALS_H
#define _TDSIGNALS_H

#include <pthread.h>

typedef pthread_t td_alarm_t;

int td_createalarm(unsigned int seconds, 
                   void (*shandler)(int, pthread_mutex_t *), 
                   pthread_mutex_t *mutex, 
                   pthread_t *alrmtd);
void td_destroyalarm(td_alarm_t *alarm, pthread_mutex_t *mutex);

#endif /* _TDSIGNALS_H */
