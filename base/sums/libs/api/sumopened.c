/* sumopened.c
 *
 * SUMOPENED is a linked list storage structure for all the opens with
 * sum_svc that have been done by a single client.
 *
 * The following functions are defined for use with SUMOPENED lists:
 * void setsumopened (SUMOPENED **list, SUMID_t uid, SUM *sum)
 *	Add an entry with the given values.
 * SUMOPENED *getsumopened (SUMOPENED *list, SUMID_t uid)
 *	return pointer to entry containing the uid. (can only be one)
 * void remsumopened (SUMOPENED **list, SUMID_t uid)
 *	remove the entry containing the uid. (can only be one)
*/

#include <SUM.h>
#include <sum_rpc.h>

void setsumopened(SUMOPENED **list, SUMID_t uid, SUM_t *sum, char *user)
{
  SUMOPENED *newone;

  newone = (SUMOPENED *)malloc(sizeof(SUMOPENED));
  newone->next = *list;
  newone->uid = uid;
  newone->sum = sum;
  strcpy(newone->user, user);
  *list = newone;
}

SUMOPENED *getsumopened(SUMOPENED *list, SUMID_t uid)
{
  SUMOPENED *walk = list;

  while(walk) {
    if(walk->uid != uid)
      walk = walk->next;
    else
      return walk;
  }
  return walk;
}

void remsumopened(SUMOPENED **list, SUMID_t uid)
{
  SUMOPENED *walk = *list;
  SUMOPENED *trail = NULL;

  while(walk) {
    if(walk->uid != uid) {
      trail = walk;
      walk = walk->next;
    }
    else {
      if(trail) 
        trail->next = walk->next;
      else 
        *list = walk->next;
      free(walk);
      walk = NULL;
    }
  }
}
