/* padata.c
 *
 * PADATA is a linked list storage structure for each DSDS storage directory
 * assigned to all PEs. (see dsds.h)
 *
 * The following functions are defined for use with padata lists:
 * NOTE: long replaced with uint64_t
 * void setpadata (PADATA **list, char *wd, long uid, double bytes,
 *	int stat, int substat, char *eff_date, int group_id, int safe_id,
 *              unsigned long ds_index)
 *	Add an entry with the given values. Don't allow dup wd and uid.
 * void uidpadata(PADATA *pkt, PADATA **start, PADATA **end)
 *      Add the packet to the list in ascending uid order.
 * void remuidpadata(PADATA **start, PADATA **end, char *wd, long uid)
 *      remove the given entry from the ascending uid order list.
 * PADATA *getpadata (PADATA *list, char *wd, long uid)
 *	return pointer to entry containing wd and uid. (can only be one)
 * PADATA *getpawd (PADATA *list, char *wd)
 *	return pointer to first entry containing wd, if list = -1 gets next
 *	entry. Note: Do not interleave calls for two different lists.
 * PADATA *getpauid (PADATA *list, long uid)
 *	return pointer to first entry containing uid, if list = -1 gets next
 *	entry. Note: Do not interleave calls for two different lists.
 * PADATA *getpanext (PADATA *list)
 *	get first entry on list, if list = -1 gets next entry
 *	Note: Do not interleave calls for two different lists
 * void rempadata (PADATA **list, char *wd, long uid)
 *	remove the entry containing wd and uid. (can only be one)
 *	updates all the "next entry" pointers if they are the removed entry
 * void updpadata (PADATA **list, char *wd, long uid, char *eff_date)
 *      updates the effective date of the given entry (can only be one)
 *
 * PADATA *makepadata(char *wd, long uid, double bytes, int stat, int substat, 
 *			char *eff_date, int group_id, int safe_id, unsigned long ds_index)
 *	Used internally to construct a new paket with a NULL next pointer.
*/

#include <stdlib.h>
#include <stdio.h>
#include <SUM.h>
#include <sum_rpc.h>

#define MONE (long)-1	/* needs to be long for IRIX64 compile */

			/* the "next entry" pointers */
PADATA *panext;		/* used between getpanext() calls */
PADATA *panextwd;	/* used between getpawd() calls */
PADATA *panextuid;	/* used between getpauid() calls */

char *p_strdup (char *s)
{
  char *duped;

  if(!s) return NULL;
  duped = (char *)malloc(strlen(s)+1);
  strcpy(duped, s);
  return duped;
}


PADATA *getpadata(PADATA *list, char *wd, uint64_t sumid)
{
  PADATA *walk = list;

  while(walk) {
    if(walk->sumid != sumid)
      walk = walk->next;
    else {
      if(!(strcmp(walk->wd, wd)))
        return walk;
      else
        walk = walk->next;
    }
  }
  return walk;
}


PADATA *makepadata(char *wd, uint64_t sumid, double bytes, int stat, int substat, char *eff_date, int group_id, int safe_id, uint64_t ds_index)
{
  PADATA *newone;

  newone = (PADATA *)malloc(sizeof(PADATA));
  newone->next = NULL;
  newone->wd = p_strdup(wd);
  newone->sumid = sumid;
  newone->bytes = bytes;
  newone->status = stat;
  newone->archsub = substat;
  newone->effective_date = p_strdup(eff_date);
  newone->group_id = group_id;
  newone->safe_id = safe_id;
  newone->ds_index = ds_index;
  return(newone);
}


void setpadata(PADATA **list, char *wd, uint64_t sumid, double bytes, int stat, int substat, char *eff_date, int group_id, int safe_id, uint64_t ds_index)
{
  PADATA *newone;

  if(getpadata(*list, wd, sumid)) return;
  newone = makepadata(wd, sumid, bytes, stat, substat, eff_date, group_id, safe_id, ds_index);
  newone->next = *list;
  *list = newone;
}

/* Like setpadata() but that makes the list in reverse order of call (last
 * PADATA *newone is at top of linked list). This makes it in the "normal"
 * order so that the last PADATA *newone added is at the end of the linked list.
 * This is used by tapearcX so that the sunums in a tape file end up in
 * ascending sunum order.  NOTE: first call must have list = NULL.
*/
void setpadatar(PADATA **list, char *wd, uint64_t sumid, double bytes, int stat, int substat, char *eff_date, int group_id, int safe_id, uint64_t ds_index)
{
  PADATA *newone;
  PADATA *walk;

  if(getpadata(*list, wd, sumid)) return;
  newone = makepadata(wd, sumid, bytes, stat, substat, eff_date, group_id, safe_id, ds_index);
  if(*list == NULL) {
    *list = newone;
    newone->next = NULL;
  }
  else {
    walk = *list;
    while(walk->next) {
      walk = walk->next;
    }
    walk->next = newone;
  }
}


/* Add a duplicate of the given PADATA packet to the linked list in 
 * ascending sumid order. (Must make a dup as the packet may already be
 * on some other list.)
 * Must be called first with start=end=NULL. Start and end are updated 
 * on each call.
 *   new = packet to dup and add to list
 *   start = head of list to add to
 *   end = tail of list to add to
*/
void uidpadata(PADATA *pkt, PADATA **start, PADATA **end)
{
  PADATA *old, *p, *addnew;

  addnew = makepadata(pkt->wd, pkt->sumid, pkt->bytes, pkt->status, pkt->archsub,
			pkt->effective_date, pkt->group_id, pkt->safe_id, pkt->ds_index);
  p = *start;
  if(!*end)	{ /* first element in list */
    addnew->next = NULL;
    *end = addnew;
    *start = addnew;
    return;
  }
  old = NULL;
  while(p) {
    if(p->sumid < addnew->sumid) {
      old = p;
      p = p->next;
    }
    else {
      if(old) {			/* goes in middle */
        old->next = addnew;
        addnew->next = p;
        return;
      }
      addnew->next = p;		/* new first element */
      *start = addnew;
      return;
    }
  }
  (*end)->next = addnew;	/* put on end */
  addnew->next = NULL;
  *end = addnew;
}

/* Remove an entry from the list built by uidpadata() which has the
 * sumid in ascending order and has a head and tail list pointer.
 * Removes the enty that matches the given wd an sumid, frees the
 * storage for the entry, and updates the head and tail pointers.
*/
void remuidpadata(PADATA **start, PADATA **end, char *wd, uint64_t sumid)
{
  PADATA *walk = *start;
  PADATA *trail = NULL;

  while(walk) {
    if(walk->sumid != sumid) {
      trail = walk;
      walk = walk->next;
    }
    else {
      if(!(strcmp(walk->wd, wd))) {
        if(trail) {
          trail->next = walk->next;
          if(trail->next == NULL) *end = trail;
        }
        else {
          *start = walk->next;
          if(*start == NULL) *end = NULL;
        }
        free(walk->wd);
        free(walk->effective_date);
        free(walk);
        walk = NULL;
      }
      else {
        trail = walk;
        walk = walk->next;
      }
    }
  }
}


/* get first entry containing wd, if list = -1 gets the next entry.
 * Note: Do not interleave calls for two different lists.
*/
PADATA *getpawd(PADATA *list, char *wd)
{
  PADATA *walk;

  if(list != (PADATA *)MONE) 
    panextwd = list;
  walk = panextwd;
  while(walk) {
    if(!(strcmp(walk->wd, wd))) {
      panextwd = walk->next;
      return walk;
    }
    else
      walk = walk->next;
  }
  return walk;
}

/* get first entry containing sumid, if list = -1 gets the next entry.
 * Note: Do not interleave calls for two different lists.
*/
PADATA *getpauid(PADATA *list, uint64_t sumid)
{
  PADATA *walk;

  if(list != (PADATA *)MONE)
    panextuid = list;
  walk = panextuid;
  while(walk) {
    if(walk->sumid != sumid)
      walk = walk->next;
    else {
      panextuid = walk->next;
      return walk;
    }
  }
  return walk;
}

/* get first entry on list, if list = -1 gets the next entry.
 * Note: Do not interleave calls for two different lists.
*/
PADATA *getpanext(PADATA *list)
{
  PADATA *walk;

  if(list != (PADATA *)MONE) {
    panext = list;
  }
  if(walk=panext)
    panext = panext->next;
  return walk;
}

void rempadata(PADATA **list, char *wd, uint64_t sumid)
{
  PADATA *walk = *list;
  PADATA *trail = NULL;

  while(walk) {
    if(walk->sumid != sumid) {
      trail = walk;
      walk = walk->next;
    }
    else {
      if(!(strcmp(walk->wd, wd))) {
        if(trail) 
          trail->next = walk->next;
        else
          *list = walk->next;
        /* now update any "next" pointer that is at this entry */
        if(panext == walk)
          panext = walk->next;
        if(panextwd == walk)
          panextwd = walk->next;
        if(panextuid == walk)
          panextuid = walk->next;
        free(walk->wd);
        free(walk->effective_date);
        free(walk);
        walk = NULL;
      }
      else {
        trail = walk;
        walk = walk->next;
      }
    }
  }
}

/* Update the effective date of the given entry */
void updpadata (PADATA **list, char *wd, uint64_t sumid, char *eff_date)
{
  PADATA *walk = *list;

  while(walk) {
    if(walk->sumid != sumid) {
      walk = walk->next;
    }
    else {
      if(!(strcmp(walk->wd, wd))) {
        free(walk->effective_date);
        walk->effective_date = p_strdup(eff_date);
      }
      walk = walk->next;
    }
  }
}
