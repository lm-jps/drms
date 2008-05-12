#include "list.h"
#include "jsoc.h"

LinkedList_t *list_llcreate(unsigned int datasize)
{
   LinkedList_t *list = calloc(1, sizeof(LinkedList_t));
   
   if (list)
   {
      list->dsize = datasize;
   }

   return list;
}

LinkedList_t *list_llinsert(LinkedList_t *llist, void *data)
{
   LinkedList_t *ret = NULL;

   if (llist && data)
   {
      ListNode_t *node = calloc(1, sizeof(ListNode_t));

      if (node)
      {
	 node->data = malloc(llist->dsize);
	 if (node->data)
	 {
	    memcpy(node->data, data, llist->dsize);
	 }

	 if (!llist->first)
	 {
	    llist->first = node;
	 }
	 else
	 {
	    node->next = llist->first;
	    llist->first = node;
	 }
      }
   }

   return ret;
}

ListNode_t *list_llnext(LinkedList_t *llist)
{
   ListNode_t *next = NULL;

   if (llist)
   {
      if (llist->next == NULL)
      {
	 if (llist->first != NULL)
	 {
	    next = llist->first;
	 }
      }
      else
      {
	 next = llist->next;
      }

      if (llist->next != NULL && llist->next->next != NULL)
      {
	 llist->next = llist->next->next;
      }
      else
      {
	 llist->next = NULL;
      }
   }

   return next;
}

void list_llfree(LinkedList_t **llist)
{
   if (llist && *llist)
   {
      ListNode_t *pElem = (*llist)->first;
      ListNode_t *nElem = NULL;

      while (pElem)
      {      
	 nElem = pElem->next;

	 /* need to free malloc'd mem */
	 if (pElem->data)
	 {
	    free(pElem->data);
	 }
	 free(pElem);
	 pElem = nElem;
      }

      *llist = NULL;
   }
}
