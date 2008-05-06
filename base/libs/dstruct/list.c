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
      }
   }

   return ret;
}
