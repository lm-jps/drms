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

ListNode_t *list_llinserthead(LinkedList_t *llist, void *data)
{
   ListNode_t *node = NULL;

   if (llist && data)
   {
      node = calloc(1, sizeof(ListNode_t));

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

   return node;
}

ListNode_t *list_llinserttail(LinkedList_t *llist, void *data)
{
   ListNode_t *node = NULL;

   if (llist && data)
   {
      node = calloc(1, sizeof(ListNode_t));

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
            ListNode_t *end = llist->first;
	    node->next = NULL;

            while (end)
            {
               if (end->next == NULL)
               {
                  end->next = node;
                  break;
               }

               end = end->next;
            }
	 }
      }
   }

   return node;
}

void list_llremove(LinkedList_t *llist, ListNode_t *item)
{
   ListNode_t *node = llist->first;
   ListNode_t *prev = NULL;

   while (node)
   {
      if (node == item)
      {
         break;
      }

      prev = node;
      node = node->next;
   }

   if (node)
   {
      if (prev == NULL)
      {
         /* item was the first node */
         llist->first = node->next;
      }
      else
      {
         prev->next = node->next;
      }
   }
}

void list_llreset(LinkedList_t *llist)
{
   llist->next = llist->first;
}

ListNode_t *list_llnext(LinkedList_t *llist)
{
   ListNode_t *next = NULL;

   if (llist)
   {
      next = llist->next;

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

ListNode_t *list_llfind(LinkedList_t *llist, void *data)
{
   ListNode_t *node = NULL;

   if (llist && data)
   {
      ListNode_t *iter = llist->first;
      while (iter)
      {
         if (memcmp(iter->data, data, llist->dsize) == 0)
         {
            node = iter;
         }

         iter = iter->next;
      }
   }

   return node;
}

ListNode_t *list_llgettail(LinkedList_t *llist)
{
   ListNode_t *node = NULL;

   if (llist)
   {
      node = llist->first;

      while (node)
      {
         if (node->next = NULL)
         {
            break;
         }

         node = node->next;
      }
   }

   return node;
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

void list_llfreenode(ListNode_t **node)
{
   if (node && *node)
   {
      if ((*node)->data)
      {
         free((*node)->data);
         (*node)->data = NULL;
      }
      free(*node);
      *node = NULL;
   }
}
