#include "list.h"
#include "jsoc.h"

LinkedList_t *list_llcreate(unsigned int datasize, ListFreeFn_t freefn)
{
    LinkedList_t *list = calloc(1, sizeof(LinkedList_t));

    if (list)
    {
        list->dsize = datasize;
        list->freefn = freefn;
        list->nitems = 0;
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
                llist->last = node;
            }
            else
            {
                node->next = llist->first;
                llist->first = node;
            }

            llist->nitems++;
        }
    }

    return node;
}

ListNode_t *list_llinserttail(LinkedList_t *llist, void *data)
{
    ListNode_t *node = NULL;
    ListNode_t *last = NULL;

    if (llist && data)
    {
        node = calloc(1, sizeof(ListNode_t));

        if (node)
        {
            node->data = calloc(1, llist->dsize);
            if (node->data)
            {
                memcpy(node->data, data, llist->dsize);
            }

            if (!llist->first)
            {
                llist->first = node;
                llist->last = node;
            }
            else
            {
                last = llist->last;
                llist->last = node;
                last->next = node;
            }

            llist->nitems++;
        }
    }

    return node;
}

void list_llremove(LinkedList_t *llist, ListNode_t *item)
{
    if (!llist) return;
    if (!item) return;

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

            if (node->next == NULL)
            {
                /* item was the last node */
                llist->last = prev;
            }
        }

        llist->nitems--;
    }
}

void list_llreset(LinkedList_t *llist)
{
    if (llist) llist->next = llist->first;
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
                break;
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
        node = llist->last;
    }

    return node;
}

void list_llfree(LinkedList_t **llist)
{
    ListNode_t *pElem = NULL;
    ListNode_t *nElem = NULL;

    if (llist && *llist)
    {
        pElem = (*llist)->first;
        nElem = NULL;

        while (pElem)
        {
            nElem = pElem->next;

            /* need to free malloc'd mem */
            if (pElem->data)
            {
                if ((*llist)->freefn)
                {
                    /* deep free the node*/
                    (*((*llist)->freefn))(pElem->data);
                }

                /* free the mem allocated for the node's data */
                free(pElem->data);
            }

            free(pElem);
            pElem = nElem;
        }

        free(*llist);
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
