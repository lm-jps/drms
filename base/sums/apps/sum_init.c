/* /home/jim/cvs/PROTO/src/SUM/sum_init.c
*/
/* This function is called by sum_svc whenever it starts up.
 * This initializes the PART, PEUID and PADATA structures from their 
 * corresponding database tables.
 * Returns non-0 on error.
*/
#include <SUM.h>
#include <sum_rpc.h>

/* Must decl functions */
int DS_PavailRequest2();

int SUM_Init(char *dbname)
{

  DS_ConnectDB(dbname); /* connect to DB for init */
  if(DS_PallocClean())  /* delete old DARW & DARO in sum_partn_alloc */
    return(1);		/* also deletes all entries in sum_open */
  if(DS_PavailRequest2())
    return(1);
/************* OLD stuff now obsolete ****
  if(DS_PavailRequest())
    return(1);
  if(DS_OpenPeRequest())
    return(1);
  if(DS_PallocRequest())
    return(1);
*************/
  return(0);
}

