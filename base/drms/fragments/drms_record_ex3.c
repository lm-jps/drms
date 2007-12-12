#include "drms.h"

DRMS_RecordSet_t *recSet = drms_open_records(drms_env, recSetStr, &status);

if (status == 0 && recSet != NULL)
{
   int nRecs = recSet->n;
}

if (recSet != NULL)
{
   drms_close_records(recSet, DRMS_FREE_RECORD);
}


DRMS_RecordSet_t *recSetClone = drms_clone_records(recSet,
						   DRMS_PERMANENT,
						   DRMS_SHARE_SEGMENTS,
						   &status);

if (status == 0 && recSetClone != NULL)
{
   /* Do something with recSetClone here. */
}

if (recSetClone != NULL)
{
   drms_close_records(recSetClone, DRMS_INSERT_RECORD);
}
