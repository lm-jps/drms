#include "drms.h"

int status = 0;
DRMS_RecordSet_t *recSet = drms_open_records(drms_env, recSetStr, &status);
DRMS_RecordSet_t *recSetClone = NULL;

if (status == DRMS_SUCCESS && recSet != NULL)
{
   DRMS_RecordSet_t *recSetClone =
     drms_clone_records(recSet,
			DRMS_PERMANENT,
			DRMS_SHARE_SEGMENTS,
			&status);

   if (status == 0 && recSetClone != NULL)
   {
      /* Do something with recSetClone here. */
   }
}

if (recSet != NULL)
{
   drms_close_records(recSet, DRMS_FREE_RECORD);
}

if (recSetClone != NULL)
{
   drms_close_records(recSetClone, DRMS_INSERT_RECORD);
}
