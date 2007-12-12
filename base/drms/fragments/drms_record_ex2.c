#include "drms.h"

int status = 0;
DRMS_RecordSet_t *recordSet = NULL;

recordSet = drms_open_records(drms_env, recSetStr, &status);
if (status == DRMS_SUCCESS && recordSet != NULL)
{
   /* Do something with recordSet here. */
}

if (recordSet != NULL)
{
   drms_close_records(recordSet, DRMS_FREE_RECORD);
}
