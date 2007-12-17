#include "drms.h"

int status = 0;
DRMS_Record_t *template = drms_template_record(drms_env, 
                                               seriesNameStr, 
                                               &status);

if (status == DRMS_SUCCESS && template != NULL)
{
     /* Do something with template here. */
     int nPrime = template->seriesinfo->pidx_num;
     ...
}
