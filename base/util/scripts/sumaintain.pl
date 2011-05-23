#!/home/jsoc/bin/linux_x86_64/perl -w

use Inline C => Config => LIBS => '-L/home/arta/cvs/JSOC/lib/linux_x86_64/ -ldrmsserver.a', INC => '-I/home/arta/cvs/JSOC/include';
use Inline C => << 'END_C_CODE';


#include "jsoc_main.h"

char *GetSUs(char *query)
{
    DRMS_RecordSet_t *recset = NULL;
    int drmsstatus = DRMS_SUCCESS;
    int irec;

    recset = drms_open_records(drms_env, query, &drmsstatus);

    if (recset && recset->n > 0)
    {
        
    }
    
}

END_C_CODE

$sulist = GetSUs("hmi.M_45s[2010.11.20/1m]");

print "$sulist\n";
exit(0);
