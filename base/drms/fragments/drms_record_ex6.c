DRMS_RecordSet_t *rs = drms_open_recordset(env, query, &drmsstatus);
DRMS_Record_t *rec = NULL;

if (rs)
{
   while ((rec = drms_recordset_fetchnext(env, gCacheRS, &drmsstatus)) != NULL)
   {
      /* do something with rec */
   }

   drms_close_records(rs, DRMS_FREE_RECORD);
   rs = NULL;
}
