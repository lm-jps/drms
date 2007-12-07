int drms_close (DRMS_Env_t *env, int action) 
{
   int status;

   if ((status = drms_closeall_records(env,action)))
   {
      fprintf(stderr, "ERROR in drms_close: failed to close records in cache.\n");
   }

   /*  Close connection to database  */
   drms_disconnect(env, 0);
   drms_free_env(env);
   return status;
}
