//To check for the existence of a series:
int status = DRMS_SUCCESS;
drms_series_exists(drms_env, seriesout, &status);
if (status == DRMS_ERROR_UNKNOWNSERIES))
{
  fprintf(stderr, "Output series %s doesn't exist.\n", seriesout);
  return 1;
}

