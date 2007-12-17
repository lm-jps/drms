// To check if each member of a set of keys exists in a series:
int status = DRMS_SUCCESS;
int error = 0;
if (!drms_series_checkkeycompat(drms_env, series, keys, nKeys, &status))
{
  if (status == DRMS_SUCCESS)
  {
    fprintf(stderror,
            "One or more keywords do not match a keyword in series %s.\n",
            series);
            error = 1;
  }
}
