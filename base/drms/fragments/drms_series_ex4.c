// To check if each member of a set of segments exists in a series:
int status = DRMS_SUCCESS;
int error = 0;
if (!drms_series_checksegcompat(drms_env, series, segs, nSegs, &status))
{
  if (status == DRMS_SUCCESS)
  {
    fprintf(stderror,
            "One or more segments do not match a segment in series %s.\n",
            series);
            error = 1;
  }
}
