//To access the primary keyword names:
int nPKeys = 0;
char **pkArray = NULL;
int ikey;

pkArray = drms_series_createpkeyarray(drms_env, dsout, &nPKeys, &status);
if (status == DRMS_SUCCESS)
{
  for (iKey = 0; iKey < nPKeys; iKey++)
  {
    /* Do something with pkArray[ikey] */
  }

  drms_series_destroypkeyarray(&pkArray, nPKeys);
}
