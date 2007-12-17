// To check a record's compatibility with a series:
int status = DRMS_SUCCESS;
int error = 0;
int compat = 0;
HContainer_t *matchSegNames = NULL;
XASSERT((matchSegNames = (HContainer_t *)malloc(sizeof(HContainer_t))) != NULL);
compat = drms_series_checkrecordcompat(drms_env,
                                       series,
                                       prototype,
                                       matchSegNames,
                                       &status);
if (!compat)
{
  fprintf(stderr,
          "Output series %s is not compatible with output data.\n",
          series);
          error = 1;
}
...
hcon_destroy(&matchSegNames);

