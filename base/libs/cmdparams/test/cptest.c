#include "jsoc.h"
#include "cmdparams.h"

ModuleArgs_t module_args[] = { 
   {ARG_FLAG, "p", NULL, "Test flag.", NULL},   /* print details */
   {ARG_STRING, "strarg1", NULL, "Test string arg with no default", NULL},
   {ARG_STRING, "strarg2", "jicama", "Test string arg with default", NULL},
   {ARG_INT, "intarg", NULL, "Test int arg with no default", NULL},
    {ARG_INT, "intargrange", NULL, "Test int arg with no default", "[7,10]"},
   {ARG_FLOAT, "fltarg", "23.7", "Test float arg with default", NULL},
   {ARG_DOUBLE, "dblarg", "67.237627", "", NULL},
   {ARG_INTS, "intsarg", "[1,2,6,2,6,7,3,78]", "Test int-array arg with default", NULL},
   {ARG_FLOATS, "fltsarg", "[20.2,26.26,62.10]", "Test float-array arg with default", NULL},
   {ARG_DOUBLES, "dblsarg", NULL, "Test double-array arg with no default", NULL},
   {ARG_NUME, "numearg", "blue", "Test enumerated argument type with default.", "red,orange,yellow,green,blue,violet"},
   {ARG_END, NULL, NULL, NULL, NULL}
};

ModuleArgs_t *gModArgs = module_args;

static void Argprint(const void *data) 
{
   CmdParams_Arg_t *arg = (CmdParams_Arg_t *)data;

   char *name = arg->name;
   int unnamednum = arg->unnamednum;
   char *value = arg->strval;

   if (name)
   {
      printf("%s=\"%s\"\n", name, value);
   }
   else
   {
      printf("unnamed_arg_%03d=\"%s\"\n", unnamednum, value);
   }
}

int main(int argc, char **argv)
{
    int err = 0;
    char reservebuf[128];
    CmdParams_t cmdparams;
    int status = CMDPARAMS_SUCCESS;
    
    memset(&cmdparams, 0, sizeof(CmdParams_t));
    
    /* Reserve some cmd-line params */
    snprintf(reservebuf, sizeof(reservebuf), "L,Q,V" );
    cmdparams_reserve(&cmdparams, reservebuf, "cptest");
    
    /* Parse the cmd-line arguments. */
    status = cmdparams_parse(&cmdparams, argc, argv);
    
    if (status == CMDPARAMS_QUERYMODE) 
    {
        cmdparams_usage("cptest");
        return 0;
    } 
    else if (status == CMDPARAMS_NODEFAULT) 
    {
        fprintf(stderr, "For usage, type %s [-H|--help]\n", argv[0]);
        err = 1;
    } 
    else if (status < 0) 
    {
        fprintf(stderr, "Error: Command line parsing failed. Aborting.\n");
        fprintf(stderr, "For usage, type %s [-H|--help]\n", "cptest");
        err = 1;
    }
    
    if (!err)
    {
        int iarg = 0;
        const char *argname = NULL;
        const char *argval = NULL;
        
        /* Spit cmd-line args back out. */
        fprintf(stdout, "Printing out entire CmdParams_t struct.\n");
        hcon_map(cmdparams.args, Argprint);
        fprintf(stdout, "\n");
        
        fprintf(stdout, "Using cmdparams_getargument() to print all arguments as strings.\n");
        while (cmdparams_getargument(&cmdparams, iarg, &argname, &argval, NULL, NULL))
        {
            fprintf(stdout, "arg %d: %s='%s'\n", iarg, argname, argval);
            iarg++;
        }
    }
    
    /* Test individual API function calls. */
    int8_t val8 = cmdparams_get_int8(&cmdparams, "intarg", &status);
    int16_t val16 = cmdparams_get_int16(&cmdparams, "intarg", &status);
    int32_t val32 = cmdparams_get_int32(&cmdparams, "intarg", &status);
    int64_t val64 = cmdparams_get_int64(&cmdparams, "intarg", &status);
    uint64_t mask = cmdparams_get_mask64(&cmdparams, "intarg", &status);
    
    printf("8-bit signed int: %hhd\n", val8);
    printf("16-bit signed int: %hd\n", val16);
    printf("32-bit signed int: %d\n", val32);
    /* on 64-bit machines, int64_t is long int, not long long int*/
    printf("64-bit signed int: %lld\n", (long long)val64);
    printf("64-bit mask (uint64_t): %llu\n", (unsigned long long)mask);
    
    cmdparams_freeall(&cmdparams);
    
    return err;
}
