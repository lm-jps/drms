#include <stdio.h>
#include <soi_key.h>
#include <printk.h>

extern int write_log(char *fmt, ...);

void logkey (KEY *key)
{
switch(key->type) {
case KEYTYP_STRING:
  write_log ("%s:\tKEYTYP_STRING\t%s\n", key->name, (char*)key->val);
  break;
case KEYTYP_BYTE:
  write_log ("%s:\tKEYTYP_BYTE\t%d\n", key->name, *(char*)key->val);
  break;
case KEYTYP_INT:
  write_log ("%s:\tKEYTYP_INT\t%d\n", key->name, *(int*)key->val);
  break;
case KEYTYP_FLOAT:
  write_log ("%s:\tKEYTYP_FLOAT\t%13.6e\n", key->name, *(float*)key->val);
  break;
case KEYTYP_DOUBLE:
  write_log ("%s:\tKEYTYP_DOUBLE\t%23.6e\n", key->name, *(double*)key->val);
  break;
case KEYTYP_TIME:
  write_log ("%s:\tKEYTYP_TIME\t%23.6e\n", key->name, *(TIME*)key->val);
  break;
case KEYTYP_SHORT:
  write_log ("%s:\tKEYTYP_SHORT\t%d\n", key->name, *(short*)key->val);
  break;
case KEYTYP_LONG:
  write_log ("%s:\tKEYTYP_LONG\t%d\n", key->name, *(long*)key->val);
  break;
case KEYTYP_UBYTE:
  write_log ("%s:\tKEYTYP_UBYTE\t%d\n", key->name, *(unsigned char*)key->val);
  break;
case KEYTYP_USHORT:
  write_log ("%s:\tKEYTYP_USHORT\t%d\n", key->name, *(unsigned short*)key->val);
  break;
case KEYTYP_UINT:
  write_log ("%s:\tKEYTYP_UINT\t%d\n", key->name, *(unsigned int*)key->val);
  break;
case KEYTYP_ULONG:
  write_log ("%s:\tKEYTYP_ULONG\t%d\n", key->name, *(unsigned long*)key->val);
  break;
case KEYTYP_UINT64:
  write_log ("%s:\tKEYTYP_UINT64\t%ld\n", key->name, *(uint64_t*)key->val);
  break;
case KEYTYP_UINT32:
  write_log ("%s:\tKEYTYP_UINT32\t%d\n", key->name, *(uint32_t*)key->val);
  break;
case KEYTYP_FILEP:
  write_log ("%s:\tKEYTYP_FILEP\t%ld\n", key->name, *(FILE **)key->val);
  break;
default:
  write_log ("(void)\n");
}
}

/* Read a file of keylist values and return a keylist.
 * Return 1 if error, else 0.
 * File looks like:
 * current_client: KEYTYP_FILEP    5393392
 * in:     KEYTYP_STRING   ds_mdi.fd_V_01h_lev1_8[70361-70363]
 * in_nsets:       KEYTYP_INT      1
 * in_select:      KEYTYP_STRING   ds_mdi.fd_V_01h_lev1_8
 * in_fsn: KEYTYP_INT      70361
*/
int file2keylist(char *filename, KEY **list) {
  FILE *fp2;
  char *token;
  char line[256], name[96], type[96], value[96];

  if((fp2=fopen(filename, "r")) == NULL) {
    printk("**Can't keylist file %s\n", filename);
    return(1);
  }
  while(fgets(line, 128, fp2)) {
    if(line[0] == '#' || line[0] == '\n') continue;
    if(token=(char *)strtok(line, ":\t")) {
      strcpy(name, token);
      if(token=(char *)strtok(NULL, "\t\n")) {
        strcpy(type, token);
      }
      else {
        fclose(fp2);
        return(1);
      }
      if(token=(char *)strtok(NULL, "\t\n")) 
        strcpy(value, token);		/* allow empty token here */
      if(!strcmp(type, "KEYTYP_STRING")) {
        setkey_str(list, name, value);
        continue;
      }
      if(!strcmp(type, "KEYTYP_INT")) {
        setkey_int(list, name, atoi(value));
        continue;
      }
      if(!strcmp(type, "KEYTYP_FILEP")) {
        setkey_fileptr(list, name, (FILE *)strtoul(value, NULL, 0));
        continue;
      }
      if(!strcmp(type, "KEYTYP_BYTE")) {
        setkey_byte(list, name, (char)atoi(value));
        continue;
      }
      if(!strcmp(type, "KEYTYP_UBYTE")) {
        setkey_ubyte(list, name, (unsigned char)atoi(value));
        continue;
      }
      if(!strcmp(type, "KEYTYP_SHORT")) {
        setkey_short(list, name, (short)atoi(value));
        continue;
      }
      if(!strcmp(type, "KEYTYP_USHORT")) {
        setkey_ushort(list, name, (unsigned short)atoi(value));
        continue;
      }
      if(!strcmp(type, "KEYTYP_UINT")) {
        setkey_uint(list, name, (unsigned int)atoi(value));
        continue;
      }
      if(!strcmp(type, "KEYTYP_LONG")) {
        setkey_long(list, name, (long)atol(value));
        continue;
      }
      if(!strcmp(type, "KEYTYP_ULONG")) {
        setkey_ulong(list, name, (unsigned long)strtoul(value,NULL,0));
        continue;
      }
      if(!strcmp(type, "KEYTYP_FLOAT")) {
        setkey_float(list, name, (float)atof(value));
        continue;
      }
      if(!strcmp(type, "KEYTYP_DOUBLE")) {
        setkey_double(list, name, (double)strtod(value,NULL));
        continue;
      }
      /* TBD - figure out how to get value ****
      if(!strcmp(type, "KEYTYP_TIME")) {
        setkey_time(list, name, value);
      }
      ****************************************/
    }
    else {
      fclose(fp2);
      return(1);
    }
  }
  fclose(fp2);
  return(0);
}
