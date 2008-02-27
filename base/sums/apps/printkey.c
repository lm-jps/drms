#include <stdio.h>
#include <soi_key.h>

void printkey (KEY *key)
{
printf ("%s:\t", key->name);
switch(key->type) {
case KEYTYP_STRING:
  printf ("KEYTYP_STRING\t");
  printf ("%s\n", (char *) key->val);
  break;
case KEYTYP_BYTE:
  printf ("KEYTYP_BYTE\t");
  printf ("%d\n", *(char *)key->val);
  break;
case KEYTYP_INT:
  printf ("KEYTYP_INT\t");
  printf ("%d\n", *(int *)key->val);
  break;
case KEYTYP_FLOAT:
  printf ("KEYTYP_FLOAT\t");
  printf ("%13.6e\n", *(float *)key->val);
  break;
case KEYTYP_DOUBLE:
  printf ("KEYTYP_DOUBLE\t");
  printf ("%23.16e\n", *(double *)key->val);
  break;
case KEYTYP_TIME:
  printf ("KEYTYP_TIME\t");
  printf ("%23.16e\n", *(TIME *)key->val);
  break;
case KEYTYP_SHORT:
  printf ("KEYTYP_SHORT\t");
  printf ("%d\n", *(short *)key->val);
  break;
case KEYTYP_LONG:
  printf ("KEYTYP_LONG\t");
  printf ("%ld\n", *(long *)key->val);
  break;
case KEYTYP_UBYTE:
  printf ("KEYTYP_UBYTE\t");
  printf ("%d\n", *(unsigned char *)key->val);
  break;
case KEYTYP_USHORT:
  printf ("KEYTYP_USHORT\t");
  printf ("%d\n", *(unsigned short *)key->val);
  break;
case KEYTYP_UINT:
  printf ("KEYTYP_UINT\t");
  printf ("%d\n", *(unsigned int *)key->val);
  break;
case KEYTYP_ULONG:
  printf ("KEYTYP_ULONG\t");
  printf ("%lu\n", *(unsigned long *)key->val);
  break;
case KEYTYP_UINT64:
  printf ("KEYTYP_UINT64\t");
  printf ("%lu\n", *(uint64_t *)key->val);
  break;
case KEYTYP_UINT32:
  printf ("KEYTYP_UINT32\t");
  printf ("%d\n", *(uint32_t *)key->val);
  break;
case KEYTYP_FILEP:
  printf ("KEYTYP_FILEP\t");
  printf ("%lu\n", *(unsigned long *)key->val);
  break;
default:
  printf ("(void)\n");
}
}
