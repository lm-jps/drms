#if defined(__linux__) && __linux__
#define _GNU_SOURCE
#endif /* LINUX */
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <alloca.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/statvfs.h>
#include <ctype.h>
#include "util.h"
#include "xassert.h"
#include "xmem.h"

#define ISUPPER(X) (X >= 0x41 && X <= 0x5A)
#define ISLOWER(X) (X >= 0x61 && X <= 0x7A)
#define ISDIGIT(X) (X >= 0x30 && X <= 0x39)
#define DRMS_MAXNAMELEN 32

typedef enum
{
     kKwCharFirst = 0,
     kKwCharNew,
     kKwCharError
} KwCharState_t;

// To extract namespace from a fully qulified name
char *ns(const char *name) {
   char *nspace = strdup(name);
   char *pc = strrchr(nspace, '.');
   if (pc) {
      *pc = '\0';
   }
   return nspace;
}

void copy_string(char **dst, char *src)
{
  XASSERT((src!=NULL));
  XASSERT(*dst != src); /* Probably a mistake. */
  if (*dst)
    free(*dst);
  *dst = strdup(src);
}

void strtolower(char *str)
{
  int n,i;
  n= strlen(str);
  for (i=0;i<n;i++)
    str[i] = tolower(str[i]);
}

void strtoupper(char *str)
{
  int n,i;
  n= strlen(str);
  for (i=0;i<n;i++)
    str[i] = toupper(str[i]);
}

/* Always NULL-terminates dst */
size_t base_strlcat(char *dst, const char *src, size_t size)
{
   size_t max = size - strlen(dst) - 1; /* max non-NULL can add */
   size_t start = strlen(dst);

   if (max > 0)
   {
      snprintf(dst + start, max + 1, "%s", src); /* add 1 to max for NULL */
   }

   return start + strlen(src);
}

/* sizedst is the currently allocated size of dst */
void *base_strcatalloc(char *dst, const char *src, size_t *sizedst)
{
   size_t srclen = strlen(src);
   size_t dstlen = strlen(dst);
   void *retstr = NULL;

   if (srclen > *sizedst - dstlen - 1)
   {
      void *tmp = realloc(dst, *sizedst * 2);
      if (tmp)
      {
         *sizedst *= 2;
         retstr = tmp;
      }
   }
   else
   {
      retstr = dst;
   }

   if (retstr)
   {
      base_strlcat(retstr, src, *sizedst);
   }

   return retstr;
}

int convert_int_field(char *field, int len)
{
  char *buf;
  
  buf = alloca(len+1);
  memcpy(buf,field,len);
  return atoi(buf);
}

long convert_long_field(char *field, int len)
{
  char *buf;
  
  buf = alloca(len+1);
  memcpy(buf,field,len);
  return atol(buf);
}

float convert_float_field(char *field, int len)
{
  char *buf;
  
  buf = alloca(len+1);
  memcpy(buf,field,len);
  return (float)atof(buf);
}

double convert_double_field(char *field, int len)
{
  char *buf;
  
  buf = alloca(len+1);
  memcpy(buf,field,len);
  return atof(buf);
}


void convert_string_field(char *field, int len, char *output, int maxlen)
{
  int l;
  
  l = (len>maxlen?maxlen:len);
  strncpy(output,field,l);
  output[l] = '\0';
}


#define BUFSIZE (1<<20)

int copyfile(const char *inputfile, const char *outputfile)
{
  int fin, fout;
  ssize_t nread;
  char *buffer = 0;
  int oflags;

#ifdef _GNU_SOURCE
  struct statvfs stat;
  static unsigned long align=-1;
  oflags = O_DIRECT;
#else
  oflags = 0;
#endif

  if ( (fin = open(inputfile, O_RDONLY|oflags) ) == -1 )
    return -1;

  if ( (fout = open(outputfile, O_WRONLY|O_CREAT|O_TRUNC|oflags, 0644 ) ) == -1
)
  {
    close(fin);
    return -2;
  }

#ifdef _GNU_SOURCE
  if (align == -1)
  {
    fstatvfs(fin, &stat);
    align = stat.f_bsize;
    fstatvfs(fout, &stat);
    align = align > stat.f_bsize ? align : stat.f_bsize;

    XASSERT((buffer = malloc(BUFSIZE+align)));
    buffer = buffer + (align - ((unsigned long)buffer % align));
  }
#else
  XASSERT((buffer = malloc(BUFSIZE)));
#endif


  while ( (nread = read(fin, buffer, BUFSIZE)) > 0 )
  {
    if ( write(fout, buffer, nread) < nread )
    {
      close(fin);
      close(fout);
      unlink(outputfile);
      return -3;
    }
  }
  close(fin);
  close(fout);

  if (nread == -1)
    return -4;
  else
    return 0;
}

int IsValidFitsKeyName(const char *fitsName)
{
   int error = 0;
   KwCharState_t state = kKwCharNew;
   char *nameC = strdup(fitsName);
   char *pc = nameC;

   if (strlen(fitsName) > 8)
   {
      /* max size is 8 chars */
      error = 1;
   }
   else
   {
      while (*pc != 0 && !error)
      {
	 switch (state)
	 {
	    case kKwCharError:
	      error = 1;
	      break;
	    case kKwCharNew:
	      if (*pc == '-' ||
		  *pc == '_' ||
		  ISUPPER(*pc) ||
		  ISDIGIT(*pc))
	      {
		 state = kKwCharNew;
		 pc++;
	      }
	      else
	      {
		 state = kKwCharError;
	      }
	      break;
	    default:
	      state = kKwCharError;
	 }
      }
   }

   if (*pc != 0) 
   {
      error = 1;
   }

   if (nameC)
   {
      free(nameC);
   }

   return !error;
}

/* Phil's scheme for arbitrary fits names. */
/* XXX This has to change to Phil's default scheme */
int GenerateFitsKeyName(const char *drmsName, char *fitsName, int size)
{
   const char *pC = drmsName;
   int nch = 0;

   memset(fitsName, 0, size);

   if (size >= 9)
   {
      while (*pC && nch < 8)
      {
	 if (*pC >= 65 && *pC <= 90)
	 {
	    fitsName[nch] = *pC;
	    nch++; 
	 }
	 else if (*pC >= 97 && *pC <= 122)
	 {
	    fitsName[nch] = toupper(*pC);
	    nch++;
	 }

	 pC++;
      }
   }
   else
   {
      return 0;
   }

   return 1;
}

/*
<Keyword>
	= 'Keyword:' <KeyName> ',' <TypeAndFields>
<KeyName>
	= <Name>
<Name>
	= [A-Za-z] { <NameEnd> }
<NameEnd>
	= [A-Za-z0-9_] { <NameEnd> }
*/
int IsValidDRMSKeyName(const char *drmsName)
{
   int error = 0;
   KwCharState_t state = kKwCharFirst;
   char *nameC = strdup(drmsName);
   char *pc = nameC;

   if (strlen(drmsName) > DRMS_MAXNAMELEN - 1)
   {
      error = 1;
   }
   else
   {
      while (*pc != 0 && state != kKwCharError)
      {
	 switch (state)
	 {
	    case kKwCharError:
	      error = 1;
	      break;
	    case kKwCharFirst:
	      if (ISUPPER(*pc) ||
		  ISLOWER(*pc))
	      {
		 state = kKwCharNew;
		 pc++;
	      }
	      else
	      {
		 state = kKwCharError;
	      }
	      break;
	    case kKwCharNew:
	      if (ISUPPER(*pc) ||
		  ISLOWER(*pc) ||
		  ISDIGIT(*pc) ||
		  *pc == '_')
	      {
		 state = kKwCharNew;
		 pc++;
	      }
	      else
	      {
		 state = kKwCharError;
	      }
	      break;
	    default:
	      state = kKwCharError;
	 }
      }
   }

   if (*pc != 0)
   {
      error = 1;
   }

   if (nameC)
   {
      free(nameC);
   }

   return !error;
}

/* XXX This has to change to Phil's default scheme */
int GenerateDRMSKeyName(const char *fitsName, char *drmsName, int size)
{
   int error = 0;
   const char *pcIn = fitsName;
   char *pcOut = drmsName;
   char trail[8] = {0};

   KwCharState_t state = kKwCharFirst;

   size--; /* Leave room for terminated null char. */
   while (*pcIn != 0 && pcOut < drmsName + size)
   {
      switch (state)
      {
	 case kKwCharError:
	   error = 1;
	   break;

	 case kKwCharFirst:
	   if (*pcIn == '-')
	   {
	      /* FITS keyword name starts with an hyphen */
	      if (pcOut + 4 < drmsName + size)
	      { 
		 *pcOut++ = 'm';
		 *pcOut++ = 'h';
		 *pcOut++ = '_';
		 *pcOut++ = '_';
		 state = kKwCharNew;
	      }
	      else
	      {
		 state = kKwCharError;
	      }
	   }
	   else if (*pcIn == '_')
	   {
	      /* FITS keyword name starts with an underscore */
	      if (pcOut + 4 < drmsName + size)
	      { 
		 *pcOut++ = 'm';
		 *pcOut++ = 'n';
		 *pcOut++ = '_';
		 *pcOut++ = '_';
		 state = kKwCharNew;
	      }
	      else
	      {
		 state = kKwCharError;
	      }
	   }
	   else if (*pcIn >= 0x30 && *pcIn <= 0x39)
	   {
	      /* FITS keyword name starts with a numeral */
	      if (pcOut + 4 < drmsName + size)
	      { 
		 *pcOut++ = 'm';
		 *pcOut++ = 'n';
		 *pcOut++ = '_';
		 *pcOut++ = *pcIn;
		 state = kKwCharNew;
	      }
	      else
	      {
		 state = kKwCharError;
	      }
	   }
	   else
	   {
	      *pcOut++ = *pcIn;
	      state = kKwCharNew;
	   }
	    
	   break;

	 case kKwCharNew:
	   if (*pcIn == '-')
	   {
	      if (*trail == '\0')
	      {
		 strncat(trail, "_mh", sizeof(trail) - 1);
	      }

	      /* FITS keyword has an hyphen - need room for trailing "_mh" */
	      if (pcOut + 4 < drmsName + size)
	      {
		 *pcOut++ = '_';
		 state = kKwCharNew;
	      }
	      else
	      {
		 state = kKwCharError;
	      }
	   }
	   else
	   {
	      *pcOut++ = *pcIn;
	      state = kKwCharNew;
	   }
	   break;
	    
      } /* switch */

      if (state != kKwCharError)
      {
	 pcIn++;
      }

   } /* while */

   if (strlen(trail) > 0 && pcOut + strlen(trail) < drmsName + size)
   {
      strcpy(pcOut, trail);
      pcOut += strlen(trail);
   }

   *pcOut = '\0';

   return !error;
}
