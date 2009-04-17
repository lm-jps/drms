//#if defined(__linux__) && __linux__
//#define _GNU_SOURCE
//#endif /* LINUX */
#include "jsoc.h"
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
#include <dirent.h>
#include "util.h"
#include "xassert.h"
#include "xmem.h"
#include "hcontainer.h"

#define ISUPPER(X) (X >= 0x41 && X <= 0x5A)
#define ISLOWER(X) (X >= 0x61 && X <= 0x7A)
#define ISDIGIT(X) (X >= 0x30 && X <= 0x39)
#define DRMS_MAXNAMELEN 32

char *kFITSRESERVED[] =
{
   "SIMPLE",
   "EXTEND",
   "BZERO",
   "BSCALE",
   "BLANK",
   "BITPIX",
   "NAXIS",
   "COMMENT",
   "HISTORY",
   "END",
   ""
};

HContainer_t *gCleanup = NULL;
HContainer_t *gReservedFits = NULL;

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
    str[i] = (char)tolower(str[i]);
}

void strtoupper(char *str)
{
  int n,i;
  n= strlen(str);
  for (i=0;i<n;i++)
    str[i] = (char)toupper(str[i]);
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

static void FreeReservedFits(void *data)
{
   if (gReservedFits != (HContainer_t *)data)
   {
      fprintf(stderr, "Unexpected argument to FreeReservedFits(); bailing.\n");
      return;
   }

   hcon_destroy(&gReservedFits);
}

int FitsKeyNameValidation(const char *fitsName)
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
      /* Disallow FITS reserved keywords - simple, extend, bzero, bscale, blank, bitpix, naxis, naxisN, comment, history, end */
      if (!gReservedFits)
      {
         char bogusval = 'A';
         int i = 0;

         gReservedFits = hcon_create(1, 128, NULL, NULL, NULL, NULL, 0);
         while (*(kFITSRESERVED[i]) != '\0')
         {
            hcon_insert(gReservedFits, kFITSRESERVED[i], &bogusval);
            i++;
         }

         /* Register for clean up (also in the misc library) */
         BASE_Cleanup_t cu;
         cu.item = gReservedFits;
         cu.free = FreeReservedFits;
         base_cleanup_register("reservedfitskws", &cu);
      }

      if (gReservedFits)
      {
         char *tmp = strdup(fitsName);
         char *pch = NULL;
         char *endptr = NULL;
         char *naxis = "NAXIS";
         int len = strlen(naxis);
         int theint;

         strtoupper(tmp);

         if (strncmp(tmp, naxis, len) == 0)
         {
            pch = tmp + len;

            if (*pch)
            {
               theint = (int)strtol(pch, &endptr, 10);
               if (endptr == pch + strlen(pch))
               {
                  /* the entire string after NAXIS was an integer */
                  if (theint > 0 && theint <= 999)
                  {
                     /* fitsName is a something we can't export */
                     error = 2;
                  }
               }
            }
         }

         if (hcon_lookup(gReservedFits, tmp))
         {
            error = 2;
         }

         free(tmp);
      }
 
      if (!error)
      {
         while (*pc != 0 && !error)
         {
            switch (state)
            {
               case kKwCharError:
                 error = 2;
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
   }

   if (*pc != 0) 
   {
      error = 3;
   }

   if (nameC)
   {
      free(nameC);
   }

   return error;
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
	    fitsName[nch] = (char)toupper(*pC);
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

#define kMAXRECURSION 128
int RemoveDir(const char *pathname, int maxrec)
{
   int status = 0;

   char pbuf[PATH_MAX];
   struct stat stBuf;

   if (maxrec < kMAXRECURSION && maxrec >= 0)
   {
      if (!stat(pathname, &stBuf) && S_ISDIR(stBuf.st_mode))
      {
         /* Append '/' if necessary */
         snprintf(pbuf, sizeof(pbuf), "%s", pathname);

         if (pathname[strlen(pathname) - 1] != '/')
         {
            base_strlcat(pbuf, "/", sizeof(pbuf));
         }

         struct dirent **fileList = NULL;
         int nFiles = -1;

         /* delete all non-dir files */
         if ((nFiles = scandir(pbuf, &fileList, NULL, NULL)) > 0 && 
             fileList != NULL)
         {
            int fileIndex = 0;

            while (fileIndex < nFiles)
            {
               struct dirent *entry = fileList[fileIndex];
               if (entry != NULL)
               {
                  char *oneFile = entry->d_name;
                  char dirEntry[PATH_MAX] = {0};
                  snprintf(dirEntry, 
                           sizeof(dirEntry), 
                           "%s%s", 
                           pbuf,
                           oneFile);
                  if (*dirEntry !=  '\0' && !stat(dirEntry, &stBuf) && status == 0)
                  {
                     if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode))
                     {
                        /* delete single file */
                        status = unlink(dirEntry);
                     }
                     else if (S_ISDIR(stBuf.st_mode))
                     {
                        /* don't try to delete . or .. */
                        if (strcmp(oneFile, ".") != 0 && strcmp(oneFile, "..") != 0)
                        {
                           maxrec--;
                           if (maxrec >= 0)
                           {
                              status = RemoveDir(dirEntry, maxrec);
                           }
                        }
                     }
                  }

                  free(entry);
               }

               fileIndex++;
            }
         }	

         /* delete the directory */
         if (status == 0)
         {
            status = rmdir(pathname);
         }
      }
   }

   return status;
}

size_t CopyFile(const char *src, const char *dst)
{
   struct stat stbuf;
   FILE *fptrS = NULL;
   FILE *fptrD = NULL;
   char buf[2048];
   size_t nbytes = 0;
   size_t nbytesW = 0;
   size_t nbytesTotal = 0;
   

   if (!stat(src, &stbuf) && (S_ISREG(stbuf.st_mode) || S_ISLNK(stbuf.st_mode)))
   {
      fptrS = fopen(src, "r");
      fptrD = fopen(dst, "w");

      if (fptrS && fptrD)
      {
         while ((nbytes = fread(buf, sizeof(char), sizeof(buf), fptrS)) > 0 && !ferror(fptrS) && !ferror(fptrD))
         {
            nbytesW = fwrite(buf, sizeof(char), nbytes, fptrD);
            if (nbytesW != nbytes)
            {
               break;
            }

            nbytesTotal += nbytesW;
         }
      }

      if (fptrS)
      {
         fclose(fptrS);
      }

      if (fptrD)
      {
         fclose(fptrD);
      }
   }

   return nbytesTotal;
}

void base_cleanup_init()
{
   gCleanup = hcon_create(sizeof(BASE_Cleanup_t), 128, NULL, NULL, NULL, NULL, 0);
}

int base_cleanup_register(const char *key, BASE_Cleanup_t *cu)
{
   int error = 0;

   if (!gCleanup)
   {
      base_cleanup_init();
   }

   if (gCleanup)
   {
      if (hcon_lookup(gCleanup, key))
      {
         /* already exists */
         fprintf(stderr, "base_cleanup_register(): cannot register '%s' - already exists.\n", key);
         error = 1;
      }
      else
      {
         hcon_insert(gCleanup, key, cu);
      }
   }

   return error;
}

int base_cleanup_go(const char *explicit)
{
   int error = 0;
   BASE_Cleanup_t *cu = NULL;

   if (gCleanup)
   {
      if (explicit && *explicit)
      {
         cu = hcon_lookup(gCleanup, explicit);

         if (cu)
         {
            (*(cu->free))(cu->item);
         }
         else
         {
            error = 1;
         }
      }
      else
      {
         /* clean all up */
         HIterator_t *hiter = hiter_create(gCleanup);

         while ((cu = hiter_getnext(hiter)) != NULL)
         {
            (*(cu->free))(cu->item);
         }
      }
   }

   return error;
}

void base_cleanup_term()
{
   if (gCleanup)
   {
      hcon_destroy(&gCleanup);
   }
}

void base_term()
{
   base_cleanup_go("reservedfitskws");
}
