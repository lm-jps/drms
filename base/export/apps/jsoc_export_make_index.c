// #define DEBUG 1
#define DEBUG 0

/*
 *  jsoc_export_make_index - Generates index.XXX files for dataset export.
 *  Should be run in the directory containing a jsoc export index.txt file.
 *  Will read the index.txt and generate index.json and index.html
 *
*/

#include <stdio.h>
#include "json.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>

#define kPROTOCOL_as_is 0
#define kPROTOCOL_fits 1

// #include "json.h"

static char x2c (char *what) {
  register char digit;

  digit = (what[0] >= 'A' ? ((what[0] & 0xdf) - 'A')+10 : (what[0] - '0'));
  digit *= 16;
  digit += (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A')+10 : (what[1] - '0'));
  return (digit);
}

static void CGI_unescape_url (char *url) {
  register int x, y;

  for (x = 0, y = 0; url[y]; ++x, ++y) {
    if ((url[x] = url[y]) == '%') {
      url[x] = x2c (&url[y+1]);
      y += 2;
    }
  }
  url[x] = '\0';
}

char * string_to_json(char *in)
  {
  char *new;
  new = json_escape(in);
  return(new);
  }

#define HTML_INTRO "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">" \
		"<html><head title=\"JSOC Data Export Request Results Index\">\n"  \
		"</head><body>\n"  \
		"<h2>JSOC Data Request Summary</h2>\n"  \
		"<table>\n"

#define HTTP_SERVER "http://jsoc.stanford.edu"
#define FTP_SERVER  "ftp://pail.stanford.edu/export"
		

/* Module main function. */
int main(int argc, char **argv)
  {
  FILE *index_txt, *index_html, *index_json;
  char buf[1000];
  char dir[1000];
  char tarfile[1000]; *tarfile = '\0';
  char requestid[1000]; *requestid = '\0';
  json_t *jroot;
  json_t *recinfo;
  json_t *fileinfo;
  char *json, *final_json;
  int state = 0;
  int method_ftp = 0;
  int method_tar = 0;
  int protocol = kPROTOCOL_as_is;
  tarfile[0] = '\0';
  requestid[0] = '\0';

  if (argc && strcmp(*argv, "-h") == 0)
    {
    printf ("Usage:\njsoc_export_make_index {-h}\n");
    return(0);
    }

  index_txt = fopen("index.txt","r");
  if (!index_txt)
    {
    fprintf(stderr,"XX jsoc_export_make_index - failed to open index.txt.\n");
    return(1);
    }

  index_html = fopen("index.html","w");
  if (!index_html)
    {
    fprintf(stderr,"XX jsoc_export_make_index - failed to create index.html.\n");
    return(1);
    }
  fprintf(index_html, HTML_INTRO);

  index_json = fopen("index.json","w");
  if (!index_json)
    {
    fprintf(stderr,"XX jsoc_export_make_index - failed to create index.json.\n");
    return(1);
    }
  jroot = json_new_object();
  recinfo = json_new_array();
  
  int doHeader = 0;

  while (fgets(buf, 1000, index_txt))
    {
    char *name, *val, *sustr;
    char *namestr, *valstr;
    char *sustatus = NULL;
    char *susize = NULL;
    char linkbuf[2048];
    char protocolbuf[1000];
    char *p = buf + strlen(buf) - 1;
    char *c;
    char *procArg = NULL;
    char *procVal = NULL;
    
    if (p >= buf && *p == '\n')
      *p = '\0'; 
    p = buf;
    switch (state)
    {
      case 0: // Initial read expect standard header line
        if (strncmp(buf, "# JSOC ",7) != 0)
        {
            fprintf(stderr, "XX jsoc_export_make_index - incorrect index.txt file.\n");
            return(1);
        }
    	state = 1;
	    break;
	  case 10: // Processing section            
        // Skip header separators.
        if (strncmp(buf, "  --", 4) == 0)
        {
            break;
        }

        if (strncmp(buf, "# DATA",6) == 0)
        {
            if (strncmp(buf, "# DATA SU", 9) == 0)
                state = 3;
            else
                state = 2;
                
            fprintf(index_html, "</table>\n");
            fprintf(index_html, "<p><h2><b>Selected Data</b></h2><p>\n");
            fprintf(index_html, "<table>\n");
            break;
        }
        
        // skip blank and comment lines
        if (!*p || *p == '#') 
        {
            break;
        }
        
        if (strncmp(buf, "Processing", 10) == 0)
        {
            c = strchr(buf, ':');
            procVal = strtok(++c, " ");
            
            fprintf(index_html, "<tr><td><b>%s</b></td></tr>\n", procVal);
            doHeader = 1;
        }
        else
        {
            /* Gotta split on whitespace, tab. */
            procArg = strtok(buf, " \t");
            procVal = strtok(NULL, " \t");
            if (doHeader)
            {
                fprintf(index_html, "<tr><td><b>%s</b></td><td><b>%s</b></td></tr>\n", procArg, procVal);
            }
            else
            {
                fprintf(index_html, "<tr><td><b>%s</b></td><td>%s</td></tr>\n", procArg, procVal);
            }
            doHeader = 0;
	    }
	    break;
	    
      case 1:  // In header section, take name=val pairs.
        if (strncmp(buf, "# DATA",6) == 0 || strncmp(buf, "# PROCESSING", 12) == 0) // done with header ?
        {  // Now at end of header section, write special information
            if (strncmp(buf, "# PROCESSING", 12) == 0)
            {
                state = 10;
            }
            else if (strncmp(buf, "# DATA SU", 9) == 0)
                state = 3;
            else
                state = 2;
                
            // special line for keywords
            if (protocol == kPROTOCOL_as_is)
                sprintf(protocolbuf, "%s/%s.keywords.txt", dir, requestid);
            else
                sprintf(protocolbuf, "**IN FITS FILES**");
                
            namestr = string_to_json("keywords");
            valstr = string_to_json(protocolbuf);
            json_insert_pair_into_object(jroot, namestr, json_new_string(valstr));
            free(namestr);
            free(valstr);
            fprintf(index_html, "<tr><td><b>keywords</b></td><td>%s</td></tr>\n", protocolbuf);
              
            // special line for tarfiles
            if (method_tar)
            {
                sprintf(tarfile, "%s/%s.tar", dir, requestid);
                namestr = string_to_json("tarfile");
                valstr = string_to_json(tarfile);
                json_insert_pair_into_object(jroot, namestr, json_new_string(valstr));
                free(namestr);
                free(valstr);
                // put name=value pair into index.html
                fprintf(index_html, "<tr><td>tarfile</td><td><a href=\"%s%s\">%s</a></td></tr>\n", (method_ftp ? FTP_SERVER : HTTP_SERVER), tarfile, tarfile);
            }
            
            if (state == 10)
            {
                fprintf(index_html, "</table>\n");
                fprintf(index_html, "<p><h2><b>Processing Steps</b></h2><p>\n");
                fprintf(index_html, "<table>\n");
            }
            else
            {
                fprintf(index_html, "</table><p><h2><b>Selected Data</b></h2><p><table>\n");
            }
            break;
        }
        
        if (*p == '#' || !*p) // skip blank and comment lines
          break;
        if ((val=index(p, '='))==NULL)
          {
          fprintf(stderr, "XX jsoc_export_make_index - ignore unexpected line in header, \n    %s\n", buf);
          break;
          }
        p = val++;
        name = buf;
        while (isblank(*name))
          name++;
            *p-- = '\0';
        while (isblank(*p) && p >= buf)
              *p-- = '\0';
            while (isblank(*val))
              val++;
        p = val + strlen(val);
            p--;
        while (isblank(*p) && p >= val)
              *p-- = '\0';

            // Convert names to lower case.
        for (c=name; *c; c++)
               *c = tolower(*c);

            // check for special actions on some keywords
        // save dir for use in data section
        if (strcmp(name, "dir") == 0)
            {
               strncpy(dir, val, 1000);

               // linux/unix only!
               if (dir[strlen(dir) - 1] == '/')
               {
                  dir[strlen(dir) - 1] = '\0';
               }
            }

        // save requestid 
        if (strcmp(name, "requestid") == 0)
          strncpy(requestid, val, 1000);
            // Check for method==ftp
            if (strncmp(name, "protocol", 6) == 0)
              {
              if (strcmp(val, "as-is") == 0)
                protocol = kPROTOCOL_as_is;
              else if (strcmp(val, "fits") == 0)
                protocol = kPROTOCOL_fits;
              }
            if (strncmp(name, "method", 6) == 0)
              {
              char *dash = index(val, '-');
              if (dash && strncmp(dash+1, "tar", 3) == 0)
                method_tar = 1;
              if (strncmp(val, "ftp", 3) == 0)
                method_ftp = 1;
              }

        // put name=value pair into index.json
        namestr = string_to_json(name);
        valstr = string_to_json(val);
        json_insert_pair_into_object(jroot, namestr, json_new_string(valstr));
        free(namestr);
        free(valstr);
        // put name=value pair into index.html
        fprintf(index_html, "<tr><td><b>%s</b></td><td>%s</td></tr>\n", name, val);
        break;
      case 2: // Data section contains pairs of record query and filenames
        if (*p == '#' || !*p) // skip blank and comment lines
          break;
        name = buf;
        while (isblank(*name)) // skip leading blanks
          name++;
            p = name;

            /* record query might have spaces in it - can't use space as a delimiter;
             * but it appears that jsoc_export_as_is separates the two fields with a \t */
        while (*p && *p != '\t') // skip past query
          p++;
        if (*p)
          *p++ = '\0'; // mark end of query
        val = p;
        while (isblank(*val)) // skip leading blanks
          val++;
        p = val + strlen(val);
            p--;
        while (isblank(*p) && p >= val) // omit trailing blanks
              *p-- = '\0';
        // put query : filename pair into index.json
        fileinfo = json_new_object();
        namestr = string_to_json(name);
        json_insert_pair_into_object(fileinfo, "record", json_new_string(namestr));
        free(namestr);
            valstr = string_to_json(val);
        json_insert_pair_into_object(fileinfo, "filename", json_new_string(valstr));
        free(valstr);
        json_insert_child(recinfo, fileinfo);
        // put name=value pair into index.html
            if (method_tar)
          fprintf(index_html, "<tr><td>%s</td><td>%s</td></tr>\n", name, val);
            else
              {
          fprintf(index_html, "<tr><td>%s</td><td><A HREF=\"%s%s/%s\">%s</A></td></tr>\n",
                name, (method_ftp ? FTP_SERVER : HTTP_SERVER), dir, val, val);
              }
        break;
      case 3: // Data section for Storage Units contains triples of sunum, seriesname, path, online status, file size
        if (*p == '#' || !*p) // skip blank and comment lines
          break;
        sustr = buf;
        while (isblank(*sustr)) // skip leading blanks
          sustr++;
        p = sustr;
        while (*p && !isblank(*p)) // skip past sunum
          p++;
        if (*p)
          *p++ = '\0'; // mark end of sunum
        name = p;
        while (isblank(*name)) // skip leading blanks
          name++;
        p = name;
        while (*p && !isblank(*p)) // skip past seriesname
          p++;
        if (*p)
          *p++ = '\0'; // mark end of seriesname
        val = p;
        while (isblank(*val)) // skip leading blanks
          val++;

        p = val;
        while (*p && !isblank(*p)) // skip past directory
          p++;
        if (*p)
          *p++ = '\0'; // mark end of directory

        sustatus = p;
        while (isblank(*sustatus)) // skip leading blanks
          sustatus++;
        p = sustatus;
        while (*p && !isblank(*p)) // skip past sustatus
          p++;
        if (*p)
          *p++ = '\0'; // mark end of sustatus

        susize = p;
        while (isblank(*susize)) // skip leading blanks
          susize++;
        p = susize;
        while (*p && !isblank(*p)) // skip past susize
          p++;
        if (*p)
          *p++ = '\0'; // mark end of susize

        // put sunum, seriesname, and path into json
        fileinfo = json_new_object();
        namestr = string_to_json(sustr);
        json_insert_pair_into_object(fileinfo, "sunum", json_new_string(sustr));
        free(namestr);
        namestr = string_to_json(name);
        json_insert_pair_into_object(fileinfo, "series", json_new_string(namestr));
        free(namestr);
        valstr = string_to_json(val);
        json_insert_pair_into_object(fileinfo, "path", json_new_string(valstr));
        free(valstr);

        // online status
        json_insert_pair_into_object(fileinfo, "sustatus", json_new_string(sustatus));

        // SU size
        json_insert_pair_into_object(fileinfo, "susize", json_new_string(susize));

        json_insert_child(recinfo, fileinfo);

        if (strcasecmp(val, "NA") == 0)
        {
           snprintf(linkbuf, sizeof(linkbuf), "NA");
        }
        else
        {
           /* fill in with SU path */
           snprintf(linkbuf, sizeof(linkbuf), "<A HREF=\"%s/%s\">%s</A>", HTTP_SERVER, val, val);
        }

        // put name=value pair into index.html
        fprintf(index_html, 
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n", 
                sustr, /* sunum */
                name, /* owning series */
                linkbuf, /* link or NA */
                sustatus, /* SU status - Y, N, X, I */
// probably in MB
                susize /* SU size in bytes */ );
        break;
      default:
        fprintf(stderr, "Unsupported case '%d'.\n", state);
      }
    }
  fclose(index_txt);

  // finish json
  json_insert_pair_into_object(jroot, "data", recinfo);
  json_tree_to_string(jroot,&json);
  final_json = json_format_string(json);
  fprintf(index_json, "%s\n",final_json);
  fclose(index_json);

  // finish html
  fprintf(index_html, "</table></body></html>\n");
  fclose(index_html);

  return(0);
  }
