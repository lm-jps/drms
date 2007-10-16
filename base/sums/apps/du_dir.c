/* Return the du info on the given directory. Recursively goes down any 
 * subdirectories. Includes the size of the directory entries themselves.
 * Returns the number of bytes.
*/

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

void fsize(char *);
void dirwalk(char *, void (*fcn)(char *));

static double total;		/* # of bytes */

/* Original call with a directory name */
double du_dir(char *dirname)
{
  total = 0.0;
  fsize(dirname);
  return(total);		/* ret # of bytes */
}


/* Get size of given file. If a dir then call dirwalk which will call us. */
void fsize(char *name)
{
  struct stat stbuf;

  if(lstat(name, &stbuf) == -1) {
    fprintf(stderr, "du_dir can't access %s\n", name); 
    return;
  }
  if((stbuf.st_mode & S_IFMT) == S_IFDIR)
    dirwalk(name, fsize);
  total = total + (double)stbuf.st_size;
}

/* Apply fcn to all files in the given dir. */
void dirwalk(char *dir, void (*fcn)(char *))
{
  char name[196];
  struct dirent *dp;
  DIR *dfd;

  if((dfd=opendir(dir)) == NULL) {
    fprintf(stderr, "du_dir can't open dir %s\n", dir);
    return;
  }
  while((dp=readdir(dfd)) != NULL) {
    if(strcmp(dp->d_name, ".") == 0
    || strcmp(dp->d_name, "..") == 0)
      continue;				/* skip self and parent */
    sprintf(name, "%s/%s", dir, dp->d_name);
    (*fcn)(name);
  }
  closedir(dfd);
}
