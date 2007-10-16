/* Clean up starting at the directory given. Removes this directory and then 
 * moves up one directory and if there are no subdirs here then will delete it.
 * This continues until we find a subdir or until we get to the top of the 
 * dsds assigned storage root which is given in the second argument.
 * The given directory name (wd) can end in a slash (/) or not.
 * Returns 1 if error on removing any directory.
*/
#include <dirent.h>
#include <strings.h>
#include <SUM.h>

int rmdirs(char *wd, char *root)
{
  char *cptr;
  int i;
  DIR *dirp;
  char rmstr[MAXSTR];
  char rmcmd[MAXSTR];

  strcpy(rmstr, wd);
  if(!(cptr=(char *)rindex(rmstr, '/'))) 
    return(1);
  if(!strcmp(cptr+1, ""))		/* wd ends in a slash */
    *cptr=(char)NULL;			/* remove the slash */
  sprintf(rmcmd, "rm -rf %s\n", rmstr);
  if(system(rmcmd))
    return(1);
  cptr=(char *)rindex(rmstr, '/');	/* next directory up */
  *cptr=(char)NULL;
  while(strstr(rmstr, root)) {
    if(!(dirp=opendir(rmstr))) return(1);
    i=0;
    while(readdir(dirp)) i++;
    closedir(dirp);
    if(i == 2) {			/* no subdir. ok to rmdir */
      sprintf(rmcmd, "rm -rf %s\n", rmstr);
      if(system(rmcmd))
        return(1);
    }
    else
      break;
    cptr=(char *)rindex(rmstr, '/');	/* next directory up */
    *cptr=(char)NULL;
  }
  return(0);
}
