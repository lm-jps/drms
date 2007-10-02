//#define DEBUG
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/wait.h>
#include <unistd.h>
#include <zlib.h>
#include "tee.h"

#define BUFSZ 1024

static gzFile gz_fd_e, gz_fd_o;
static int stdout_sav, stderr_sav;

static void sighandler(int signo)
{
  fprintf(stderr,"Process received signal %d. Aborting.\n",signo);
  gzclose(gz_fd_e);
  gzclose(gz_fd_o);
  _exit(1);
}  

/* Tee stdout and stderr to the given files. 

   How it works:
   A fork is performed inside this function and thus two processes will
   exist after the call to tee_stdio. The child process will return from 
   the call to tee_stdio and should proceed normally. The parent process 
   will handle the teeing and wait for the child process process to finish,
   after which the parent process will exit with the exit status code set by 
   the child. The communication of the stdout and stderr streams is done 
   through named pipes (fifos) residing in /tmp/fifo_stderr.<pid> and
   /tmp/fifo_stdout.<pid>, where <pid> is the PID of the parent process.

   The role of parent and child in the above design has to be reversed
   due to requirement in jsoc_main.c, namely to be able to wait on
   child processes.

   Since this is communications between parent and child, pipes are
   sufficient. The named pipes are replaced by just pipes. This way we
   avoid temporary files altogether.

   Return value:
   tee_stdio return 0 on success and -1 on error.
*/

pid_t tee_stdio(const char *stdout_file, mode_t stdout_mode, 
		const char *stderr_file, mode_t stderr_mode)
{
  pid_t pid;
  static struct sigaction act;

  pid = getpid();
#ifdef DEBUG
  fprintf(stderr,"teeing...\n");
#endif
  int pipe_fd_e[2], pipe_fd_o[2];
  if (pipe(pipe_fd_e) < 0 || 
      pipe(pipe_fd_o) < 0) {
    perror("pipe error");
    return -1;
  }

  /* Set signal handler to clean up. */
  act.sa_handler = sighandler;
  sigfillset(&(act.sa_mask));
  sigaction(SIGINT, &act, NULL);
  sigaction(SIGTERM, &act, NULL);


  /* Fork off a child process to handle the tee'ing. */
  if ((pid = fork()) == -1)
  {
    fprintf(stderr,"Fork system call failed, aborting\n");
    exit(-1);
  }
  if (pid == 0)
  {
    int nread,closed_e=0, closed_o=0;
    int numfd;
    fd_set readset;
    char buf[BUFSZ];
    int fd_e, fd_o;

    /* This is the child. */

    /* close write end */
    close(pipe_fd_e[1]);
    close(pipe_fd_o[1]);

    /* Create file for stderr. */
    if ((fd_e = creat(stderr_file, stderr_mode)) == -1)
    {
      fprintf(stderr,"Couldn't create file '%s'\n",stderr_file);
      exit(-1);
    }
    gz_fd_e = gzdopen(fd_e, GZFMODE);
#ifdef DEBUG
    printf("created file '%s'\n",stderr_file);
#endif	
    /* Create file for stdout. */
    if ((fd_o = creat(stdout_file, stdout_mode)) == -1)
    {
      fprintf(stderr,"Couldn't create file '%s'\n",stdout_file);
      exit(-1);
    }
    gz_fd_o = gzdopen(fd_o, GZFMODE);
#ifdef DEBUG
    printf("created file '%s'\n",stdout_file);
    write(STDOUT_FILENO, "STARTING TEE\n", sizeof("STARTING TEE\n"));   
#endif
    numfd = pipe_fd_o[0]>pipe_fd_e[0] ?  pipe_fd_o[0]+1 : pipe_fd_e[0]+1;
    while(!closed_e || !closed_o)
    {
      FD_ZERO(&readset);
      if (!closed_o)
	FD_SET(pipe_fd_o[0],&readset);
      if (!closed_e)
	FD_SET(pipe_fd_e[0],&readset);
      if (select(numfd, &readset, NULL, NULL, NULL) < 0)
      {
	if (errno == EINTR)
	  continue;
	else
	{
	  perror("Select failed with error code");
	  exit(-1);
	}
      }
      if (FD_ISSET(pipe_fd_o[0], &readset))
      {
	if ((nread = read(pipe_fd_o[0],buf,BUFSZ)) < 0)
	{
	  /* We always have to check for interrupted system calls... */
	  if (errno == EINTR)
	    continue;
	  else
	    break;
	}
	else
	{
	  if (nread==0)
	  {
#ifdef DEBUG
	    write(STDOUT_FILENO, "GOT CLOSE ON STDOUT\n", 
		  sizeof("GOT CLOSE ON STDERR\n"));
#endif
	    closed_o++;
	  }
	  else
	  {
	    /* Write to both file and stdout. */
	    while( gzwrite(gz_fd_o, buf,nread) < 0 )
	    {
	      if (errno != EINTR)
		break;
	    }
	    while( write(STDOUT_FILENO, buf, nread) < 0 )
	    {
	      if (errno != EINTR)
		break;
	    }	      
	  }
	}
      }
      if (FD_ISSET(pipe_fd_e[0], &readset))
      {
	if ((nread = read(pipe_fd_e[0],buf,BUFSZ)) < 0)
	{
	  /* We always have to check for interrupted system calls... */
	  if (errno == EINTR)
	    continue;
	  else
	    break;
	}
	else
	{
	  if (nread==0)
	  {
#ifdef DEBUG
	    write(STDOUT_FILENO, "GOT CLOSE ON STDERR\n", 
		  sizeof("GOT CLOSE ON STDERR\n"));
#endif	    
	    closed_e++;
	  }
	  else
	  {
	    /* Write to both file and stdout. */
	    while( gzwrite(gz_fd_e, buf,nread) < 0)
	    {
	      if (errno != EINTR)
		break;
	    }
	    while( write(STDERR_FILENO, buf, nread) < 0)
	    {
	      if (errno != EINTR)
		break;
	    }	      
	  }
	}
      }
    }
    gzclose(gz_fd_e);
    gzclose(gz_fd_o);
    _exit(0);
  }
  else {
    /* parent */
    /* close read end */
    close(pipe_fd_e[0]);
    close(pipe_fd_o[0]);
    if (!redirect_stdeo(pipe_fd_e[1], pipe_fd_o[1]))
      return pid;
    return -1;    
  }
}

/* Redirect stdout and stderr to files.
   If the old stdout descriptor is connected to a terminal,
   line buffering is also used on the new stdout. Otherwise
   block buffering is used to increase performance of pipelines.
   stdout is not buffered.
 */
int redirect_stdio(const char *stdout_file, mode_t stdout_mode, 
		   const char *stderr_file, mode_t stderr_mode)
{
  int fd_e,fd_o, isterm;

  /* Redirect stderr to file. */
  if ((fd_e = open(stderr_file,O_WRONLY | O_CREAT, stderr_mode)) == -1)
  {
    fprintf(stderr,"Couldn't open file '%s'\n",stderr_file);
    return -1;
  }
  fflush(stderr); /* Just in case the default (no) buffering has been
		     altered. */
  if( dup2(fd_e, STDERR_FILENO) == -1)
  {
    perror("dup2 call failed for stderr");
    return -1;
  }
  close(fd_e);
  /* Set buffering  to get the usual behavior for stderr. */
  setbuf(stderr,NULL);

  /* Redirect stdout to file. */
  if ((fd_o = open(stdout_file,O_WRONLY | O_CREAT, stdout_mode)) == -1)
  {
    fprintf(stderr,"Couldn't open file '%s'\n",stdout_file);
    return -1;
  }
  fflush(stdout); 
  isterm = isatty(STDOUT_FILENO);
  if (dup2(fd_o, STDOUT_FILENO)==-1)
  {
    perror("dup2 call failed for stdout");
    return -1;
  }	  
  close(fd_o);
  /* Set buffering to get the usual behavior for stdout. */
  if (isterm)
    setlinebuf(stdout);
  return 0;
}

int redirect_stdeo(int fd_e, int fd_o)
{
  int isterm;

  /* Redirect stderr to file. */
  fflush(stderr); /* Just in case the default (no) buffering has been
		     altered. */
  if(dup2(fd_e, STDERR_FILENO) == -1)
  {
    perror("dup2 call failed for stderr");
    return -1;
  }
  close(fd_e);
  /* Set buffering  to get the usual behavior for stderr. */
  setbuf(stderr,NULL);

  /* Redirect stdout to file. */
  fflush(stdout); 
  isterm = isatty(STDOUT_FILENO);
  if (dup2(fd_o, STDOUT_FILENO)==-1)
  {
    perror("dup2 call failed for stdout");
    return -1;
  }	  
  close(fd_o);
  /* Set buffering to get the usual behavior for stdout. */
  if (isterm)
    setlinebuf(stdout);
  return 0;
}

/* Duplicate stderr and stdout so we can restore them later. */
int save_stdeo() {
  stderr_sav = dup(STDERR_FILENO);
  if (stderr_sav < 0) {
    perror("dup failed. failed to save stderr");    
    return 1;
  }
  stdout_sav = dup(STDOUT_FILENO);
  if (stdout_sav < 0) {
    perror("dup failed. failed to save stdout");    
    return 1;
  }
  return 0;
}

/* Restore stderr and stdout */
int restore_stdeo() {
  stderr = fdopen(stderr_sav, "w");
  if (!stderr) {
    perror("fdopen failed. Can't restore stderr");
    return 1;
  }
  stdout = fdopen(stdout_sav, "w");
  if (!stdout) {
    perror("fdopen failed. Can't restore stdout");
    return 1;
  }
  return 0;
}
		  
