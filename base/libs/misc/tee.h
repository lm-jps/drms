#ifndef __TEE_H
#define __TEE_H

#define GZFMODE "w9"
pid_t tee_stdio (const char *stdout_file, mode_t stdout_mode, 
    const char *stderr_file, mode_t stderr_mode);
int redirect_stdio (const char *stdout_file, mode_t stdout_mode, 
    const char *stderr_file, mode_t stderr_mode);
int redirect_stdeo (int fd_e, int fd_o);
int save_stdeo (void);
int restore_stdeo (void);

#endif
