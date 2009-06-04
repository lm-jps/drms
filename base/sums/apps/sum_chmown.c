#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <regex.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pwd.h>
/*  #include <grp.h>  */
#include <dirent.h>

#include "serverdefs.h"

#define PATTERN "^/SUM[0-9]*/D[0-9]+$"

void die(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    exit(1);
}

void recursive_chmown(const char *dir, uid_t owner, gid_t group) {
    DIR *d;
    struct dirent *entry;
    struct stat fs;
    char pname[1024];

    if (!(d = opendir(dir)))
	die("recursive_chmown(): can't open directory %s\n", dir);

    while (entry = readdir(d)) {
	if ((strcmp(".", entry->d_name) == 0) ||
	    (strcmp("..", entry->d_name) == 0))
	    continue;

	if (snprintf(pname, 1024, "%s/%s", dir, entry->d_name) >= 1024)
	    die("recursive_chmown(): pathname %s/%s too long", dir,
		entry->d_name);

	if (lstat(pname, &fs) < 0)
	    die("recursive_chmown(): can't lstat dirent %s\n", pname);

	if (S_ISDIR(fs.st_mode)) {
	    if (chown(pname, owner, group) < 0)
		die("recursive_chmown(): can't chown %s\n", pname);
	    if (chmod(pname, 0755) < 0)
		die("recursive_chmown(): can't chmod %s\n", pname);
	    recursive_chmown(pname, owner, group);
	} else if (S_ISREG(fs.st_mode)) {
	    if (chown(pname, owner, group) < 0)
		die("recursive_chmown(): can't chown %s\n", pname);
	    if (chmod(pname, 0644) < 0)
		die("recursive_chmown(): can't chmod %s\n", pname);
	}
    }

    closedir(d);
}

int main(int argc, char *argv[]) {
    regex_t reg;
    struct stat fs;
    struct passwd *pwd = getpwnam (SUMS_MANAGER);
    uid_t sumsowner = pwd->pw_uid;
/*
    struct group *grp = getgrnam (SUMS_GROUP);
    gid_t sumsgroup = grp->gr_gid;
*/
    if (argc != 2)
	die("Usage: %s <SUDIR>\n", argv[0]);

    if (regcomp(&reg, PATTERN, REG_EXTENDED | REG_NOSUB))
	die("%s: bad regex %s\n", argv[0], PATTERN);
    if (regexec(&reg, argv[1], 0, 0, 0))
	die("%s: non SUDIR %s ignored\n", argv[0], argv[1]);

    if (lstat(argv[1], &fs) < 0)
	die("%s: can't lstat directory %s\n", argv[0], argv[1]);
    if (!S_ISDIR(fs.st_mode))
	die("%s: %s is not a directory\n", argv[0], argv[1]);

    if (chown(argv[1], sumsowner, -1) < 0)
	die("%s: can't chown %s\n", argv[0], argv[1]);
    if (chmod(argv[1], 0755) < 0)
	die("%s: can't chmod %s\n", argv[0], argv[1]);
    recursive_chmown(argv[1], sumsowner, -1);

    return 0;
}
