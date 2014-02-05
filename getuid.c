#include <sys/types.h>
#include <pwd.h>
#include <stdio.h>

main (int argc, char **argv) {
  uid_t uid = -1;
  char *who = argv[1];
  if (who) {
    struct passwd *pwd = getpwnam (who);
    if (pwd) uid = pwd->pw_uid;
  }
  printf ("%d\n", (unsigned int)uid);
  return 0;
}
