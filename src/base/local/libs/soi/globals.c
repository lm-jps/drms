/* globals.c  - globals needed by libsoi.so, and accessor functions. */

int soi_errno;

const int GetSOIErrno()
{
   return soi_errno;
}
