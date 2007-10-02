#ifndef CTYPES_H_DEF
#define CTYPES_H_DEF

#define FLOAT 0
#define DOUBLE 1
#define COMPLEXFLOAT 2
#define COMPLEXDOUBLE 3

#define CPRINT(x) printf( #x " = %.15e + %.15ei\n",creal(x),cimag(x))
#define RPRINT(x) printf( #x " = %.15e\n",x)
#endif
