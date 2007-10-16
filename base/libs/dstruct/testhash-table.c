#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "parse_params.h"
#include "timer.h"

int main(int argc, char *argv[])
{

  int i;

  for (i=0; i<100; i++)
  {
    XXX_params_parse_arguments(argc, argv);
    XXX_params_free();       
  }
  //  params_free();       
  
  StartTimer(1);
  params_parse_arguments(argc, argv);
  printf("Time to parse parameters = %f.\n",StopTimer(1));
  /*  printf("int = %d\n",GetArgInt("int")); 
  printf("char = %c\n",GetArgChar("char")); 
  printf("string = %s\n",GetArgString("string")); 
  printf("float = %f\n",GetArgFloat("float")); 
  printf("double = %f\n",GetArgDouble("double"));  */
  printf("\nDump of hash table:\n");
  params_print();
  printf("\nHash table bin counts:\n");
  params_stat();
  params_free();       
  return 0;
}
