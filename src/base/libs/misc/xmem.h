#ifndef XMEM_H
#define XMEM_H



#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

//////////////////////// Macros implementing common calls /////////////////////

// turn all XMEM debugging options on
#define xmem_on(p) do { xmem_config(1,1,1,1,100,1,0,0); xmem_reset(); } while(0)
// turn all XMEM debugging options off
#define xmem_off(p) xmem_config(0,0,0,0,100,0,0,0)

// Use it like malloc
#define xmem_malloc(size) xmem_domalloc_params((size), __FILE__, __LINE__)

// A version of xmem_malloc that doesn't let you accidently use the wrong type.
#define xmem_malloctype(type, n_elements) (type *)xmem_malloc((n_elements) * sizeof (type))

// Allocate memory and zero it out.  Using this macro avoids the possibility
// of accidently specifying the wrong arguments to memset after allocating.
#define xmem_calloc(n_elements, size) xmem_calloc_params((n_elements), (size), __FILE__, __LINE__)
#define xmem_calloctype(type, n_elements) (type *)xmem_calloc_params((n_elements), sizeof (type), __FILE__, __LINE__)

// use it like free;  
#define xmem_free(ptr) do { xmem_dofree(ptr); ptr = NULL; } while(0)

// use it like realloc; 
#define xmem_realloc(ptr, size) xmem_dorealloc_params((ptr), (size), __FILE__, __LINE__)
#define xmem_realloctype(type, ptr, n_elements) (type *)xmem_realloc(ptr, ((n_elements) * sizeof (type)))



//////////////////////// Macros replacing LIBC calls with XMEM calls /////////////////////
#ifdef XMEM_REPLACE_LIBC
#undef malloc
#undef calloc
#undef free
#undef realloc
#undef strdup

#define malloc(size)             xmem_domalloc_params((size), __FILE__, __LINE__)
#define free(ptr)                xmem_dofree(ptr)
#define calloc(n_elements,size)  xmem_calloc_params((n_elements), (size), __FILE__, __LINE__)
#define realloc(ptr,size)        xmem_dorealloc_params((ptr), (size), __FILE__, __LINE__)
#define strdup(str)              xmem_strdup_params((str), __FILE__, __LINE__)
#endif

//////////////////////// Main allocation and freeing functions /////////////////////

// returns ptr to block of memory of the size specified.
// Returns NULL upon failure 
void* xmem_domalloc_params(unsigned int size, const char *filename, int linenum);

void* xmem_domalloc_params(unsigned int size, const char *filename, int linenum);
char *xmem_strdup_params(const char *str, const char *filename, int linenum); 

// Allocate memory and zero it out.
void *xmem_calloc_params(size_t n_elements, size_t size, const char * fname, int linenum);

// Frees space pointed to by ptr up for reuse. 
void xmem_dofree(void* ptr);

// Check that starting and ending guardwords are not corrupted.
int xmem_check_guardword(void *ptr);


// Reallocates a block of memory from the heap, keeping the data
// the same in the reallocated block of memory (up to the size of the
// old ptr).  The ptr to the reallocated memory is returned.  Returns
// NULL upon failure 
void* xmem_dorealloc_params(void* ptr, unsigned int size, const char *filename, 
			    int linenum);

//////////////////////// Utility functions /////////////////////

// See if number of allocs equals number of frees, and 
// print a list of the calls responsible for the leaked memory. 
int xmem_leakcheck(void);
void xmem_leakreport(void); // The same but without returning status. Can be used as argument to atexit.
int xmem_check_all_guardwords(FILE * fp, int maxlines); // prints a list of all corrupted memory blocks

// Print out blocks of unfreed memory.
size_t xmem_usage(FILE * fp, int maxlines);

// Print the highest memory usage since the last call to xmem_recenthighwater.
size_t xmem_recenthighwater(void);

// Empty list of unfreed memory.
void xmem_reset(void);

// Turn features of XMEM on or off.
void xmem_config(int memory_leak_locate, int trap_on_allocation, int replace_libc, 
		 int hang_on_out_of_mem, int report_length, int fill_with_nan,
		 int assert_on_null_free, int warn_only);



#endif /* XMEM_H */
