#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <xassert.h>
#include <xmem.h>

/* to use:
	call xmem_on(); at the very beginning of the program to turn it on.
	use malloc and free as usual.
	xmem_usage()- print out the memory that is allocated at that point
	xmem_check_guardword(a,0)- determine if a block of memory at address a
		has been corrupted by being written over by something else
		going out of bounds and writting in memory not allocated to it
	xmem_check_all_guardwords(<stream>, <number>)- print the first <number> of 
		memory blocks that were corrupted in the way above to <stream>
	xmem_leakcheck()- print out memory that was corrupt at the time of being
		freed (or not freed at all?)
		
*/


//
// Enable memory leak checking.  This prints out a list of all unfreed memory
// buffers at the end of the program, or whenever you choose to call
// xmem_usage().
//
// Each allocation is marked with a timestamp so that you can determine which
// memory allocation was not freed.  Simply run the program through, look
// at the timestamps, then rerun it with the environment variable 
// XMEM_TRAP_ALLOCATION_TIMESTAMP equal to the value of a timestamp 
// that was printed out. This time the program will hang when that 
// particular allocation occurs, enabling you to attach to the process
// with gdb and determine where the allocation occured.
//


/*	if defined, XMEM_REPLACE_LIBC replaces the default malloc function with its own
	for all code linked together, regardless of whether it was compiled with xmem.h
	included. however, any file/ object library that did not have xmem.h included at
	compile time will appear as if xmem.c allocated the memory, even though it did
	not.
	 
	if it is not defined, the memory checking only logs/monitors memory that is allocated
	in files with xmem.c included, but may be inaccurate in that case.
	
	it is commented out hear b/c it is defined with the -D complier option if to be
	compiled that way
#define XMEM_REPLACE_LIBC
*/

// Flags determining which features of xmem are enabled.
#ifdef XMEM_MEMORY_LEAK_LOCATE
static int xmem_memory_leak_locate = 1;
#else
static int xmem_memory_leak_locate = 0;
#endif
#ifdef XMEM_TRAP_ALLOCATION_TIMESTAMP
static int xmem_trap_allocation_timestamp = 1;
#else
static int xmem_trap_allocation_timestamp = 0;
#endif
#ifdef XMEM_REPLACE_LIBC
static int xmem_replace_libc = 1;
#else
static int xmem_replace_libc = 0;
#endif
#ifdef XMEM_HANG_ON_OUM
static int xmem_hang_on_oum =  1;
#else
static int xmem_hang_on_oum =  0;
#endif
#ifdef XMEM_REPORT_LENGTH
static int xmem_report_length =  XMEM_REPORT_LENGTH;
#else
static int xmem_report_length =  100;
#endif
#ifdef XMEM_FILL_WITH_NAN
static int xmem_fill_with_nan =  1;
#else
static int xmem_fill_with_nan =  0;
#endif
#ifdef XMEM_ASSERT_ON_NULL_FREE
static int xmem_assert_on_null_free =  1;
#else
static int xmem_assert_on_null_free =  0;
#endif
#ifdef XMEM_WARN_ONLY
static int xmem_warn_only =  1;
#else
static int xmem_warn_only =  0;
#endif
                                  
#define ASSERT(val) {if ( xmem_warn_only && !(val)) \
                       fprintf(stderr, "%s, line %d: Assertion \"" \
                         #val "\" failed.\n", __FILE__, __LINE__); \
                      else \
  		        assert_or_hang((val)); }

extern void *__libc_malloc(size_t size);
extern void *__libc_calloc(size_t nmemb, size_t size);
extern void __libc_free(void *ptr);
extern void *__libc_realloc(void *ptr, size_t size);
extern void *__libc_strdup(const char *str);

//#ifdef XMEM_MEMORY_LEAK_LOCATE


//////// Global XMEM specific information ////////

// lock to make xmem thread-safe.
pthread_mutex_t xmem_mutex = PTHREAD_MUTEX_INITIALIZER;
static char * xmem_timestamp_str = (char *) -1;
static unsigned int xmem_allocation_timestamp = 0;
static size_t xmem_allocation_highwater = 0; // The largest amount of memory we have allocated, 
//not including our header information.
static size_t xmem_allocation_total = 0;     // How much memory we have allocated now.

//
// This struct goes in front of all memory allocations done with leak
// checking enabled.
//
typedef unsigned long guard_word_t;

struct xmem_chunk {
  struct xmem_chunk * next;	// Pointer to the next element in the list.
  struct xmem_chunk * prev;	// Previous element in the list.
  size_t n_bytes;		// The number of bytes actually allocated.
  const char * filename;	// Name of the file that the allocation
				// occured from.  We don't actually copy the
				// filename because it's always passed from
				// a literal string anyway.
  int linenum;			// The line number that the allocation occured from.
  unsigned int timestamp;
                                // The data follows this structure (after possibly some 
                                // space to guarantee alignment).
};

#define XMEM_HASH_SIZE 9973
#define XMEM_HASH_MIN  16
struct xmem_hash_struct 
{
  unsigned int maxcount[XMEM_HASH_SIZE];
  unsigned int count[XMEM_HASH_SIZE];
  void **pointers[XMEM_HASH_SIZE];
} xmem_hash;

static int xmem_hash_init = 0;

static void xmem_hash_insert(void *ptr)
{
  unsigned int i,hash, newmax;

  pthread_mutex_lock( &xmem_mutex );

  if (xmem_hash_init == 0) 
  {
    for (i=0;i<XMEM_HASH_SIZE;i++)
    {
      xmem_hash.pointers[i] = __libc_malloc(XMEM_HASH_MIN*sizeof(void *));
      xmem_hash.count[i] = 0;
      xmem_hash.maxcount[i] = XMEM_HASH_MIN;
    }
    xmem_hash_init = 1;
  }

  //  return 1;
  hash = ((unsigned long) ptr) % XMEM_HASH_SIZE;
  if ( xmem_hash.count[hash]+1 > xmem_hash.maxcount[hash] )
  {
    newmax = 2*xmem_hash.maxcount[hash];
    xmem_hash.pointers[hash] = (void **)__libc_realloc(xmem_hash.pointers[hash],
						       newmax*sizeof(void *));
    xmem_hash.maxcount[hash] = newmax;
  }
  xmem_hash.pointers[hash][xmem_hash.count[hash]] = ptr;
  (xmem_hash.count[hash])++;

  pthread_mutex_unlock( &xmem_mutex );
}
/*    
static int xmem_hash_lookup(void *ptr)
{
  unsigned int i;
  unsigned long hash;

  hash = ((unsigned long) ptr) % XMEM_HASH_SIZE;
  for (i=0; i<xmem_hash.count[hash]; i++)
    if (xmem_hash.pointers[hash][i]==ptr)
      return 1;
  return 0;
}
*/

static int xmem_hash_remove(void *ptr)
{
  unsigned int i;
  unsigned long hash;

  pthread_mutex_lock( &xmem_mutex );  
  //  return 1;
  hash = ((unsigned long) ptr) % XMEM_HASH_SIZE;
  for (i=0; i<xmem_hash.count[hash]; i++)
  {
    if (xmem_hash.pointers[hash][i]==ptr)
    {
      for (; i<xmem_hash.count[hash]-1; i++)
	xmem_hash.pointers[hash][i] = xmem_hash.pointers[hash][i+1];
      (xmem_hash.count[hash])--;
      pthread_mutex_unlock( &xmem_mutex );
      return 1;
    }
  }
  pthread_mutex_unlock( &xmem_mutex );
  return 0;
}


#define ALIGNMENT 8		// Return memory blocks aligned on 8-byte
                                // boundaries.

// The actual amount of space the header takes up.
static const size_t xmem_chunk_size = (sizeof (struct xmem_chunk) + sizeof(guard_word_t) +ALIGNMENT-1) & ~(ALIGNMENT-1);


// Fixed bitpattern to stuff into the guard word.
static const guard_word_t guard_word_contents = 0xdeadbeef;

// The header information.  Initially the doubly-linked list is set up
// to contain no elements.
static struct xmem_chunk xmem_header = { &xmem_header, &xmem_header, 0, 0, 0, 0};

//#else
static int xmem_alloccount = 0;

//#endif // XMEM_MEMORY_LEAK_LOCATE


#if	__BYTE_ORDER == __BIG_ENDIAN
static unsigned char xmem_nan_value[8] = {0x7f,0xf7,0xff,0xff,0x7f,0xbf,0xff,0xff};
#else
static unsigned char xmem_nan_value[8] = {0xff,0xff,0xbf,0x7f,0xff,0xff,0xf7,0x7f};
#endif
/*
** ----------------------------------------------------------------------------------------------
*/

// Unlink previously allocated memory blocks from list, so they are ignored 
// when checking for memory leaks.
void xmem_reset(void)
{
  pthread_mutex_lock( &xmem_mutex );
  xmem_header.next = &xmem_header;
  xmem_header.prev = &xmem_header;
  xmem_header.n_bytes = 0;
  xmem_header.filename = 0;
  xmem_header.linenum = 0;
  xmem_header.timestamp = 0;
  pthread_mutex_unlock( &xmem_mutex );
}

void xmem_config(int memory_leak_locate, int trap_on_allocation, int replace_libc, 
		 int hang_on_out_of_mem, int report_length, int fill_with_nan, 
		 int assert_on_null_free, int warn_only)
{
  pthread_mutex_lock( &xmem_mutex );
  xmem_memory_leak_locate = memory_leak_locate;
  xmem_trap_allocation_timestamp = trap_on_allocation;
  xmem_replace_libc = replace_libc;
  xmem_hang_on_oum = hang_on_out_of_mem;
  xmem_report_length = report_length;
  xmem_fill_with_nan = fill_with_nan;
  xmem_assert_on_null_free = assert_on_null_free;
  xmem_warn_only = warn_only;
  pthread_mutex_unlock( &xmem_mutex );
}


void* xmem_domalloc_params(unsigned int size, const char *filename, int linenum)
{
  if (!xmem_memory_leak_locate)
  {
    void *ptr = (void *)__libc_malloc(size);
    if (ptr==NULL) 
    {
      fprintf(stderr,"XMEM: Malloc returned NULL. Probably out of memory. Aborting.\n");
      if (xmem_hang_on_oum)
      {
	hang(HANG_HOURS);
      }
      else 
	abort();
    }
    ++xmem_alloccount;
    if (xmem_fill_with_nan)
    {
      unsigned char *p1= (unsigned char *) ptr, *p2=xmem_nan_value;
      while (p1 < (unsigned char *)ptr+size)
      {
	if (p2 >= xmem_nan_value+8 )
	  p2 = xmem_nan_value;
	*p1++ = *p2++;
      }
    }			      
    xmem_hash_insert(ptr);
    return ptr;          // That was pretty easy.
  }
  else
  {
    void * user_mem, *ending_guard_word, *starting_guard_word;
    unsigned int bad_timestamp;
    struct xmem_chunk * mem_chunk;

    // Requested size, rounded up to the size of a guard word to avoid unaligned access:
    size_t actual_size = (size + sizeof(guard_word_t)-1) & ~(sizeof (guard_word_t)-1);  
    size_t request_size = xmem_chunk_size + // Size of header information.
      + actual_size			  // User data size.
      + sizeof (guard_word_t); // A guard word at the end to detect running
    // off the end of the array.

    if (xmem_trap_allocation_timestamp)
    {
      pthread_mutex_lock( &xmem_mutex );
      if ( xmem_timestamp_str == (char *) -1)
      {
	xmem_timestamp_str = getenv("XMEM_TRAP_ALLOCATION_TIMESTAMP");
      }
      if ( xmem_timestamp_str )
      {
	bad_timestamp = (unsigned int) atoi(xmem_timestamp_str);
	if (xmem_allocation_timestamp == bad_timestamp)
	  pthread_mutex_unlock( &xmem_mutex );
	ASSERT(xmem_allocation_timestamp != bad_timestamp);
      }
      pthread_mutex_unlock( &xmem_mutex );
    }

    // Allocate the memory.
    mem_chunk = (struct xmem_chunk *)__libc_malloc(request_size);
    if (mem_chunk == NULL) 
    {
      xmem_leakcheck();
      fprintf(stderr,"XMEM: Malloc returned NULL. Probably out of memory. Aborting.\n");
      if (xmem_hang_on_oum)
      {      
	hang(HANG_HOURS);
      }
      else 
	abort();
    }
    mem_chunk->n_bytes = size;
    mem_chunk->filename = filename;
    mem_chunk->linenum = linenum;
    mem_chunk->timestamp = xmem_allocation_timestamp++;
    user_mem = (char *)mem_chunk + xmem_chunk_size;  // Point to the block of memory we'll return.
    starting_guard_word =(char *)user_mem - sizeof(guard_word_t);
    ending_guard_word = (char *)user_mem + actual_size;  // Point to the end of the user memory.
    *(guard_word_t *)starting_guard_word = guard_word_contents;  // Put the guard word at the end.
    *(guard_word_t *)ending_guard_word = guard_word_contents;  // Put the guard word at the end.
  
    //
    // Put onto the linked list of memory pieces:
    //
    pthread_mutex_lock( &xmem_mutex );
    mem_chunk->prev = &xmem_header; // Put it right after the header.
    mem_chunk->next = xmem_header.next;
    mem_chunk->next->prev = mem_chunk;
    xmem_header.next = mem_chunk;

    // Keep some statistics about the totals:
    xmem_allocation_total += size;
    if (xmem_allocation_total > xmem_allocation_highwater)
      xmem_allocation_highwater = xmem_allocation_total;
    pthread_mutex_unlock( &xmem_mutex );

    if (xmem_fill_with_nan)
    {
      unsigned char *p1= (unsigned char *) user_mem, *p2=xmem_nan_value;
      while (p1 < (unsigned char *)user_mem+size)
      {
	if (p2 >= xmem_nan_value+8 )
	  p2 = xmem_nan_value;
	*p1++ = *p2++;
      }
    }			  

    xmem_hash_insert(user_mem);
    return user_mem;
  }
}


char *xmem_strdup_params(const char *str, const char *filename, int linenum)
{
  char *result;
  int len;
  len = strlen(str);
  result = xmem_domalloc_params(len+1, filename, linenum);
  memcpy(result,str,len+1);
  return result;
}

//
// Allocate and initialize memory to zero:
//
void *
xmem_calloc_params(size_t  n_elements, size_t size,
		   const char * filename, int linenum)
{
  void * mem = xmem_domalloc_params(n_elements*size, filename, linenum);
  memset(mem, 0, n_elements*size);
  return mem;
}

/*
** ----------------------------------------------------------------------------------------------
*/

void xmem_dofree(void* ptr)
{
  if ( xmem_assert_on_null_free )
  {
    ASSERT(ptr);
  }
  else if (ptr == 0)  
    return;

  if (!xmem_memory_leak_locate)
  {
    if ( xmem_hash_remove(ptr) )
    {
      pthread_mutex_lock( &xmem_mutex );      
      --xmem_alloccount;  
      pthread_mutex_unlock( &xmem_mutex );      
    }
    __libc_free(ptr);
  }
  else
  {
    if ( xmem_hash_remove(ptr) ) 
    {
      int gc;
      struct xmem_chunk * mem_chunk = (struct xmem_chunk *)
	((char *)ptr - xmem_chunk_size);	// Point to the header info.

      /* Check guard word. */
      gc = xmem_check_guardword(ptr);
      if (gc==1)
	fprintf(stderr,"Starting ");
      else if (gc==2)
	fprintf(stderr,"Ending ");
      else if (gc==3)
	fprintf(stderr,"Starting and ending");

      if (gc)
      {
	fprintf(stderr," guardword corrupted in chunk of %ld bytes at %#lx, "
		"allocated at %s:%d (timestamp %d)\n",
		(unsigned long)  mem_chunk->n_bytes,
		(unsigned long)((char *)mem_chunk + xmem_chunk_size),
		mem_chunk->filename,
		mem_chunk->linenum, mem_chunk->timestamp);
	printf("To find out where an unfreed chunk was allocated. "
	       "Do\n  setenv XMEM_TRAP_ALLOCATION_TIMESTAMP <timestamp>\n"
	       "where <timestamp> is one of the values given above and "
	       "re-run the code.\n");
	hang(HANG_HOURS);
      }

      pthread_mutex_lock( &xmem_mutex );      
      xmem_allocation_total -= mem_chunk->n_bytes;
      mem_chunk->next->prev = mem_chunk->prev;
      mem_chunk->prev->next = mem_chunk->next; // Unlink ourselves from the list.
      pthread_mutex_unlock( &xmem_mutex );      

      __libc_free(mem_chunk);
    }
    else
    {
      __libc_free(ptr);
    }
  }
}


int xmem_check_guardword(void *ptr)
{
  int res=0;
  struct xmem_chunk * mem_chunk = (struct xmem_chunk *)
    ((char *)ptr - xmem_chunk_size);	// Point to the header info.
  size_t actual_size = (mem_chunk->n_bytes + sizeof (guard_word_t)-1) &
    ~(sizeof (guard_word_t)-1);
  // Get the request size, rounded up to the
  // size of a guard word.
  void * ending_guard_word = (char *)ptr + actual_size;
  void * starting_guard_word = (char *)ptr-sizeof(guard_word_t);
  
  if (*((guard_word_t *)starting_guard_word) != guard_word_contents) 
    res = 1;
  if (*((guard_word_t *)ending_guard_word) != guard_word_contents)
    res += 2;
  return res;
}


/*
** ----------------------------------------------------------------------------------------------
*/

void* xmem_dorealloc_params(void* ptr, unsigned int size, const char *filename, int linenum)
{
  if (!xmem_memory_leak_locate)
  {
    if (ptr == NULL) // First time. Simply allocate new block.
      return xmem_domalloc_params(size, filename, linenum);
    else
    {
      xmem_hash_remove(ptr);
      ptr = (void *)__libc_realloc(ptr, size);
      xmem_hash_insert(ptr);
      if (ptr == NULL) 
      {
	fprintf(stderr,"XMEM: Realloc returned NULL. Probably out of memory. Aborting.\n");
	if (xmem_hang_on_oum)
	  hang(HANG_HOURS);
	else 
	  abort();
      }
      return ptr;
    }
  }
  else
  {
    if (ptr == 0)
      return xmem_domalloc_params(size, filename, linenum); // Allocating for the first time.
    else
    {
      void * user_mem, * ending_guard_word;
      // Point to the header info:
      struct xmem_chunk * mem_chunk = (struct xmem_chunk *)
	((char *)ptr - xmem_chunk_size);    
      // New requested size, rounded up to the size
      // of a guard word to avoid unaligned
      // access:
      size_t actual_size = (size + sizeof(guard_word_t)-1) &
	~(sizeof (guard_word_t)-1);
      
      size_t request_size = xmem_chunk_size + // Size of header information.
	+ actual_size		            // User data size.
	+ sizeof (guard_word_t);              // A guard word at the end to detect 
      // running off the end of the array.
      pthread_mutex_lock( &xmem_mutex );      
      mem_chunk->prev->next = mem_chunk->next;
      mem_chunk->next->prev = mem_chunk->prev;
      // Unlink ourselves from the list, in case
      // we are moved.
      xmem_allocation_total -= mem_chunk->n_bytes;
      pthread_mutex_unlock( &xmem_mutex );      

      // Get some more memory.
      mem_chunk = (struct xmem_chunk *)__libc_realloc(mem_chunk, request_size); 
      if (mem_chunk == NULL) 
      {
	// Ooops we are out of memory. Report leaks (probably the cause) and abort.
	// we can attach a debugger and s!
	xmem_leakcheck();
	fprintf(stderr,"XMEM: Realloc returned NULL. Probably out of memory. Aborting.\n");
	if (xmem_hang_on_oum)
	{
	  hang(HANG_HOURS);
	}
	else 
	  abort();
      }
      pthread_mutex_lock( &xmem_mutex );      
      mem_chunk->prev = &xmem_header; // Put ourselves back onto the list
      mem_chunk->next = xmem_header.next;	// right after the header.
      mem_chunk->next->prev = mem_chunk;
      xmem_header.next = mem_chunk;
      xmem_allocation_total += size;
      if (xmem_allocation_total > xmem_allocation_highwater)
	xmem_allocation_highwater = xmem_allocation_total;
      pthread_mutex_unlock( &xmem_mutex );      
		
      //
      // Set up a bit of user header information.  We don't need to set up
      // everything in the header since most of it was set on the initial call.
      //
      mem_chunk->n_bytes = size; // Remember the size.
      user_mem = (char *)mem_chunk + xmem_chunk_size;
      // Point to the block of memory we'll return.
      ending_guard_word = (char *)user_mem + actual_size;
      // Point to the end of the user memory.
      *(guard_word_t *)ending_guard_word = guard_word_contents;
      // Put the guard word at the end.

      xmem_hash_remove(ptr);
      xmem_hash_remove(user_mem);
      return user_mem;
    }
  }
}

/*
** ----------------------------------------------------------------------------------------------
*/

void xmem_leakreport(void)
{
  if (!xmem_memory_leak_locate)
  {
    fprintf(stderr, "XMEM: Number of unfreed memory buffers is %d.\n", xmem_alloccount);
  }
  else
  {
    fprintf(stderr, "XMEM: Largest amount of memory allocated was %lu.\n",
	    (unsigned long) xmem_allocation_highwater);
    fprintf(stderr,"XMEM: Total amount of unfreed memory is %lu.\n", 
	    (unsigned long) xmem_usage(stdout, xmem_report_length)); /* prints the first leaks on the list*/
  }
}


int xmem_leakcheck(void)
{
  if (!xmem_memory_leak_locate)
  {
    fprintf(stderr, "XMEM: Number of unfreed memory buffers is %d.\n",  xmem_alloccount);
    return (xmem_alloccount == 0);
  }
  else
  {
    fprintf(stderr, "XMEM: Largest amount of memory allocated was %lu.\n",
	     (unsigned long) xmem_allocation_highwater);
    fprintf(stderr,"XMEM: Total amount of unfreed memory is %lu.\n", 
	    (unsigned long) xmem_usage(stdout, xmem_report_length)); /* prints the first leaks on the list */
    return (xmem_header.next == &xmem_header);  // False if there's anything on the list.
  }
}

/*
** ----------------------------------------------------------------------------------------------
*/


/**
   Return the highest amount of memory allocated since the last time this
   function was called.

   This only works if you have XMEM_MEMORY_LEAK_LOCATE enabled.
*/
size_t xmem_recenthighwater()
{
  if (xmem_memory_leak_locate)
  {
    size_t recent_highwater;
    
    pthread_mutex_lock( &xmem_mutex );      
    recent_highwater = xmem_allocation_highwater;
    // Get the latest highwater mark.
    xmem_allocation_highwater = xmem_allocation_total;
    // Reset it for the next call.
    pthread_mutex_unlock( &xmem_mutex );      
    return recent_highwater;
  }
  else 
    return 0;
}



//
// Print out a list of all unfreed memory buffers.
// Arguments:
// 1) The file pointer to print out the summary.
// 2) The maximum number of things to print out.
//
//   This only works if you have XMEM_MEMORY_LEAK_LOCATE enabled.
size_t xmem_usage(FILE * fp, int maxlines)
{
  size_t unfreed = 0;
  if ( xmem_memory_leak_locate )
  {
    int numlines = 0;
    struct xmem_chunk * mem_chunk;
    
    if (xmem_header.next != &xmem_header)
    {
      if (fp)
	fprintf(fp, "XMEM: Unfreed memory:\n");
      
      pthread_mutex_lock( &xmem_mutex );      
      for (mem_chunk = xmem_header.next;
	   mem_chunk != &xmem_header;
	   mem_chunk = mem_chunk->next, numlines++)
      {
	unfreed += mem_chunk->n_bytes;
	if (fp)
	{
	  if ( numlines < maxlines )
	    fprintf(fp, "%ld bytes at %#lx, allocated at %s:%d "
		    "(timestamp %d)\n", (unsigned long)  mem_chunk->n_bytes,
		    (unsigned long)((char *)mem_chunk + xmem_chunk_size),
		    mem_chunk->filename,
		    mem_chunk->linenum, mem_chunk->timestamp);
	  else if ( numlines == maxlines )
	    fprintf(fp,". . .\nXMEM: List truncated. More than %d memory "
		    "blocks were not freed.\n",
		    maxlines);
	}
      }
      pthread_mutex_unlock( &xmem_mutex );      
      printf("To find out where an unfreed chunk was allocated. "
	     "Do\n  setenv XMEM_TRAP_ALLOCATION_TIMESTAMP <timestamp>\n"
	     "where <timestamp> is one of the values given above and "
	     "re-run the code.\n");
    }
  }
  return unfreed;
}



//
// Print out a list of all corrupted memory buffers.
// Arguments:
// 1) The file pointer to print out the summary.
// 2) The maximum number of things to print out.
//
//   This only works if you have XMEM_MEMORY_LEAK_LOCATE enabled.
int xmem_check_all_guardwords(FILE * fp, int maxlines)
{
  int corrupted = 0;
  char *ptr;
  struct xmem_chunk * mem_chunk;

  if (fp==NULL)
    return -1;

  pthread_mutex_lock( &xmem_mutex );      
  if ( xmem_memory_leak_locate && (xmem_header.next != &xmem_header))
  {
    
    for (mem_chunk = xmem_header.next;
	 mem_chunk != &xmem_header;
	 mem_chunk = mem_chunk->next)
    {
      if ( corrupted < maxlines )
      {
	ptr = ((char *)mem_chunk + xmem_chunk_size);
	if (xmem_check_guardword(ptr))	
	{ 
	  if (corrupted==0)
	    fprintf(fp, "XMEM: Corrupted memory:\n");
	  fprintf(fp, "Corrupted block of %ld bytes at %#lx, "
		  "allocated at %s:%d (timestamp %d)\n",
		  (unsigned long)  mem_chunk->n_bytes,
		  (unsigned long) ptr,
		  mem_chunk->filename,
		  mem_chunk->linenum, mem_chunk->timestamp);
	  corrupted++;
	}
      }
      else
      {
	fprintf(fp,". . .\nXMEM: List truncated. More than %d memory "
		"blocks were corrupted.\n", maxlines);
	break;
      }
    }
    if (corrupted>0)
    fprintf(fp,"To find out where a corrupted chunk was allocated. "
	    "Do\n  setenv XMEM_TRAP_ALLOCATION_TIMESTAMP <timestamp>\n"
	    "where <timestamp> is one of the values given above and "
	    "re-run the code.\n");
  }
  pthread_mutex_unlock( &xmem_mutex );      
  return corrupted;
}





#ifdef XMEM_REPLACE_LIBC
// Prototypes for LIBC functions.
extern void *__real_malloc(size_t size);
extern void *__real_calloc(size_t nmemb, size_t size);
extern void __real_free(void *ptr);
extern void *__real_realloc(void *ptr, size_t size);
extern void *__real_strdup(const char *str);

void *__wrap_malloc(size_t size)
{
  if (xmem_replace_libc)
    return xmem_domalloc_params(size, __FILE__, __LINE__);
  else
    return __real_malloc(size);
}

void *__wrap_calloc(size_t n_elements, size_t size)
{
  if (xmem_replace_libc)
    return  xmem_calloc_params(n_elements, size, __FILE__, __LINE__);
  else    
    return  __real_calloc(n_elements, size);
}

void __wrap_free(void *ptr)
{
  if (xmem_replace_libc)
    xmem_dofree(ptr); 
  else
    __real_free(ptr);
}

void *__wrap_realloc(void *ptr, size_t size)
{
  if (xmem_replace_libc)
    return xmem_dorealloc_params(ptr, size, __FILE__, __LINE__);
  else
    return __real_realloc(ptr, size);
}


char *__wrap_strdup(const char *str)
{
  if (xmem_replace_libc)
    return xmem_strdup_params(str, __FILE__, __LINE__);
  else
    return __real_strdup(str);
}


#endif
