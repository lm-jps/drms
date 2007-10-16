//
// This module contains code to enable and disable floating point exceptions
// and to control floating point precision.
//
// Gary Holt  17 Aug 2001.
//

#if !defined(__CYGWIN__)		// I don't know how to do this under cygwin.
#if defined(USE_OLD_GLIBC_WAY)
#include <fpu_control.h>
#include <fenv.h>
#elif defined(_MSC_VER)			// MSC doesn't yet support the C99 standard
#include <float.h>				// routines for doing this.
#else
#define _GNU_SOURCE				// Need GNU extensions to enable/disable
								// the exceptions.
#include <fenv.h>
#endif

//
// We store the exception state in a stack, so a routine can temporarily
// disable exceptions without knowing the previous state, and without
// affecting other routines.
//
#define EXCEPTION_STACK_SIZE 5
static int exception_stack_idx = 0;

#if defined(USE_OLD_GLIBC_WAY)
static fpu_control_t exceptions_flag_stack[EXCEPTION_STACK_SIZE];
const fpu_control_t exception_bits = // See fpu_control.h for definitions.
  (_FPU_MASK_IM |			// Invalid operation
   _FPU_MASK_DM |			// Denormalized operand (|x| < 1e-38 for floats)
   _FPU_MASK_ZM |			// Divide by zero
   _FPU_MASK_OM |			// Overflow
//   _FPU_MASK_UM			// Underflow
   );
#elif defined(_MSC_VER)
static unsigned int exceptions_flag_stack[EXCEPTION_STACK_SIZE];
const unsigned int exception_bits =
  _EM_INVALID |			// Invalid operation
//  _EM_DENORMAL |		// Denormalized operand (|x| < 1e-38 for floats)
// Turning this on seems to cause crashes for no good reason, when floats
// aren't even close to the limits.
  _EM_ZERODIVIDE |		// Divide by zero
  _EM_OVERFLOW			// Overflow
//  _EM_UNDERFLOW		// Underflow (nonzero number forced to become 0).
;
#else
static int exceptions_flag_stack[EXCEPTION_STACK_SIZE];
const int exception_bits = FE_DIVBYZERO /* | FE_UNDERFLOW */ | FE_OVERFLOW | FE_INVALID;
					// Control all supported exceptions at once.
#endif

//
// Enable or disable exceptions, remembering the old state.
// Arguments:
// 1) True if you want to enable exceptions.
//
void
fpu_exception_push_and_set(int enable_flag)
{
#if defined(USE_OLD_GLIBC_WAY)
	fpu_control_t fpu_control_bits;


	_FPU_GETCW(fpu_control_bits); // Get the current bits.
	exceptions_flag_stack[exception_stack_idx++] = fpu_control_bits;
							// Store it so we can restore it.
	if (enable_flag)
		fpu_control_bits &= ~ exception_bits;
					// Clearing these bits enables traps on all
					// kinds of errors and also whenever floating
					// point numbers become excessively small
					// (denormalized).
	else
		fpu_control_bits |= exception_bits;

	_FPU_SETCW(fpu_control_bits); // Set the new state.
#elif defined(_MSC_VER)
	unsigned int fpu_control_bits = _control87(0, 0); // Get the current state.
	exceptions_flag_stack[exception_stack_idx++] = fpu_control_bits;

	if (enable_flag)
	{
		_clear87();		   // Don't trap now if an exception occured while
						// trapping was disabled.
		_control87(0, exception_bits); // Turning off the bits enables exceptions.
	}
	else
		_control87(exception_bits, exception_bits);
#else
	// With C99 standard routines + GNU extensions:
	exceptions_flag_stack[exception_stack_idx++] = 
		(fegetexcept() == exception_bits);
						// Store the old exception state.
	if (enable_flag)
	{
		feclearexcept(exception_bits); // Clear the exceptions before enabling
						// traps, because a trap will be triggered
						// immediately if there was an exception a
						// while ago.
		feenableexcept(exception_bits);
	}
	else
		fedisableexcept(exception_bits);
#endif
//	DEBUG_ASSERT(exception_stack_idx < EXCEPTION_STACK_SIZE);

}

//
// Restore the last exception state (i.e., turn off exceptions if you just
// enabled them, or vice versa).
//
void
fpu_exception_pop(void)
{
//	DEBUG_ASSERT(exception_stack_idx > 0);
#if defined(USE_OLD_GLIBC_WAY)
	feclearexcept(FE_ALL_EXCEPT); // Clear the exceptions before enabling
	                              // traps, because a trap will be triggered
					// immediately if there was an exception a
					// while ago.
	_FPU_SETCW(exceptions_flag_stack[--exception_stack_idx]);
#elif defined(_MSC_VER)
	_clear87();	// Don't trap now if an exception occured while
			// trapping was disabled.

	_control87(exceptions_flag_stack[--exception_stack_idx], exception_bits);
#else
	if (exceptions_flag_stack[--exception_stack_idx])
	{
		feclearexcept(exception_bits); // Clear the exceptions before enabling
						// traps, because a trap will be triggered
						// immediately if there was an exception a
						// while ago.
		feenableexcept(exception_bits);
	}
	else
		fedisableexcept(exception_bits);
#endif
}

#else  // For systems that don't support this at all:
void
fpu_exception_push_and_set(int enable_flag)
{
}

void
fpu_exception_pop(void)
{
}
#endif

float dummy_func(float x) { return x; }
                                // A dummy function that's used to force the
                                // compiler to get a floating point value out
                                // of a register and into 32 bit precision.
                                // This is an attempt to avoid differences 
                                // between the encoder and decoder due to
                                // differing roundoff because optimized code
                                // has organized the computation differently.

