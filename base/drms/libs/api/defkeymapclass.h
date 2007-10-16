/* defkeymapclass.h */

/* This is not a true header, but merely a method for 
 * statically storing KepMapClass tables statically.  
 * At run time, these are read into the appropriate containers. */

DEFKEYMAPCLASS(kKEYMAPCL_SSW, "ssw", "BUNIT\tbunit_ssw\n" \
	                             "BMAJ Bmajor_ssw\n" \
                                     "noexist,intKey3_ssw\n")

DEFKEYMAPCLASS(kKEYMAPCL_GNG, "gng", "LONPOLE\tlongitude_pole_gng\n" \
	                             "BUNIT\tbunit_gng\n" \
                                     "EQUINOX, intKey_gng\n")

