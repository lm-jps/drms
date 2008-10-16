#! /bin/csh -f
if ( ! $?JSOC_MACHINE ) then

  # Get host name, OS and CPU.
  set OS = `uname -s`
  switch ("$OS")
    case "Linux*":
      set CPU = `uname -m`
      breaksw
    case "Darwin":
      set CPU = `uname -p`
      breaksw
    default:
      set CPU = `uname -p`
      breaksw
  endsw

  if ( $OS == "Linux" ) then
    switch ("$CPU")
    case "i686":
    case "i386":
    case "ia32":
      echo linux_ia32
      breaksw
    case "ia64":
      echo linux_ia64
      breaksw
    case "x86_64":
    case "em64t":
      echo linux_x86_64
      breaksw
    default:
      echo custom
      breaksw
    endsw
  else if ( $OS == "Darwin" ) then
    switch ("$CPU")
    case "powerpc":
      echo mac_osx_ppc
      breaksw
    case "i386":
      echo mac_osx_ia32
      breaksw
    default:
      echo custom
    endsw
  else
     echo custom
  endif

else
    echo $JSOC_MACHINE
endif
