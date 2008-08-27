#! /bin/csh -f

if (! $?MACHINE) then
    set ARCH=UNKNOWN
    set MACHINE=UNKNOWN
    if (-x /bin/uname) then
	set os="`/bin/uname -s`"
	set ht="`/bin/uname -m`"
	set osht="$os,$ht"
	set or="`/bin/uname -r`"

	switch ("$osht")
	case SunOS,sun3*:
		set ARCH=SUN3
		breaksw
	case SunOS,sun4*:
        case SunOS,i86pc*:
		switch ("$or")
		case 4.*:
			set ARCH=SUN4
			set MACHINE=sun4
			breaksw
		case 5.*:
			set ARCH=SUN4SOL2
			set MACHINE=sol
			breaksw
		endsw
		breaksw
	case ULTRIX,RISC:
		set ARCH=PMAX
		set MACHINE=mips
		breaksw
	case ULTRIX,VAX:
		set ARCH=UVAX
		breaksw
	case AIX,*:
		set ARCH=RS6K
		breaksw
	case *,9000/*:
		set ARCH=HPPA
		breaksw
	case IRIX,*:
	case IRIX,*:
        if (`hostname` == shaman.Stanford.EDU) then
                set ARCH=SGI5_o32
                set MACHINE=sgi_o32
        else
		set ARCH=SGI5
		set MACHINE=sgi
        endif
		breaksw
        case IRIX64,*:
                set ARCH=SGI64
		set MACHINE=sgi4
		breaksw
        case Linux,*:
                set ARCH=LINUX
                set MACHINE=linux
                if (-f /etc/P4) then
                  set MACHINE=linux4
                endif
                if($ht == ia64) then
                  set MACHINE=linuxia64
                  set ARCH=LINUXIA64
                endif
                breaksw
	endsw
    endif
endif

echo $MACHINE
