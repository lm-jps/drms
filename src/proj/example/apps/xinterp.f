C *************************************************************************
      BLOCK DATA

C Module name
        CHARACTER*127 MNAME
        COMMON / MNAME / MNAME
        DATA MNAME / 'xinterp' /

        CHARACTER*927 MARGS(128)
        COMMON / MARGS / MARGS
	DATA MARGS (1) / 'string,fitfile,
     +/scr/rick/artrack/ar9901/fits/lat20.0Nlon249.0,
     +Name of file containing ring fits'/ 
        DATA MARGS (2) / 'string,out,su_arta.rd_test_intrpfits,
     +Dataseries for output (velocities, optional frequencies)'/ 
        DATA MARGS (3) / 'end' /
      END BLOCK DATA

      INTEGER FUNCTION DOIT()
	USE FDRMS 
        implicit real*8(a-h,o-z)
	integer params, status
        character*128 filn, filout, freqfil
	CHARACTER*256 umwelt, rec, series, key, filename, segment, segname
        CHARACTER*256 arrhdl
        INTEGER naxis
        INTEGER axis(2)
        CHARACTER, ALLOCATABLE, TARGET :: datav(:)
	CHARACTER*256 pathname

	CALL CPGETHANDLE (params)
	CALL CPGETSTR (params, 'fitfile', filn, status)
	CALL CPGETSTR (params, 'out', series, status)

	print *, 'series =', series
	print *, 'fitfile =', filn
	umwelt = F_DRMS_ENV_HANDLE()
C	print *, 'F_DRMS_ENV_HANDLE returned ', umwelt

	rec = F_DRMS_CREATE_RECORD (umwelt, series, DRMS_PERMANENT,
     +	    status)
	if (status .ne. 0) then
	  print *, 'F_DRMS_CREATE_RECORD returned ', rec, 'with status', status
	  print *, 'target series:', series
	  DOIT = 1
	  return
	endif
	key = 'FileName'
	filename = 'eight'
	status = F_DRMS_SETKEY_STRING (rec, key, filename)
C	print *, 'F_DRMS_SETKEY_STRING (',key,', ',filename,
C     +    ') returned ', status
	print *, 'F_DRMS_SETKEY_STRING (', filename, ') returned ', status
C	call  F_DRMS_PRINT_RECORD (rec)
	segname = 'Vfits'
	segment = F_DRMS_SEGMENT_LOOKUP (rec, segname)
	CALL  F_DRMS_SEGMENT_FILENAME (segment, pathname)
C	print *, 'F_DRMS_SEGMENT_FILENAME (',segname,') returned ', pathname
	print *, 'F_DRMS_SEGMENT_FILENAME returned ', pathname

	open (23, file=pathname, IOSTAT=status, STATUS='NEW')
        print *, 'open segfile returned ', status
	open (3, file='/tmp/xinterpfile')
	write (23, 100) -1000, 10000
	write (3, 100) -1000, 10000
100	format (2i6)
C	status = F_DRMS_SEGMENT_WRITE_FROM_FILE (segment, '/tmp/xinterpfile')
	status = F_DRMS_CLOSE_RECORD (rec, DRMS_INSERT_RECORD)
	print *, 'F_DRMS_CLOSE_RECORD returned ', status
C       test arrays
        naxis = 2
        axis(1) = 1024
        axis(2) = 256

        arrhdl = F_DRMS_ARRAY_CREATE_EMPTY(DRMS_TYPE_FLOAT, 
     +    naxis, axis(1), status)
        print *, 'F_DRMS_ARRAY_CREATE_EMTPY returned ', arrhdl
        call F_DRMS_FREE_ARRAY(arrhdl)
        print *, 'F_DRMS_FREE_ARRAY - handle is now ', arrhdl
	DOIT = 0
	RETURN
      end
