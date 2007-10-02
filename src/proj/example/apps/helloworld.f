C The following string literals have a 'C' suffix.
C This causes the strings to be null-terminated.

      BLOCK DATA NAMENOTNEEDED

C Module name
        CHARACTER*127 mname
        COMMON / mname / mname
        DATA mname / 'helloworld' /

C List of module arguments.  These are provided as a long
C string, which is then parsed by jsoc_main.c  Each argument
C is contained within 928 bytes, which accounts for the 
C following fields:
C    CHARACTER*32 type
C    CHARACTER*128 name
C    CHARACTER*128 value
C    CHARACTER*512 description
C    CHARACTER*128 range

        CHARACTER*927 margs(128) 
        COMMON / margs / margs
        DATA margs(1) / 'string,recsin,"blahin",Input Data Records, 
     2na' /
        DATA margs(2) / 'string,seriesout,"blahout",Output Series, 
     2na' /
        DATA margs(3) / 'floats,error_paramf,"3.4",
     2Error suppression parameters - float,na' /
        DATA margs(4) / 'doubles,error_paramd,"5.85",
     2Error suppression parameters - double,na' /
        DATA margs(5) / 'end' /
      END

C Main DoIt routine
      INTEGER FUNCTION DOIT()
         INTEGER status
         INTEGER cpHandl
         CHARACTER*128 recsin
         INTEGER flagset
         INTEGER intvar
         BYTE int8var
         INTEGER*2 int16var
         INTEGER*4 int32var
         INTEGER*8 int64var
         REAL floatvar
         DOUBLE PRECISION doublevar
         BYTE bytevar
         INTEGER*2 shortvar
         CHARACTER*128 handle2
         CHARACTER*128 cpgethandle2
         INTEGER narr
         REAL arrval

         PRINT *, 'Hello World'
         call cpgethandle(cpHandl)

         handle2 = cpgethandle2()
         PRINT *, 'handle2 is ', handle2

         call cpgetstr(cpHandl, 'recsin', recsin, status)
         IF (status.EQ.0) THEN
            PRINT *, 'recsin', recsin
         END IF

         call cpflagset(cpHandl, 'f', flagset)
         PRINT *, 'flagset', flagset

         call cpgetint(cpHandl, 'int', intvar, status)
         IF (status.EQ.0) THEN
            PRINT *, 'intvar', intvar
         END IF

         call cpgetint8(cpHandl, 'int8', int8var, status)
         IF (status.EQ.0) THEN
            PRINT *, 'int8var', int8var
         END IF

         call cpgetint16(cpHandl, 'int16', int16var, status)
         IF (status.EQ.0) THEN
            PRINT *, 'int16var', int16var
         END IF

         call cpgetint32(cpHandl, 'int32', int32var, status)
         IF (status.EQ.0) THEN
            PRINT *, 'int32var', int32var
         END IF

         call cpgetint64(cpHandl, 'int64', int64var, status)
         IF (status.EQ.0) THEN
            PRINT *, 'int64var', int64var
         END IF

         call cpgetfloat(cpHandl, 'floatp', floatvar, status)
         IF (status.EQ.0) THEN
            PRINT *, 'floatvar', floatvar
         END IF

         call cpgetdouble(cpHandl, 'doublep', doublevar, status)
         IF (status.EQ.0) THEN
            PRINT *, 'doublevar', doublevar
         END IF

         call paramsgetchar(cpHandl, 'bytep', bytevar)
         PRINT *, 'bytevar', bytevar

         call paramsgetshort(cpHandl, 'shortp', shortvar)
         PRINT *, 'shortvar', shortvar

         call cpgetint(cpHandl, 'error_paramf_nvals', narr, status)
         PRINT *, 'narrf', narr

         call cpgetint(cpHandl, 'error_paramd_nvals', narr, status)
         PRINT *, 'narrd', narr

         call cpgetfloat(cpHandl, 
     2          'error_paramf_0_value', 
     3          arrval, 
     4          status)
         PRINT *, 'arrval0', arrval

         DOIT = 0
         RETURN
      END
