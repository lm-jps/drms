C *************************************************************************
C The following string literals have a 'C' suffix.
C This causes the strings to be null-terminated.

      BLOCK DATA NAMENOTNEEDED

C Module name
        CHARACTER*127 mname
        COMMON / mname / mname
        DATA mname / 'DRMS_FORT_eg2' /

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
        DATA margs(1) / 'string,seriesname,nso_igor.gong_mrv_ex1,
     2Series Name,na' /
        DATA margs(2) / 'end' /
      END BLOCK DATA NAMENOTNEEDED


      INTEGER FUNCTION DOIT()
         USE FDRMS 

         INTEGER cpHandl
         character*256 orig_rsHdl, clone_rsHdl, recHdl, siHdl, envHdl,
     2segHdl, hcHdl, segsHcHdl, arrHdl
         character*256 seriesname, segmentname, rs_query
         character(len=FHANDLEKEYSIZE) si_kw(DRMS_MAXPRIMIDX)
         character*256 path

         integer autoscale, status
         integer I, rec_num, total_pixels, axis(DRMS_MAXRANK)
         integer*2, ALLOCATABLE, TARGET :: dataV(:)

         rs_query   = "nso_igor.gong_mrv_ex1[?datetime__obs=909349908?]"

         segmentname= 'short_image'

CC       Get the DRMS Arguments handle
CC       The arguments handle is of type integer
CC       This is an exception as in this interface handles are of type
CC       CHARACTER*256 and contain information about the underlying C
CC        and function it was called from.
         call cpgethandle(cpHandl)

         print 81, 'cpHandl', cpHandl
CC       Get Arguments values
         call cpgetstr(cpHandl,'seriesname',seriesname,status)
         IF (status.EQ.0) THEN
            PRINT *, 'seriesname::', seriesname 
         END IF

CC       Get the DRMS environment handle
CC       Note that handles are of type CHARACTER*256
         envHdl = f_drms_env_handle()
         print 80, 'drms_env', envHdl

C Open a set of drms records for the given query set 
         orig_rsHdl = f_drms_open_records(envHdl, rs_query, status)
C check the returned status
         IF (status.NE.0) THEN
            PRINT 200, 'drms_open_records', status
         END IF
C This is how to check for null values
         if (f_isnull(orig_rsHdl)) stop "Failed opening records ... 
     2exiting ..."
         print 80, 'orig_rsHdl', orig_rsHdl 

C Clone Records
         clone_rsHdl = f_drms_clone_records(orig_rsHdl, DRMS_PERMANENT,
     2DRMS_COPY_SEGMENTS, status)
C Check if clone record is null
         if (f_isnull(clone_rsHdl)) stop "Failed cloning records ... 
     2exiting ..."
         print 80, 'clone rsHdl', clone_rsHdl 
C Get the original number of records in the record set
C Disabled f_get_rs_num() because it always returns 1.
C        rec_num = f_get_rs_num(orig_rsHdl)
         rec_num = 1

         print 81, "original record", rec_num
C Get the cloned number of records in the record set
C        rec_num = f_get_rs_num(clone_rsHdl)
         rec_num = 1
         print 81, "clone record", rec_num

C Now loop through the records in the record set
         I=0
         LOOP_OUT : DO WHILE (I .LT. rec_num)
           print 10, I, rec_num
C Get the record 'I' from the record set
           recHdl = f_get_rs_record(clone_rsHdl,I)
C Print the record to the STDOUT
C           call f_drms_print_record(recHdl)
C Get the record series info
           siHdl= f_get_record_seriesinfo(recHdl)
C Print the series name for that record
           print 20, f_get_si_seriesname(siHdl),
     2               f_get_record_recnum(recHdl)
C Get a container handle for all the series info keywords
           call f_get_si_keyword_handleV(si_kw, siHdl)
C print a couple of keywords
           print 30, si_kw(1) 
           print 30, si_kw(2) 
CC Get segment containers           
           segsHcHdl = f_get_record_segments(recHdl)
           print 80, 'segsHdl', segsHcHdl 
CC create a iterator  on the HContainer
           hcHdl  = f_new_hiterator(segsHcHdl)
           print 80, 'hcHdl', hcHdl 
C Now loop through the segments within the record
           LOOP_IN : DO WHILE (1 .EQ. 1)
CC   Get the next available segment
             segHdl = f_hiterator_getnext(hcHdl)
             print 80, 'segHdl', segHdl 
CC   If null handle means no more segments, then quit
             if (f_isnull(segHdl)) exit LOOP_IN

             arrHdl = f_drms_segment_read(segHdl, DRMS_TYPE_RAW, status)
CC           Read array axis and calculate total dimensions
             call f_get_array_axis(axis, arrHdl)
             total_pixels = axis(1)*axis(2)
             print 90, axis(1), axis(2), total_pixels
CC get the array data from the segment
CC ***************
CC IMPORTANT NOTE: 
CC   Note that despite dataV being of type allocatable hasn't been allocated
CC   in fortran at all.
CC   The f_get_array_data_<type> set of functions already do the allocation
CC   for us in C.
CC   In principle there is no need to deallocate the data array as the
CC   f_drms_free_records(<result set handle>) function would take care of it.
CC   If for whatever reason there is a need to dealocate the array
CC   you can always use f_drms_free_array(<array handle>)
CC ***************
CC  
             call f_get_array_data_byte(dataV,arrHdl)
CC           Print first three elements from the data buffer
             print 100, dataV(1)
             print 100, dataV(2)
             print 100, dataV(3)
             print 100, dataV(2097)
CC   Now lets do something on the data itself. The below fortran subroutine
CC   will draw a cross on the the data image
             call DRAW_A_CROSS(dataV, axis(1))
CC   Now lets write back the array into the segment
CC    Don't autoscale
            autoscale = 0;
            status = f_drms_segment_write(segHdl, arrHdl, autoscale);

           END DO LOOP_IN
CC         Destroy the iterator
           call f_destroy_hiterator(hcHdl)
           I=I+1
CC Check out in what directory the segment was written to
         call f_drms_record_directory(recHdl,path,0)
         print 110, path
         END DO LOOP_OUT

CC  INSERT RECORD AGAIN
         status = f_drms_insert_records(clone_rsHdl);
         print 120, status

CC If you need to close the records
C         status = f_drms_close_records(clone_rsHdl,DRMS_FREE_RECORD)

CC free the records
         call f_drms_free_records(clone_rsHdl);

10       format("[", i8 "] of  [", i8, "] total records")
20       format("seriesname [",a50,"]; record number [", i8, "]")
30       format("SI keyword handle [",a50,"]")
81       format ('handle : type=', a30, ';value=' i8)
80       format ('handle : type=', a30, ';value=' a50)
90       format ('Axis1 [',i8,'] ; Axis2 [', i8, '] ; total pixels [',
     2 i8,']') 
100      format ('data array [', i8, ']')
110      format('drms directory path :', a50)
120      format("insert status [",i8,"]")
200      format("function [", a30, "] failed with status [",i8,"]")
300      format("##### !!!!! #### DRMS SERVER PID [",i8,"]")
         DOIT = 0
         RETURN
      END FUNCTION DOIT

      INTEGER FUNCTION AINDEX( AXIS, X, Y)
        INTEGER AXIS, X, Y

        AINDEX = X + (Y * AXIS)

        IF (X .GT. AXIS .OR. Y .GT. AXIS ) THEN
          stop "Error image index outbout"
        END IF
        RETURN
      END FUNCTION AINDEX

      SUBROUTINE DRAW_A_CROSS(ARRAY, AXIS)
        INTEGER*2, ALLOCATABLE :: ARRAY (:)
        INTEGER AXIS,Y,X
        INTERFACE
          INTEGER FUNCTION AINDEX(A,B,C)
            INTEGER A,B,C
          END FUNCTION
        END INTERFACE

        print 10, ARRAY(1)
        print 10, ARRAY(2)
        print 10, ARRAY(3)
        print 10, ARRAY(2097)
        Y = 1
        X = AXIS/2
        DO WHILE (Y .LT. AXIS)
          Y = Y + 1
          ARRAY(AINDEX(AXIS, X, Y)) = 1400
        END DO

        X = 1
        Y = AXIS/2
        DO WHILE (X .LT. AXIS)
          X = X + 1
          ARRAY(AINDEX(AXIS, X, Y)) = 1400
        END DO
10      format ('DRAW_A_CROSS data array [', i8, ']')
20      format ('index [', i48, ']')
30      format ('X [', i48, ']')
35      format ('AXIS =', i8, '; Y = ', i8 ,'; X = ', i8)
      END SUBROUTINE DRAW_A_CROSS

      SUBROUTINE ZERO(ARRAY, TSIZE)
        INTEGER*2, ALLOCATABLE :: ARRAY (:)
        INTEGER TSIZE
        DO WHILE (I .LT. TSIZE)
          I = I + 1
          ARRAY(I) = 0
        END DO
      END SUBROUTINE ZERO
