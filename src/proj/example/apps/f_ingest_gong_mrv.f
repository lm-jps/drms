C *************************************************************************

C The following string literals have a 'C' suffix.
C This causes the strings to be null-terminated.

      BLOCK DATA NAMENOTNEEDED

C Module name
        CHARACTER*127 mname
        COMMON / mname / mname
        DATA mname / 'DRMS_FORT_eg1' /

CC List of module arguments.  These are provided as a long
CC string, which is then parsed by jsoc_main.c  Each argument
CC is contained within 928 bytes, which accounts for the 
CC following fields:
CC    CHARACTER*32 type
CC    CHARACTER*128 name
CC    CHARACTER*128 value
CC    CHARACTER*512 description
CC    CHARACTER*128 range

        CHARACTER*927 margs(128)
        COMMON / margs / margs
        DATA margs(1) / 'string,filename,/home/igor/data/examples/mrvzi
     2051025t2111.fits, FITS File Name,na'/ 
        DATA margs(2) / 'end' /
      END BLOCK DATA NAMENOTNEEDED


      INTEGER FUNCTION DOIT()
         USE FDRMS 

         INTEGER cpHandl
         character*256 recordsetHdl, recordHdl, envHdl, arrayHdl, segHdl
         character*256 seriesname, segmentname, filename
         character*256 path
         CHARACTER(len=7000), ALLOCATABLE :: header(:) 
C         CHARACTER*7000 header

         integer status
         integer nOfRec, lifetime, rec_size, headlen, readraw, autoscale

         seriesname = 'nso_igor.gong_mrv_ex1'
         segmentname= 'short_image'
         nOfRec   = 1
         lifetime = DRMS_PERMANENT

CC get parameters handle
         call cpgethandle(cpHandl)

         print 81, 'cpHandl', cpHandl
CC get filename argument value
         call cpgetstr(cpHandl,'filename',filename,status)
         IF (status.EQ.0) THEN
            PRINT *, 'filename::', filename 
         END IF

CC Get the DRMS environment handle
         envHdl = f_drms_env_handle()
         print 80, 'drms_env', envHdl

CC Create a set of records for the given seriesname
         recordsetHdl = f_drms_create_records(envHdl, nOfRec,
     2                    seriesname, lifetime, status)

CC Check if handle is null
         if (f_isnull(recordsetHdl)) stop "Failed reading fits file ... 
     2exiting ..."
         print 80, 'recordsetHdl', recordsetHdl 

CC Get the first record from the record set
         recordHdl = f_get_rs_record(recordsetHdl,0);
         print 80, 'recordHdl', recordHdl

CC Check the path where SUMS will put your data
         call f_drms_record_directory(recordHdl,path,0)
         print 100, path

CC Check the size of your record
         rec_size = f_drms_record_size(recordHdl)
         print 110, rec_size

         write(*,'(1x,A)') 'Try opening the fits file: '

         readraw  = 1
CC *************
CC  NOTE: 
CC   when using f_drms_readfits there are two posibilities:
CC   1.- f_drms_readfits will allocate the header in C so
CC     do NOT double allocate in fortran. The "header" variable
CC     has to be of type allocatable in fortran though.
CC   2.- f_drms_readfits2 will not allocate in C. You'll need
CC     to pass a local variable of enough size
CC *************
CC Go and read a fits file using the drms_readfits function
C         arrayHdl = f_drms_readfits2(filename, readraw, headlen,
C     2                header, status)
CC Allocatable version
C
C  XXX For Art to complete
C  drms_readfits() is not a an external API.  Use drms_segment_read().
C  
C  -- Art Amezcua 8/14/2007
C
C         arrayHdl = f_drms_readfits(filename, readraw, headlen,
C     2                header, status)


CC Check for nulls
         if (f_isnull(arrayHdl)) stop "Failed reading fits file ... 
     2exiting ..."

         print 80, 'arrayHdl', arrayHdl
         print 95, status
         print 95, headlen 
C         print 90, header(0:80)
CC now that we loaded the fits file and it's contents is in 
CC a valid arrayHdl lets ingest it in DRMS.

CC lets get the segment first from our record
         segHdl = f_drms_segment_lookup(recordHdl, segmentname);

         print 80, 'segHdl', segHdl

CC  Don't autoscale
         autoscale = 0;

CC  write array to segment
         status = f_drms_segment_write(segHdl, arrayHdl, autoscale);

CC We don't need the arrayHdl anymore so lets free it's memory
         call f_drms_free_array(arrayHdl);

CC Set the series keywords using a C interface
         call f_gong2drms_set_keywords(recordHdl, headlen, header);

CC        insert records
         status = f_drms_insert_records(recordsetHdl);

         print 120, status 
CC        free records. If records are not freed the drms_close will attempt
CC        a drms_insert_records after return from the DOIT subroutine if the
CC        records structure is not empty.
CC        This will result in a DB error trying to insert a duplicate key
CC        In other words use drms_insert_records and drms_free_records or none
         call f_drms_free_records(recordsetHdl);

         print*, "About to leave the module ..."
         DOIT = 0
         RETURN
81       format ('handle : type=', a30, ';value=' i8)
80       format ('handle : type=', a30, ';value=' a50)
90       format('header ::', a80)
95       format('status ::', i8)
100      format('drms directory path :', a50)
110      format('record size:', i8)
120      format("insert status [",i8,"]")

      END FUNCTION DOIT
