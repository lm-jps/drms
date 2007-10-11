; IDL Startup Procedure for HMI

DEFSYSV, "!db_status", long(0)
DEFSYSV, "!dbi_path", ""
DEFSYSV, "!dbi_connected", 0
DEFSYSV, "!dbi_server", ""

;COMMON TDPPATH_COMMON, gcTDPPATH
;gcTDPPATH='../data/'

!dbi_server = GETENV("APIDIR")

;.run $APIDIR/drms
;.run $GPBDB/db/gpb_time
;.run $GPBDB/db/gpb_db
;.run $GPBDB/utilities/derive_lib
;.run $GPBDB/db/time_lib

; -- End of startup procedure
end
