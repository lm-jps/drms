;******************************************************************************
; DRMS.pro - Procedures for interacting with the HMI/AIA JSOC database        *
; Last Update: 10 Sept 2007 - J. Spencer                                       *
;******************************************************************************

;****************************************************************************
;*            Routines for Beginning and Ending a Database Session          *
;****************************************************************************

PRO DB_CONNECT, LOGIN=login, PASSWORD=password, DBNAME=dbname, HOST=host, SESSION=session
  
; DB_CONNECT: Connect to the jsoc databases 

; This procedure must be called prior to any other calls to the 
; database. In addition to making a connection, it sets up the logical
; path to the shared library that contains the C routines for accessing
; the database
; dbi_connected returns 1 if user connects

  ON_ERROR, 2 

  ; Set up the logical path to the shared library of database access routines

  !dbi_path = GETENV ("JSOCROOT") + "/lib/" + GETENV("JSOC_MACHINE") + "/"

  if !dbi_path eq "" then begin

    ; The required logical pathname has not been defined.
    ; Print an error message and return
    
  MESSAGE, "Can't locate JSOC DB routines: make sure environment variables JSOCROOT and JSOC_MACHINE are defined"

   endif else !dbi_path = !dbi_path + "libidl.so"

  ; Get the user name and password 
  if KEYWORD_SET (LOGIN) then login = login $
    else login = 0 
;PRINT, "LOGIN: ", login

  if KEYWORD_SET (PASSWORD) then password = password $
     else password = 0

  ; Set to query the proper database server
  if KEYWORD_SET (DBNAME) then dbname = dbname $
    else dbname = 0

  if KEYWORD_SET (HOST) then host = host $
    else host = 0

  if KEYWORD_SET (SESSION) then session = session $
    else session = 0 

PRINT, login, password, host, dbname, session

;  if KEYWORD_SET (HOST) then PRINT, "   REQ_HOST = ",host
;  PRINT, "   ACT_HOST = ",host


  ; Convert the name, password and server name into char strings for
  ; use by C routines

  name_data     = BYTARR (17)
  password_data = BYTARR (17)
  host_data     = BYTARR (50)
  dbname_data   = BYTARR (17)
  session_data  = BYTARR (17)

  nchars = STRLEN (login)
  if nchars NE 0 then name_data(0:nchars-1) = BYTE (login) $
  else name_data = NULL

  nchars = STRLEN (password)
  if nchars NE 0 then  password_data(0:nchars-1) = BYTE (password) $
  else  password_data = NULL

  nchars = STRLEN (host)
  if nchars NE 0 then host_data(0:nchars-1) = BYTE (host) $
  else host_data = NULL
 
  nchars = STRLEN (dbname)
  if nchars NE 0 then dbname_data(0:nchars-1) = BYTE (dbname) $
  else dbname_data = NULL
 
  nchars = STRLEN (session)
  if nchars NE 0 then session_data(0:nchars-1) = BYTE (session) $
  else session_data = NULL
  
  ;PRINT, "Got to just before CALL_EXTERNAL function"

  !db_status = 0

  PRINT, !db_status
  !db_status = CALL_EXTERNAL (!dbi_path, "idl_db_connect", $ 
                             name_data, password_data, host_data, dbname_data, session_data)
  PRINT, !db_status

  if !db_status gt 0 then !dbi_connected = 1
 
  return

end


; DB_DISCONNECT: Disconnect from the JSOC database

; This procedure should be called when all database activity is complete.
; Any open cursors (used by pairs of select/get routines) are closed
;
; Disconnect from jsoc expects input dbstatus = 1 to abort/rollback, 0 to commit
; !db_status = 1, !dbi_connected=0 when user is disconnected

PRO DB_DISCONNECT, dbstatus

  ON_ERROR, 2
  !dbi_path = GETENV ("JSOCROOT") + "/lib/" + GETENV("JSOC_MACHINE") + "/"
  if !dbi_path eq "" then begin
    ; The required logical pathname has not been defined.
    ; Print an error message and return

  MESSAGE, "Can't locate JSOC DB routines: make sure environment variables JSOCROOT and JSOC_MACHINE are defined"

   endif else !dbi_path = !dbi_path + "libidl.so"

; Do C variable declaration
  dbstatus_data  = LONG (dbstatus)

PRINT, dbstatus_data

  if !dbi_connected then begin
    !db_status = CALL_EXTERNAL (!dbi_path, "idl_db_disconnect", dbstatus_data)

    if !db_status gt 0 then !dbi_connected = 0
  endif
  return

end


;******************************************************************************
;                      JSOC Database Access Routines                          *
;******************************************************************************
; DRMS_OPEN_RECORDS: Retrieve records from the jsoc drms
;
; Parameters:
;
; series_name   - JSOC series name for requested handle 
;
; Outputs:
;  
; Handle from C
;
; Upon return the global variable !db_status is set as follows:
;
; < 0 - An error occurred during the database query
; = 0 - No (more) packets available
; > 0 - Number of packets returned in this call (up to nmax).  If
;       !db_status = nmax, there may be more packets to be read --
;       call this routine again to get them

PRO DRMS_OPEN_RECORDS, series_name

  ON_ERROR, 2

  if not !dbi_connected then $
    MESSAGE, "You're not connected to the JSOC database: call DB_CONNECT first"

PRINT, "You requested series: ", series_name

; Do C variable declaration
  jsoc_series  = BYTARR (64)
  handle    = BYTARR (64)
  buffer    = LONG(64)

  nchars = STRLEN(series_name)
  if nchars NE 0 then jsoc_series(0:nchars-1) = BYTE (series_name) $
  else jsoc_series = NULL

  ; Check for the existence of data

  !db_status = CALL_EXTERNAL (!dbi_path, "idl_drms_open_records", $
                              jsoc_series, handle, buffer)

  if !db_status gt 0 then begin
    PRINT, "  handle returned: ", STRING(handle)
  endif
  return
end

; -- End of file
