;+
;
; DLAC_LISTPROFILES
;
; Retrieve the profiles supported by the storage manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  profile    Requested service profile string.
;  =format    The output format.  The default is "text".
;
; OUTPUTS:
;  profiles   A list of the names of the supported profiles or a
;              dictionary of the specific profile.
;
; USAGE:
;  IDL>profiles = dlsc_listprofiles(token,profile)
;
; By D. Nidever  June 2017, translated from storeClient.py
;-

function dlsc_listprofiles,token,profile,format

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'token not input'
; Defaults
if n_elements(format) eq 0 then format='text'

dburl = '/profiles?'
if n_elements(profile) gt 0 then if  profile ne 'None' and profile ne '' then $
  dburl += "profile="+profile+"&"
dburl += "format="+format

profiles = dlsc_getfromurl(dburl, token)
; Parse json
;   on some system this throws errors, not sure why
if strpos(profiles,'{') ne -1 then begin
  profiles = json_parse(profiles)
endif

; Get the profile
return,!dls.svc_profile

end
