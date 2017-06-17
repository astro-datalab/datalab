;+
;
; DLSC_GETPROFILE
;
; Get the requested service profile.
;
; INPUTS:
;  None
;
; OUTPUTS:
;  profile   The currently requested service profile.
;
; USAGE:
;  IDL>profile = dlsc_getprofile()
;
; By D. Nidever  June 2017, translated from storeClient.py
;-

function dlsc_getprofile

compile_opt idl2
On_error,2
  
; Initialize the DL Storage global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Get the profile
return,!dls.svc_profile

end
