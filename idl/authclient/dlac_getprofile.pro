;+
;
; DLAC_GETPROFILE
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
;  IDL>profile = dlac_getprofile()
;
; By D. Nidever  June 2017, translated from authClient.py
;-

function dlac_getprofile

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Get the profile
return,!dla.svc_profile

end
