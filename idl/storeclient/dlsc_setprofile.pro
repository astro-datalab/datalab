;+
;
; DLSC_SETPROFILE
;
; Set the requested service profile.
;
; INPUTS:
;  profile  Requested service profile string.
;
; OUTPUTS:
;  None
;
; USAGE:
;  IDL>dlsc_setprofile,"dev"
;
; By D. Nidever  June 2017, translated from storeClient.py
;-

pro dlsc_setprofile,profile

compile_opt idl2
On_error,2
  
; Initialize the DL Storage global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(profile) eq 0 then message,'Profile not supplied'

; Set the profile
!dls.svc_profile = profile

end
