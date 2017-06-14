;+
;
; DLAC_SETPROFILE
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
;  IDL>dlac_setprofile,"dev"
;
; By D. Nidever  June 2017, translated from authClient.py
;-

pro dlac_setprofile,profile

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(profile) eq 0 then message,'Profile not supplied'

; Set the profile
!dla.svc_profile = profile

end
