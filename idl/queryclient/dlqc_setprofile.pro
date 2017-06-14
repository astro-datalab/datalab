;+
;
; DLQC_SETPROFILE
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
;  IDL>dlqc_setprofile,"dev"
;
; By D. Nidever  June 2017, translated from queryClient.py
;-

pro dlqc_setprofile,profile

compile_opt idl2
On_error,2
  
; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Not enough inputs
if n_elements(profile) eq 0 then message,'Profile not supplied'

; Set the profile
!dlq.svc_profile = profile

end
