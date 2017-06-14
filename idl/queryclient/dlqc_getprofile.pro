;+
;
; DLQC_GETPROFILE
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
;  IDL>profile = dlqc_getprofile()
;
; By D. Nidever  June 2017, translated from queryClient.py
;-

function dlqc_getprofile

compile_opt idl2
On_error,2
  
; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Get the profile
return,!dlq.svc_profile

end
