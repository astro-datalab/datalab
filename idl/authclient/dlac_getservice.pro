;+
;
; DLAC_GETSERVICE
;
; Return the currently-used Authentication Service URL.
;
; INPUTS:
;  None
;
; OUTPUTS:
;  profile   The currently-used Authentication service URL.
;
; USAGE:
;  IDL>svcurl = dlac_getservice()
;
; By D. Nidever  June 2017, translated from authClient.py
;-

function dlac_getservice

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Get the profile
return,!dla.svc_url

end
