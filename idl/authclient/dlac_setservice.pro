;+
;
; DLAC_SETSERVICE
;
; Set the URL of the Authentication Service to be used.
;
; INPUTS:
;  svc_url  Authentication service base URL to call.
;
; OUTPUTS:
;  None
;
; USAGE:
;  IDL>dlac_setservice,"http://localhost:7001/"
;
; By D. Nidever  June 2017, copied from authClient.py
;-

pro dlac_setservice,svc_url

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(svc_url) eq 0 then message,'Service URL not supplied'

; Set the service
!dla.svc_url = svc_url

end
