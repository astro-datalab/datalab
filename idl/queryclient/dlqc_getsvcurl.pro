;+
;
; DLQC_GETSVCURL
;
; Get the query manager service URL.
;
; INPUTS:
;  None
;
; OUTPUTS:
;  svc_url    Current Query Manager service URL.
;
; USAGE:
;  IDL>url = dlqc_getsvcurl()
;
; By D. Nidever   June 2017, translated from queryClient.py
;-
 
function dlqc_getsvcurl

compile_opt idl2
On_error,2

; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

return,!dlq.svc_url

end
