;+
;
; DLQC_GETTIMEOUT
;
; Get the current sync query timeout value.
;
; INPUTS:
;  None
;
; OUTPUTS:
;  nsec       Current sync query timeout value in seconds.
;
; USAGE:
;  IDL>nsec = dlqc_gettimeout()
;
; By D. Nidever   June 2017, translated from queryClient.py
;-
 
function dlqc_gettimeout

compile_opt idl2
On_error,2

; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

return,!dlq.timeout_request

end
