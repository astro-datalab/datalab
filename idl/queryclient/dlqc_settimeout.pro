;+
;
; DLQC_SETTIMEOUT
;
; Set the requested sync query timeout value (in seconds).
;
; INPUTS:
;  nsec       The number of seconds requested before a sync query timeout occurs.
;             The service may cap this as a server defined maximum.
;
; OUTPUTS:
;  None
;
; USAGE:
;  IDL>dlqc_settimeout,120
;
; By D. Nidever   June 2017, translated from queryClient.py
;-
 
pro dlqc_settimeout,nsec

compile_opt idl2
On_error,2

; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Not enough inputs
if n_elements(nsec) eq 0 then message,'nsec not input'

!dlq.timeout_request = nsec

end
