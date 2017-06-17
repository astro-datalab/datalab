;+
;
; DLQC_SETSVCURL
;
; Set the query manager service URL.
;
; INPUTS:
;  svc_url    The service URL of the query manager to use.
;
; OUTPUTS:
;  None
;
; USAGE:
;  IDL>dlqc_setsvcurl,"http://dldemo.sdm.noao.edu:7002"
;
; By D. Nidever   June 2017, translated from queryClient.py
;-
 
pro dlqc_setsvcurl,svc_url

compile_opt idl2
On_error,2

; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Not enough inputs
if n_elements(svc_url) eq 0 then message,'svc_url not input'

!dlq.svc_url = svc_url

end
