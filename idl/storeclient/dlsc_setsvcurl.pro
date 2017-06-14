;+
;
; DLSC_SETSVCURL
;
; Set the storage manager service URL.
;
; INPUTS:
;  svc_url    The service URL of the storage manager to use.
;
; OUTPUTS:
;  None
;
; USAGE:
;  IDL>dlsc_setsvcurl,"http://dldemo.sdm.noao.edu:7003"
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
pro dlsc_setsvcurl,svc_url

compile_opt idl2
On_error,2

; Initialize the DL Auth global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(svc_url) eq 0 then message,'svc_url not input'

!dls.svc_url = svc_url

end
