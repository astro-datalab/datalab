;+
;
; DLSC_LOAD
;
; Load a file from a remote endpoint to the store manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  name       The name of the output file in VOSpace.
;  endpoint   The URL to download to VOSpace.
;
; OUTPUTS:
;  Result     'OK' if everything succeeded.
;
; USAGE:
;  IDL>res = dlsc_load(token,'file.txt','http://www.google.com')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_load,token,name,endpoint

compile_opt idl2
On_error,2

; Initialize the DL Storage global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(name) eq 0 then message,'name not input'
if n_elements(endpoint) eq 0 then message,'endpoint not input'

; Patch the names with the URI prefix if needed.
if strmid(name,0,6) eq 'vos://' then nm=name else nm='vos://'+name

r = dlsc_getfromurl("/load?name="+nm+"&endpoint="+endpoint, token)

return,'OK'

end
