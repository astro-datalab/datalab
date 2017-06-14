;+
;
; DLSC_LN
;
; Delete a directory in the storage manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  fr         The source file.
;  target     The target file.
;
; OUTPUTS:
;  Result     'OK' if everything succeeded.
;
; USAGE:
;  IDL>res = dlsc_ln(token,'myresults')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_ln,token,fr,target

compile_opt idl2
On_error,2

; Initialize the DL Auth global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(fr) eq 0 then message,'fr not input'
if n_elements(target) eq 0 then message,'target not input'

; Patch the names with the URI prefix if needed.
; Patch the names with the URI prefix if needed.
if strmid(fr,0,6) eq 'vos://' then src=fr else src='vos://'+fr
if strmid(target,0,6) eq 'vos://' then dest=target else dest='vos://'+target

r = dlsc_getfromurl("/ln?from="+src+"&to="+dest, token)

return,'OK'

end
