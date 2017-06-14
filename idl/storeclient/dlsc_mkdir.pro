;+
;
; DLSC_MKDIR
;
; Create a directory in the storage manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  name       Name of the directory to create.
;
; OUTPUTS:
;  Result     'OK' if everything succeeded.
;
; USAGE:
;  IDL>res = dlsc_mkdir(token,'myresults')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_mkdir,token,name

compile_opt idl2
On_error,2

; Initialize the DL Storage global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(name) eq 0 then message,'name not input'

; Patch the names with the URI prefix if needed.
if strmid(fr,0,6) eq 'vos://' then nm=name else nm='vos://'+name

r = dlsc_getfromurl("/mkdir?file="+nm, token)

return,'OK'

end
