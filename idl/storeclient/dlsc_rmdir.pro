;+
;
; DLSC_RMDIR
;
; Delete a directory in the storage manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  name       Name of the directory to delete.
;
; OUTPUTS:
;  Result     The directory is deleted.
;
; USAGE:
;  IDL>res = dlsc_rmdir(token,'myresults')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_rmdir,token,name

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
if nm eq 'vos://' or nm eq 'vos://tmp' or nm eq 'vos://public' then $
   return,'Error: operation not permitted'

dum = dlsc_saveas(token, "deleted", nm+"/.deleted")
r = dlsc_getfromurl("/rmdir?dir="+nm, token)

return,'OK'

end
