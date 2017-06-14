;+
;
; DLSC_SAVEAS
;
; Save the string representation of a data object as a file.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  data       The data to save.
;  name       The name of the output file in VOSpace.
;
; OUTPUTS:
;  Result     'OK' if everything succeeded.
;
; USAGE:
;  IDL>res = dlsc_saveas(token,data,'file.txt')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_saveas,token,data,name

compile_opt idl2
On_error,2

; Initialize the DL Auth global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(data) eq 0 then message,'data not input'
if n_elements(name) eq 0 then message,'name not input'

; Write to a temporary file
tfd = mktemp('dl',error=error)
if n_elements(error) gt 0 then message,error
openw,unit,/get_lun,tfd
printf,unit,data
close,unit
free_lun,unit

; Patch the names with the URI prefix if needed.
if strmid(name,0,6) eq 'vos://' then nm=name else nm='vos://'+name

; Put the temp file to the VOSpace.
res = dlsc_put(token, fr=tfd, to=nm, verbose=0)

; Delete the temporary file
file_delete,tfd,/allow

return,'OK'

end
