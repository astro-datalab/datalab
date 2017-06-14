;+
;
; DLSC_CREATE
;
; Create a node in the store manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  name       The name of the node to create.
;  type       The type of node to create: container (directory),
;               data, link
;
; OUTPUTS:
;  Result     'OK' if everything succeeded.
;
; USAGE:
;  IDL>res = dlsc_create(token,'file.txt','important')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_create,token,name,tag

compile_opt idl2
On_error,2

; Initialize the DL Auth global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(name) eq 0 then message,'name not input'
if n_elements(tag) eq 0 then message,'tag not input'

; Patch the names with the URI prefix if needed.
if strmid(name,0,6) eq 'vos://' then nm=name else nm='vos://'+name

r = dlsc_getfromurl("/tag?file="+nm+"&tag="+tag, token)

return,'OK'

end
