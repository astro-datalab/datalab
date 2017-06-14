;+
;
; DLSC_GETFROMURL
;
; Utility function get response from a HTTP call.
;
; INPUTS:
;  path       The URL path
;  token      Secure token obtained via dlac_login.
;
; OUTPUTS:
;  Result     The response from the HTTP call.
;
; USAGE:
;  IDL>res = dlsc_getfromurl("/mv?from="+src+"&to="+dest, token)
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_getfromurl,path,token

compile_opt idl2
On_error,2

; Initialize the DL Storage global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(path) eq 0 then message,'path not input'
if n_elements(token) eq 0 then message,'token not input'

url = !dls.svc_url+path
response = ""
ourl = obj_new('IDLnetURL')
; Add the auth token to the request header.
headers = 'X-DL-AuthToken: '+token
ourl->SetProperty,headers=headers
response = ourl->get(/string_array,url=url)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl   ; destroy when we are done

return,response

end
