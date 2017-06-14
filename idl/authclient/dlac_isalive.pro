;+
;
; DLAC_ISALIVE
;
; INPUTS:
;  svc_url   The service URL.
;
; OUTPUTS:
;  Return    1 if the service is alive and 0 if not.
;
; USAGE:
;  IDL>good = dlac_isalive(svc_url)
;
; By D. Nidever  June 2017, translated from authClient.py
;-

function dlac_isalive,svc_url

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL
  
if n_elements(svc_url) eq 0 then svc_url=!dla.def_service_url

response = 'None'
ourl = obj_new('IDLnetURL')
ourl->SetProperty,timeout=2
response = ourl->get(/string_array,url=svc_url)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl   ; destroy when we are done

if n_elements(response) gt 0 and status_code eq 200 then return,1
return,0

end
