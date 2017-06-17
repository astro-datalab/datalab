;+
;
; DLQC_ISALIVE
;
; INPUTS:
;  svc_url   The service URL.
;
; OUTPUTS:
;  Return    1 if the service is alive and 0 if not.
;
; USAGE:
;  IDL>good = dlqc_isalive(svc_url)
;
; By D. Nidever  June 2017, copied from queryClient.py
;-

function dlqc_isalive,svc_url

compile_opt idl2
On_error,2
  
; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL
  
if n_elements(svc_url) eq 0 then svc_url=!dlq.svc_url

response = 'None'
ourl = obj_new('IDLnetURL')
ourl->SetProperty,timeout=2
response = ourl->get(/string_array,url=svc_url)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl   ; destroy when we are done

if n_elements(response) gt 0 and status_code eq 200 then return,1
return,0

end
