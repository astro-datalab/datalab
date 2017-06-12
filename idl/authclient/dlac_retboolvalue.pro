;+
;
; DLAC_RETBOOLVALUE
;
; Utility method to call a boolean service at the given URL
; for DL Authentication Client.
;
; INPUTS:
;  url      The URL to call for the boolean service.
;
; OUTPUTS:
;  Return   The True or False boolean value returned by
;              the boolean service.
;
; USAGE:
;  IDL>good = dlac_retboolvalue(url)
;
; By D. Nidever  June 2017, translated from authClient.py
;-

function dlac_retboolvalue,url

; Not enough inputs
if n_elements(url) eq 0 then message,'url not supplied'

; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL
  
response = ""
ourl = obj_new('IDLnetURL')

; Add the auth token to the request header.
if !dla.auth_token ne "" then begin
  headers = 'X-DL-AuthToken='+!dla.auth_token
  ourl->SetProperty,headers=headers
endif
response = ourl->get(/string_array,url=url)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl   ; destroy when we are done

if status_code ne 200 then message,response

if strlowcase(strtrim(response,2)) eq 'true' then return,1
return,0

end
