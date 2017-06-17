;+
;
; DLAC_LOGOUT
;
; Log the user out of the Data Lab.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;
; OUTPUTS:
;  Response   If logout was successful then 'OK' is returned,
;              otherwise the error message.
;
; USAGE:
;  IDL>resp = dlac_logout()
;
; By D. Nidever   June 2017, translated from authClient.py
;-
 
function dlac_logout,token

compile_opt idl2
On_error,2

; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Construct the logout url
url = !dla.svc_url + "/logout?"
url += '&token='+token
url += '&debug='+(!dla.debug ? "True" : "False")

if keyword_set(!dla.debug) then begin
  print,"logout: token = '"+token+"'"
  print,"logout: auth_token = '"+!dla.auth_token+"'"
  print,"logout: url = '"+url+"'"
endif
  
if not dlac_isValidToken(token) then $
  message,"Error: Invalid user token"

response = 'None'
ourl = obj_new('IDLnetURL')
; Add the auth token to the request header.
headers = 'X-DL-AuthToken='+token
response = ourl->get(/string_array,url=url)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl   ; destroy when we are done
        
if status_code ne 200 then message,response

!Dla.auth_token = ''
if !dla.username eq '' then begin   ; for datalab command-line tool
  ; Split the token into its parts
  tokenarr = strsplit(token,'.',/extract)
  if n_elements(tokenarr) lt 4 then return,0 ; not correct token
  user = tokenarr[0]
  uid = tokenarr[1]
  gid = tokenarr[2]
  hash = strjoin(tokenarr[3:*],'.')  ; recombine if dots in hash
  !dla.username = username
endif
tok_file = !dla.home + '/id_token.' + !dla.username
if file_test(tok_file) then file_delete,tok_file,/allow
                
return,response

end
