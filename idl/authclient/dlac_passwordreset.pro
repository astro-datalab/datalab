;+
;
; DLAC_PASSWORDRESET
;
; Reset a user password reset.  We require that the user provide
; either a valid 'root' token or the token for the account being
; reset.
;
; INPUTS:
;  token      The DL authentication token string.
;  username   The username for whom to reset the password.
;  password   The new password.
;
; OUTPUTS:
;  Return 1 if the token is valid and 0 if invalid.
;
; USAGE:
;  IDL>good = dlac_isvalidtoken(token)
;
; By D. Nidever  June 2017, translated from authClient.py
;-

function dlac_isvalidtoken,token,username,token

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not supplied'
if n_elements(username) eq 0 then message,'Username not supplied'
if n_elements(password) eq 0 then message,'Password not supplied'

; Make the url
url = !dla.svc_url + "/passwordReset?"
url += 'token='+token
url += '&username='+username
url += '&password='+password
url += '&debug='+(!dla.debug ? "True" : "False")

if keyword_set(!dla.debug) then begin
  print,"passwordreset: token = '"+token+"'"
  print,"passwordreset: auth_token = '"+!dla.auth_token+"'"
  print,"passwordreset: url = '"+url+"'"
endif

if not dlac_isvalidtoken(token) then message,'Error: Invalid user token'

if !dla.auth_token is "" then begin
   message,"Error: User is not currently logged in"
endif else begin
  ; Split the token into its parts
  tokenarr = strsplit(token,'.',/extract)
  if n_elements(tokenarr) lt 4 then return,0 ; not correct token
  user = tokenarr[0]
  uid = tokenarr[1]
  gid = tokenarr[2]
  hash = strjoin(tokenarr[3:*],'.')  ; recombine if dots in hash
  if user ne 'root' and user ne username then $
    message,"Error: Invalid user or non-root token"

  response = 'None'
  ourl = obj_new('IDLnetURL')
  ; Add the auth token to the request header.
  headers = 'X-DL-AuthToken='+token
  ourl->SetProperty,headers=headers
  response = ourl->get(/string_array,url=url)
  ourl->GetProperty,response_code=status_code
  obj_destroy,ourl   ; destroy when we are done

  if status_code ne 200 then message,response

  ; Update the saved user token.
  if response ne '' then begin
    !dla.auth_token = response
    tok_file = !dla.home + '/id_token.' + !dla.username
    if file_test(tok_file) then file_delete,tok_file,/allow

    ; Write the new token file
    openw,unit,/get_lun,tok_file
    printf,unit,!dla.auth_token
    close,unit
    free_lun,unit

    if keyword_set(!dla.debug) then begin
      print,"pwreset: writing new token for '"+username+"'"
      print,"pwreset: response = '"+response+"'"
      print,"pwreset: !dla.auth_token = '"+!dla.auth_token+"'"
    endif

  endif else begin
    print,'pwReset response is None'
  endelse
endelse
    
return,response

end
