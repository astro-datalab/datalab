;+
;
; DLAC_LOGIN
;
; Login task of the Data Lab Autherization Client.
;
; INPUTS:
;  username    The DL username.
;  password    The password for "username".
;
; OUTPUTS:
;  token       The token string.
;
; USAGE:
;  IDL>token = dlac_login('username','password')
;
; By D. Nidever   June 2017, translated from authClient.py
;-
 
function dlac_login,username,password

compile_opt idl2
On_error,2

; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(username) eq 0 then message,'username not input'

; Return token for default user acounts
defuser = where(tag_names(!dla.def_users) eq username,ndefuser)
if ndefuser gt 0 then return,(!dla.def_users).(defuser)

; Check the $HOME/.datalab directory for a valid token.  If that dir
; doesn't already exist, create it so we can store the new token.
if file_test(!dla.home,/directory) eq 0 then file_mkdir,!dla.home

; See if a datalab token file exists for the requested user.
tok_file = !dla.home+'/id_token.'+username
;if self.debug:
;  print ("top of login: tok_file = '" + tok_file + "'")
;  print ("top of login: self.auth_token = '%s'" % str(self.auth_token))
;  print ("top of login: token = ")
;  os.system('cat ' + tok_file)

; No password input
if n_elements(password) eq 0 then begin
  ; Check the existing token file
  if file_test(tok_file) then begin
    ; read the old token
    openr,unit,/get_lun,tok_file
    o_tok = ''
    readf,unit,o_tok
    close,unit
    free_lun,unit

    ; Return a valid token, otherwise remove the file and obtain a
    ; new one.
    if (strmid(o_tok,0,len(username)) eq username) and dlac_isvalidtoken(o_token) then begin
      !dla.username = username
      !dla.auth_token = o_tok
      ;if self.debug:
      ;  print ("using old token for '%s'" % username)
      return,o_token
    endif else begin
      ;if self.debug:
      ;  print ("removing token file '%s'" % tok_file)
      file_delete,tok_file,/allow
    endelse
  endif
endif

; Need the password now
if n_elements(password) eq 0 then message,'No password supplied'

; Either the user is not logged in or the token is invalid, so
; make a service call to get a new token.
url = !dla.svc_url + "/login?"
url += 'username='+username
url += '&password='+password
url += '&profile='+!dla.svc_profile
url += '&debug='+strtrim(!dla.debug,2)
;'https://dlsvcs.datalab.noao.edu/auth/login?username=dnidever&password=datalab&profile=default&debug=False'
;query_args = {username: username, password: password,$
;              profile: !dla.svc_profile, debug: self.debug}
response = 'None'
ourl = obj_new('IDLnetURL')
response = ourl->get(/string_array,url=url)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl   ; destroy when we are done

;if self.debug:
;  print ('resp = ' + response)
;  print ('code = ' + str(r.status_code))
if status_code ne 200 then message,response


;if self.debug:
;  print ("Raw exception msg = '%s'" + str(e))
if dlac_isalive(!dla.svc_url) ne 1 then $
   message,"AuthManager Service not responding."
if dlac_isvaliduser(username) eq 1 then begin
  if dlac_isvalidpassword(username,password) eq 0 then message,'Invalid password'  
endif else begin
  message,'Invalid username'
endelse

!dla.auth_token = response
!dla.username = username

; Save the token.
info = file_info(!dla.home)
if info.write eq 1 then begin
  tok_file = !dla.home+'/id_token.'+username

  ; Write the new token file
  openw,unit,/get_lun,tok_file
  printf,unit,!dla.auth_token
  close,unit
  free_lun,unit

  ;if self.debug:
  ;  print ("login: writing new token for '%s'" % username)
  ;  print ("login: self.auth_token = '%s'" % str(self.auth_token))
  ;  print ("login: token = ")
  ;  os.system('cat ' + tok_file)
endif
                
return,!dla.auth_token

end
