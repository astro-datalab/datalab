;+
;
; DLAC_ISVALIDPASSWORD
;
; INPUTS:
;  user      The DL authentication user string.
;  password  The DL authentication password string.
;
; OUTPUTS:
;  Return    1 if the user/password are valid and 0 if invalid.
;
; USAGE:
;  IDL>good = dlac_isvalidpassword(user,password)
;
; By D. Nidever  June 2017, copied from authClient.py
;-

function dlac_isvalidpassword,user,password

; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(user) eq 0 then message,'User not supplied'
if n_elements(password) eq 0 then message,'Password not supplied'

; Check for default user accounts
defuser = where(tag_names(!dla.def_users) eq user,ndefuser)
if ndefuser gt 0 and user eq password then return,1

; Check normal username/password
url = !dla.svc_url + "/isValidPassword?"
url += 'user='+user
url += '&password='+password
url += '&profile='+!dla.svc_profile

;if self.debug:
;   print ("isValidPassword: url = '%s'" % url)

val = dlac_retboolvalue(url)
return,val

end
