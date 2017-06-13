;+
;
; DLAC_ISVALIDUSER
;
; See whether the specified user is valid.
;
; INPUTS:
;  user    The DL authentication token string.
;
; OUTPUTS:
;  Return  1 if the user is valid and 0 if invalid.
;
; USAGE:
;  IDL>good = dlac_isvaliduser(user)
;
; By D. Nidever  June 2017, copied from authClient.py
;-

function dlac_isvaliduser,user

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(user) eq 0 then message,'User not supplied'

; Check for default user accounts
defuser = where(tag_names(!dla.def_users) eq user,ndefuser)
if ndefuser gt 0 then return,1

; Check normal username/password
url = !dla.svc_url + "/isValidUser?"
url += 'user='+user
url += '&profile='+!dla.svc_profile

if keyword_set(!dla.debug) then print,"isvaliduser: url = '"+url+"'"

val = dlac_retboolvalue(url)
return,val

end
