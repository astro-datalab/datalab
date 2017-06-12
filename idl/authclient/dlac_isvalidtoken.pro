;+
;
; DLAC_ISVALIDTOKEN
;
; INPUTS:
;  token    The DL authentication token string.
;
; OUTPUTS:
;  Return 1 if the token is valid and 0 if invalid.
;
; USAGE:
;  IDL>good = dlac_isvalidtoken(token)
;
; By D. Nidever  June 2017, copied from authClient.py
;-

function dlac_isvalidtoken,token

; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not supplied'

; Split the token into its parts
tokenarr = strsplit(token,'.')
if n_elements(tokenarr) ne 4 then return,0 ; not correct token
user = tokenarr[0]
uid = tokenarr[1]
gid = tokenarr[2]
hash = tokenarr[3]

; Check for default user accounts
defuser = where(tag_names(!dla.def_users) eq user,ndefuser)
if ndefuser gt 0 then $
  if (!dla.def_users).(defuser) eq token then return,1

; Check normal username/password
url = !dla.svc_url + "/isValidToken?"
url += 'token='+token
url += '&profile='+!dla.svc_profile

;if self.debug:
;   print ("isValidToken: url = '%s'" % url)

val = dlac_retboolvalue(url)
return,val

end
