;+
;
; DLAC_HASACCESS
;
; See whether the user has access to the named Resource.  Returns
; True if the user owns the Resource, or if the Resource grants
; group permissions to a Group to which the user belongs.
;
; INPUTS:
;  user      The DL usernae.
;  resource  The DL resource to check for access.
;
; OUTPUTS:
;  Return 1 if the user has access and 0 if not.
;
; USAGE:
;  IDL>good = dlac_hasaccess('username',resource)
;
; By D. Nidever  June 2017, translated from authClient.py
;-

function dlac_hasaccess,user,resource

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(user) eq 0 then message,'User not supplied'
if n_elements(resource) eq 0 then message,'Resource not supplied'

; Either the user is not logged in or the token is invalid, so
; make a service call to get a new token.
url = !dla.svc_url + "/hasAccess?"
url += 'user='+user
url += '&resource='+resource
url += '&profile='+!dla.svc_profile

if keyword_set(!dla.debug) then print,"hasaccess: url = '"+url+"'"
   
val = dlac_retboolvalue(url)
return,val

end
