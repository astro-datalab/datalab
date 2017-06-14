;+
;
; DLAC_ISUSERLOGGEDIN
;
; See whether the user identified by the token is currently
; logged in.
;
; INPUTS:
;  user    The username to check.
;
; OUTPUTS:
;  Return 1 if the user is logged-in and 0 if not.
;
; USAGE:
;  IDL>good = dlac_isuserloggedin('username')
;
; By D. Nidever  June 2017, translated from authClient.py
;-

function dlac_isuserloggedin,user

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(user) eq 0 then message,'User not supplied'

; Check if user is logged in
url = !dla.svc_url + "/isUserLoggedIn?"
url += 'user='+user
url += '&profile='+!dla.svc_profile

if keyword_set(!dla.debug) then print,"isuserloggedin: url = '"+url+"'"

val = dlac_retboolvalue(url)
return,val

end
