;+
;
; DLAC_ISTOKENLOGGEDIN
;
; See whether the user identified by the token is currently
; logged in.
;
; INPUTS:
;  token    The DL authentication token to check.
;
; OUTPUTS:
;  Return 1 if the user is logged-in and 0 if not.
;
; USAGE:
;  IDL>good = dlac_istokenloggedin('username')
;
; By D. Nidever  June 2017, translated from authClient.py
;-

function dlac_istokenloggedin,token

compile_opt idl2
On_error,2
  
; Initialize the DL Auth global structure
DEFSYSV,'!dla',exists=dlaexists
if dlaexists eq 0 then DLAC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not supplied'

; Check if user is logged in
url = !dla.svc_url + "/isTokenLoggedIn?"
url += 'token='+token
url += '&profile='+!dla.svc_profile

if keyword_set(!dla.debug) then begin
  print,"istokenloggedin: tok = '"+token+"'"
  print,"istokenloggedin: url = '"+url+"'"
endif
   
val = dlac_retboolvalue(url)
return,val

end
