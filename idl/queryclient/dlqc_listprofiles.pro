;+
;
; DLQC_LISTPROFILES
;
; Retrieve the profiles supported by the query manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  profile    Requested service profile string.
;  =format    The output format.  The default is "text".
;
; OUTPUTS:
;  profiles   A list of the names of the supported profiles or a
;              dictionary of the specific profile.
;
; USAGE:
;  IDL>profiles = dlqc_listprofiles(token,profile)
;
; By D. Nidever  June 2017, translated from queryClient.py
;-

function dlqc_listprofiles,token,profile,format

compile_opt idl2
On_error,2
  
; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'token not input'
; Defaults
if n_elements(format) eq 0 then format='text'

dburl = !dlq.svc_url + '/profiles?'
if n_elements(profile) gt 0 then if profile ne 'None' and profile ne '' then $
  dburl += "profile="+profile+"&"
dburl += "format="+format

profiles = ""
ourl = obj_new('IDLnetURL')
; Add the auth token to the request header.
ourl->SetProperty,headers='Content-Type: text/ascii'
ourl->SetProperty,headers='X-DL-AuthToken: '+token
profiles = ourl->get(/string_array,url=dburl)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl            ; destroy when we are done

; Parse json
;   on some system this throws errors, not sure why
dum = where(stregex(profiles,'{',/boolean) eq 1,njson)
if njson gt 0 then profiles = json_parse(profiles)

return,profiles

end
