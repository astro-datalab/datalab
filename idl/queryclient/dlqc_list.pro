;+
;
; DLQC_LIST
;
; List the tables in the user's MyDB.
;
; INPUTS:
;  token      Authentication token (see function dlac_login()).
;  table      The specific table to list (returns the schema).
;
; OUTPUTS:
;  listing    The list of tables in the user's MyDB or the schema of a specific table.
;
; USAGE:
;  IDL>res = dlqc_list(token,'results')
;
; By D. Nidever   June 2017, translated from queryClient.py
;-
 
function dlqc_list,token,table

compile_opt idl2
On_error,2

; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(table) eq 0 then message,'table not input'

dburl = !dlq.svc_url + '/list?table='+table
response = ""
ourl = obj_new('IDLnetURL')
; Add the auth token to the request header.
ourl->SetProperty,headers='Content-Type: text/ascii'
ourl->SetProperty,headers='X-DL-AuthToken: '+token
response = ourl->get(/string_array,url=dburl)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl            ; destroy when we are done

return,response

end
