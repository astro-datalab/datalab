;+
;
; DLQC_SCHEMA
;
; Return information about a data service schema value.
;
; INPUTS:
;  value      The database schema value to look up.  The input value should
;               be in the following format <catalog>.<table>.<column>.  Not
;               all levels must be specified.  Input an empty string
;               to see all the catalogs.
;  =format    The output format.  Only 'text' for now.
;  =profile   The name of the profile to use. The list of available ones can be
;               retrieved from the service (see function dlqc_list_profiles().
;               The default is 'default'.
;
; OUTPUTS:
;  schema     The schema information for the particular input value.
;
; USAGE:
;  IDL>res = dlqc_schema("usno.a2.raj2000","text","default")
;
; By D. Nidever   June 2017, translated from queryClient.py
;-
 
function dlqc_schema,value,format=format,profile=profile

compile_opt idl2
On_error,2

; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Not enough inputs
if n_elements(value) eq 0 then message,'value not input'
; Defaults
if n_elements(format) eq 0 then format='text'
if n_elements(profile) eq 0 then profile='default'

dburl = !dlq.svc_url + '/schema?value='+value
dburl += '&format='+strtrim(format,2)
dburl += '&profile='+strtrim(profile,2)
response = ""
ourl = obj_new('IDLnetURL')
response = ourl->get(/string_array,url=dburl)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl            ; destroy when we are done

return,response

end
