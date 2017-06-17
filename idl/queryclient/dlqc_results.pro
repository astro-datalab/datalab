;+
;
; DLQC_RESULTS
;
; Retrieve the results of an asynchronous query, once completed.
;
; INPUTS:
;  token      Authentication token (see function dlac_login()).
;  jobid      The jobid returned when issuing an asynchronous query via
;             dlqc_queryquery() with /async.
;
; OUTPUTS:
;  status     Either 'QUEUED' or 'EXECUTING' or 'COMPLETED'. If the token and
;             jobid combination does not correspond to an actual job, then a
;             HTML-formatted error message is returned. If there is a
;             problem with the backend, the returned value can be 'ERROR'.
;
;             When status is 'COMPLETED', you can retrieve the results of
;             the query via dlqc_results().
;
; USAGE:
;  IDL>res = dlqc_results(token,jobid)
;
; By D. Nidever   June 2017, translated from queryClient.py
;-
 
function dlqc_results,token,jobid

compile_opt idl2
On_error,2

; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(jobid) eq 0 then message,'Jobid not input'

dburl = !dlq.svc_url + '/results?jobid='+jobid
if !dlq.svc_profile ne 'default' then dburl+="&profile="+!dlq.svc_profile
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
