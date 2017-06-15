;+
;
; DLQC_QUERY
;
; Send SQL query to DB.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  =adql      ADQL query string that will be passed to the DB query manager, e.g.
;               adql='select ra,dec from gaia_dr1.gaia_source limit 3'
;  =sql       SQL query string that will be passed to the DB query manager, e.g.
;               adql='select ra,dec from gaia_dr1.gaia_source limit 3'
;  =fmt       Format of the result to be returned by the query. Permitted values are:
;              * 'csv'     the returned result is a comma-separated string that
;                            looks like a csv file (newlines at the end of every row)
;              * 'ascii'   same, but the column separator is a tab \t
;              * 'votable' result is a string XML-formatted as a VO table
;              * 'fits'    FITS binary
;              * 'hdf5'    HDF5 file
;  =out       Either the VOSpace filename (e.g. vos://output.fits) or the MyDB
;               table name (e.g. mydb://results) where the output should go.
;  /async     If /async is set, the query is asynchronous, i.e. a job is
;              submitted to the DB, and a job token is returned. The token
;              must be then used to check the query's status and to retrieve
;              the result (when status is 'COMPLETE'). Default is a synchroneous
;              query that returns the results directly to the user.
;  =timeout   The timeout to use in seconds.
;
; OUTPUTS:
;  Result     If /async not set, the return value is the result of the
;               query as a formatted string (see =FMT). Otherwise the
;               result string is a jobid, with which later the
;               asynchronous query's status can be checked dlqc_status(),
;               and the result retrieved (see dlqc_results()).
;
; USAGE:
;  IDL>res = dlqc_query(token,sql='select ra,dec from gaia_dr1.gaia_source limit 3')
;
; By D. Nidever   June 2017, translated from queryClient.py
;-
 
function dlqc_query,token,sql=sql,adql=adql,fmt=fmt,out=out,async=async,timeout=timeout

compile_opt idl2
On_error,2

; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(adql) eq 0 and n_elements(sql) eq 0 then message,'No query specified'
; Defaults
if n_elements(async) eq 0 then async=0
if n_elements(adql) eq 0 then adql=''
if n_elements(sql) eq 0 then sql=''
if n_elements(out) eq 0 then out=''

; Set any requested timeout on the call.
if n_elements(timeout) then dlqc_settimeout,timeout

; ADQL query
if adql ne '' then begin
  query = mg_urlquote(adql)
  dburl = !dlq.svc_url + '/query?adql='+query+'&ofmt='+fmt
  dburl += '&out='+out+'&async='+(async ? "True" : "False")
  if strpos(query,'q3c_') ne -1 then $
     message,'q3c functionality is not part of the ADQL specification'
  if strpos(query,'healpix_') ne -1 then $
     message,'healpix functionality is not part of the ADQL specification'
     
; SQL query   
endif else begin
  query = mg_urlquote(sql)
  dburl = !dlq.svc_url + '/query?sql='+query+'&ofmt='+fmt
  dburl += '&out='+out+'&async='+(async ? "True" : "False")
endelse
               
if !dlq.svc_profile ne 'default' then dburl+='&profile='+!dlq.svc_profile

response = ""
ourl = obj_new('IDLnetURL')
; Set service headers.
headers = 'Content-Type: text/ascii'
ourl->SetProperty,headers=headers
headers = 'X-DL-TimeoutRequest: '+strtrim(!dlq.timeout_request,2)
ourl->SetProperty,headers=headers
headers = 'X-DL-ClientVersion: '+!dlq.version
ourl->SetProperty,headers=headers
headers = 'X-DL-AuthToken: '+token
ourl->SetProperty,headers=headers
response = ourl->get(/string_array,url=dburl)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl 

if status_code ne 200 then message,response

; Output requested
if out ne '' and not keyword_set(async) then begin
  if strmid(out,0,7) eq 'file://' then ofile=strmid(out,7) else ofile=out
  if strmid(out,0,3) ne 'vos' and strmid(out,0,4) ne 'mydb' then begin
    openw,unit,/get_lun,ofile
    printf,unit,response
    close,unit
    free_lun,unit
  endif

; Return result to use
endif else begin
  return,response
endelse

return,'OK'

end
