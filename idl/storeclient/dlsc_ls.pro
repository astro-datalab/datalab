;+
;
; DLSC_LS
;
; Get a file/directory listing from the store manager service
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  name       Valid name of file or directory, e.g. 'vos://somedir'.
;  format     The output format: 'csv' or 'raw'.
;
; OUTPUTS:
;  Result     The listing information.
;
; USAGE:
;  IDL>res = dlsc_ls(token,'file.txt','csv')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_ls,token,name,format

compile_opt idl2
On_error,2

; Initialize the DL Auth global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(name) eq 0 then message,'Name not input'
; Defaults
if n_elements(format) eq 0 then format='csv'

flist = dlsc_expandfilelist(token, name, format, full=0)
if (format eq 'csv') then begin
   result = strjoin(flist,',')
   if strmid(result,0,1) eq ',' then result=strmid(result,1)
   return,result
endif else begin
  nflist = n_elements(flist)
  for i=0,nflist-1 do begin
    f = flist[i]
    url = !dls.def_service_url + "/ls?name=vos://"+f+"&format="+format
    response = ""
    ourl = obj_new('IDLnetURL')
    ; Add the auth token to the request header.
    headers = 'X-DL-AuthToken='+token
    ourl->SetProperty,headers=headers
    response = ourl->get(/string_array,url=url)
    ourl->GetProperty,response_code=status_code
    obj_destroy,ourl   ; destroy when we are done
    if n_elements(results) eq 0 then results=response else results=[results,response]
   endfor
   response = strjoin(results,'\n')
   return,response
endelse

end
