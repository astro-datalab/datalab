;+
;
; DLSC_GET
;
; Retrieve a file from the store manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  fr         The source file to get.
;  to         The name of the destination file.
;  /buffer    If 'to' is not set then return the result
;               into a byte buffer otherwise it will be
;               converted to string.
;  /verbose   Verbose output.
;
; OUTPUTS:
;  Result     'OK' if everything succeeded.
;
; USAGE:
;  IDL>res = dlsc_get(token,'file.txt','file2.txt')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_get,token,fr,to,verbose=verbose

compile_opt idl2
On_error,2

; If the url object throws an error it will be caught here
CATCH, errorStatus
IF (errorStatus NE 0) THEN BEGIN
   CATCH, /CANCEL
 
   ; Get the properties that will tell us more about the error.
   oUrl->GetProperty, RESPONSE_CODE=rspCode, $
         RESPONSE_HEADER=rspHdr, RESPONSE_FILENAME=rspFn
   ;PRINT, 'rspCode = ', rspCode
   ;PRINT, 'rspHdr= ', rspHdr
   ;PRINT, 'rspFn= ', rspFn
   ;print, 'response=', response
   
   ; Destroy the url object
   OBJ_DESTROY, oUrl

   ; If no problem then return
   if n_elements(resp) gt 0 then return,resp

   ; Display the error msg in a dialog and in the IDL output log
   ;r = DIALOG_MESSAGE(!ERROR_STATE.msg, TITLE='URL Error', $
   ;      /ERROR)
   ;PRINT, !ERROR_STATE.msg
   MESSAGE, !ERROR_STATE.msg
   
   RETURN,''
ENDIF

; Initialize the DL Storage global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(fr) eq 0 then message,'fr not input'
; Defaults
if n_elements(verbose) eq 0 then verbose=0
if n_elements(to) eq 0 then to=''
debug = 0

; Patch the names with the URI prefix if needed.
if strmid(fr,0,6) eq 'vos://' then nm=fr else nm='vos://'+fr

headers = 'X-DL-AuthToken: '+token

if keyword_set(debug) then print,'get: nm = '+nm

if dlsc_hasmeta(fr) then begin
  if not file_test(to,/directory) then message,'Location must be specified as a directory'
  if to eq '' then message,'Multi-file requests require a download location'
endif

if to ne '' then begin
  flist = dlsc_expandfilelist(token, nm, "csv", /full)
  if keyword_set(debug) then print,'get: flist = '+strtrim(flist,2) 
  nfiles = n_elements(flist)
  fnum = 1
  for i=0,nfiles-1 do begin
    f = flist[i]
    fn = file_basename(f)
    if strmid(to,0,1,/reverse_offset) eq '/' then begin
      if dlsc_hasmeta(fr) then dlname = (to+fn) else dlname=to
    endif else begin
      if dlsc_hasmeta(fr) then dlname = (to+'/'+fn) else dlname=to
    endelse

    url1 = !dls.svc_url + "/get?name="+f
    response = ""
    ourl = obj_new('IDLnetURL')
    ; Add the auth token to the request header.
    headers = 'X-DL-AuthToken: '+token
    ourl->SetProperty,headers=headers
    url2 = ourl->get(/string_array,url=url1)
    ourl->GetProperty,response_code=status_code
    obj_destroy,ourl            ; destroy when we are done

    if status_code ne 200 then begin
      if n_elements(resp) eq 0 then resp='Error: '+r else resp=[resp,'Error: '+r]
    endif else begin

      ourl = obj_new('IDLnetURL')
      ourl->SetProperty,headers=headers
      r = ourl->get(url=url2,filename=dlname)
      ;ourl->GetProperty,response_code=status_code,content_type=clen
      ;if n_elements(clen) eq 0 or clen eq 0 then total_length=0 else total_length=long(clen)
      obj_destroy,ourl            ; destroy when we are done

      if n_elements(resp) eq 0 then resp=r else resp=[resp,r]
    endelse
    fnum += 1
  endfor
  return,strtrim(resp,2)

endif else begin
   
  url1 = !dls.svc_url + "/get?name="+nm
  ourl = obj_new('IDLnetURL')
  ; Add the auth token to the request header.
  headers = 'X-DL-AuthToken: '+token
  ourl->SetProperty,headers=headers
  url2 = ourl->get(/string_array,url=url1)
  ourl->GetProperty,response_code=status_code
  obj_destroy,ourl            ; destroy when we are done

  resp = ''
  ourl = obj_new('IDLnetURL')
  ourl->SetProperty,headers=headers
  if not keyword_set(buffer) then retasstring=1 else retasstring=0
  resp = ourl->get(url=url2,string_array=retasstring,buffer=buffer)
  ourl->GetProperty,response_code=status_code
  obj_destroy,ourl            ; destroy when we are done
  
  return,resp

endelse

end
