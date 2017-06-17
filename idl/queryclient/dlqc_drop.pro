;+
;
; DLQC_DROP
;
; Drop the specified table from the user's MyDB.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  table      The specific table to drop from MyDB.
;
; OUTPUTS:
;  Result     'OK' if everything succeeded.
;
; USAGE:
;  IDL>res = dlqc_drop(token,'results')
;
; By D. Nidever  June 2017, translated from queryClient.py
;-

function dlqc_drop,token,table

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
   if n_elements(response) gt 0 then return,response

   ; Display the error msg in a dialog and in the IDL output log
   ;r = DIALOG_MESSAGE(!ERROR_STATE.msg, TITLE='URL Error', $
   ;      /ERROR)
   ;PRINT, !ERROR_STATE.msg
   MESSAGE, !ERROR_STATE.msg
   
   RETURN,''
ENDIF

; Initialize the DL Query global structure
DEFSYSV,'!dlq',exists=dlqexists
if dlqexists eq 0 then DLQC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'token not input'
if n_elements(table) eq 0 then message,'table not input'

dburl = !dlq.svc_url + '/delete?table='+table
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
