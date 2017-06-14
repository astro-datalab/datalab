;+
;
; DLSC_RM
;
; Delete a file from the store manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  name       Name of the file(s) to remove.
;  /verbose   Verbose output.
;
; OUTPUTS:
;  Result     'OK' if everything succeeded.
;
; USAGE:
;  IDL>res = dlsc_rm(token,'file.txt')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_rm,token,name,verbose=verbose

compile_opt idl2
On_error,2

; Initialize the DL Storage global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(name) eq 0 then message,'name not input'
; Defaults
if n_elements(verbose) eq 0 then verbose=0

; Patch the names with the URI prefix if needed.
if strmid(fr,0,6) eq 'vos://' then nm=name else nm='vos://'+name
if nm eq 'vos://' or nm eq 'vos://tmp' or nm eq 'vos://public' then $
   return,'Error: operation not permitted'


; If the 'from' string has no metacharacters we're copying a single file,
; otherwise expand the file list on the and process the matches
; individually.
if not dlsc_hasmeta(fr) then begin
  r = dlsc_getfromurl("/rm?file="+nm, token)
  return,r
endif else begin
  flist = dlsc_expandfilelist(token, nm, "csv", /full)
  nfiles = n_elements(flist)
  fnum = 1
  for i=0,nfiles-1 do begin
    f = flist[i]
    if keyword_set(verbose) then print,'('+fnum+' / '+strtrim(nfiles,2)+') '+f
    r = dlsc_getfromurl("/rm?file"+f,token)
    fnum += 1
    if n_elements(resp) eq 0 then resp=r else resp=[resp,r]
  endfor
endelse
if n_elements(resp) eq 0 then resp=''

return,resp    

end
