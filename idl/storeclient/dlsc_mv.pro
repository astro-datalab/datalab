;+
;
; DLSC_MV
;
; Move/rename a file/directory within the store manager service.
;
; INPUTS:
;  token      Secure token obtained via dlac_login.
;  fr         The source file to move.
;  to         The name of the destination file.
;  /verbose   Verbose output.
;
; OUTPUTS:
;  Result     'OK' if everything succeeded.
;
; USAGE:
;  IDL>res = dlsc_mv(token,'file.txt','file2.txt')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_mv,token,fr,to,verbose=verbose

compile_opt idl2
On_error,2

; Initialize the DL Auth global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not input'
if n_elements(fr) eq 0 then message,'fr not input'
if n_elements(to) eq 0 then message,'to not input'
; Defaults
if n_elements(verbose) eq 0 then verbose=0

; Patch the names with the URI prefix if needed.
if strmid(fr,0,6) eq 'vos://' then src=fr else src='vos://'+fr
if strmid(to,0,6) eq 'vos://' then dest=to else dest='vos://'+to

; If the 'from' string has no metacharacters we're copying a single file,
; otherwise expand the file list on the and process the matches
; individually.
if not dlsc_hasmeta(fr) then begin
  r = dlsc_getfromurl("/mv?from="+src+"&to="+dest, token)
  return,r
endif else begin
  flist = dlsc_expandfilelist(token, src, "csv", /full)
  nfiles = n_elements(flist)
  fnum = 1
  for i=0,nfiles-1 do begin
    f = flist[i]
    fn = file_basename(f)
    to_fname = dest+'/'+fn
    if keyword_set(verbose) then print,'('+fnum+' / '+strtrim(nfiles,2)+') '+f+' -> '+to_fname
    r = dlsc_getfromurl("/mv?from="+f+"&to="+to_fname, token)
    fnum += 1
    if n_elements(resp) eq 0 then resp=r else resp=[resp,r]
  endfor
endelse
if n_elements(resp) eq 0 then resp=''

return,resp    

end
