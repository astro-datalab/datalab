;+
;
; DLSC_EXPANDFILELIST
;
; Expand a filename pattern in a VOSpace URI to a list of files.  We
; do this by getting a listing of the parent container contents from
; the service and then match the pattern on the client side.
;
; INPUTS:
;  token      The DL authentication token.
;  pattern    The pattern to match.
;  format     The format to use for the listing (csv or raw).
;  /full      
;
; OUTPUTS:
;  Result     The expanded list of files.
;
; USAGE:
;  IDL>res = dlsc_expandfilelist(token,pattern,'csv')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-
 
function dlsc_expandfilelist,token,pattern,format,full=full

compile_opt idl2
On_error,2

; Initialize the DL Auth global structure
DEFSYSV,'!dls',exists=dlsexists
if dlsexists eq 0 then DLSC_CREATEGLOBAL

; Not enough inputs
if n_elements(token) eq 0 then message,'Token not supplied.'
if n_elements(pattern) eq 0 then message,'Pattern not supplied.'
if n_elements(format) eq 0 then message,'Format not supplied.'
; Defaults
if n_elements(debug) eq 0 then debug=0

; The URI prefix is constant whether it's included in the pattern string
; or not.  The SM sm controls a specific instance of VOSpace so at the
; moment the expansiom to the VOSpace URI is handled on the server.  We'll
; prepend this to the service call as needed to ensure a correct argument
; and give the calling routine the option of leaving it off.
uri = 'vos://'
if strmid(pattern,0,6) eq 'vos://' then str=strmid(pattern,6) else str=pattern

; Extract the directory and filename/pattern from the string.
dir = file_dirname(str)
if dir eq '.' then dir=''
name = file_basename(str)
if keyword_set(debug) then begin
    print,"-----------------------------------------"
    print,"PATTERN = '" + str + "'"
    print,'str = ' + str
    print,"split: '"+dir+"' '"+name+"'"
endif

if (name ne '' and dlsc_hasmeta(name)) then pstr=name else pstr='*'

if dir ne '' then begin
  if dir eq "/" and name ne '' then begin
    dir = dir + name
  endif else begin
    if strmid(dir,0,1,/reverse_offset) eq '/' then $
      dir = strmid(dir,0,len(dir)-1)    ; trim trailing '/'
    if strmid(dir,0,1) ne '/' then dir = '/' + dir  ; prepend '/'
  endelse
endif else begin
  dir = '/'
  if name ne '' then dir = dir + name
endelse
if dir eq "/" then begin
   dir = ""
   pstr = name
endif
if not dlsc_hasmeta(name) then begin
  if name ne '' then pstr=name else pstr='*'
endif
   
; Make the service call to get a listing of the parent directory.
url = !dls.svc_url + "/ls?name=vos://"+dir+"&format=csv"
response = ""
ourl = obj_new('IDLnetURL')
; Add the auth token to the request header.
headers = 'X-DL-AuthToken: '+token
ourl->SetProperty,headers=headers
response = ourl->get(/string_array,url=url)
ourl->GetProperty,response_code=status_code
obj_destroy,ourl   ; destroy when we are done

; Filter the directory contents list using the filename pattern.
flist = strsplit(response,',',/extract)
nflist = n_elements(flist)
for i=0,nflist-1 do begin
  f = flist[i]
  ; need unix-like pattern matching with *, ?, etc. STRMATCH
  if strmatch(f,pstr,/fold_case) or f eq pstr then begin
    if not keyword_set(full) then furi=f else $
      furi = uri + dir + '/' + f
    furi = repstr(furi,'///','//')
    if n_elements(olist) eq 0 then olist=furi else olist=[olist,furi]
  endif
endfor
        
if keyword_set(debug) then begin
  print,url
  print,pattern+" --> '"+uri+"' '"+dir+"' '"+name+"' => '"+pstr+"'"
endif

si = sort(olist)
return,olist[si]

end
