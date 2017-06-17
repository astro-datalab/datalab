function ssw_strsplit, inarray, pattern, tail=tail, head=head, lastpos=lastpos, $
	ss=ss
;
;+ 
;   Name: ssw_strsplit
;
;   Purpose: split string array at first (or last) occurence of pattern
;
;   Input Paramters:
;      inarry  - initial string array to split
;      pattern - search string (default is blank)
;
;   Output:
;      function return value is string array 
;
;   Calling Sequence:
;      strarr=ssw_strsplit(inarray, pattern , tail=tail)
;      strarr=ssw_strsplit(inarray, pattern , /tail, head=head)
;
;   Calling Examples:
;      head=ssw_strsplit(inarray, pattern)
;      tail=ssw_strsplit(inarray, pattern, /tail)
; 
;   History:
;      13-Jan-1993 (SLF)
;      11-Mar-1993 (SLF) 'released'
;      10-jun-1994 (SLF) bug fix
;      15-mar-2001 (RAS) renamed strsplit to ssw_strsplit
;
;-
if n_elements(pattern) eq 0 then pattern = ' '	; default to blank
tail=data_chk(tail,/scaler) and (1-data_chk(tail,/string))

; determine positions
postype=['strpos','str_lastpos']
pos=call_function(postype(keyword_set(tail)),inarray,pattern)

plen=strlen(pattern)
mlen=max(strlen(inarray))

; only loop for uniq positions (use vector strmids whenever possible)
if n_elements(pos) gt 1 then upos=pos(uniq(pos,sort(pos))) else upos=pos

ohead=strarr(n_elements(inarray))
otail=ohead

for i=0,n_elements(upos)-1 do begin
   which =where(pos eq upos(i))
   ohead(which)=strmid(inarray(which), 0, upos(i))
   otail(which)=strmid(inarray(which), upos(i)+plen,mlen)      
endfor

if keyword_set(tail) then begin
   head=ohead
   retval=otail
endif else begin
   tail=otail
   retval=ohead
endelse

return,retval
end
