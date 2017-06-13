;+
;
; DLSC_HASMETA
;
; Determine whether a string contains filename meta-characters.
;
; INPUTS:
;  str      The string to check for meta-characters.
;
; OUTPUTS:
;  Results  1 if the string has meta-characters or 0 if not.
;
; USAGE:
;  IDL>res = dlsc_hasmeta('test*.fits')
;
; By D. Nidever   June 2017, translated from storeClient.py
;-

function dlsc_hasmeta,str

if strpos(str,'*') gt -1 or strpos(str,'[') gt -1 or strpos(str,'?') gt -1 then val=1 else val=0
return,val

end
