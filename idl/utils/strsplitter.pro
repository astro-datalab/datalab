;+
;
; STRSPLITTER
;
; This is exactly like STRPLIT.PRO except that you can input
; arrays as well
;
; INPUTS:
;  stringin     An input array of strings
;  pattern      The pattern to use for splitting the input string.
;                 By default, pattern=' '.
;  other STRSPLIT.PRO inputs
;
; OUTPUTS:
;  The array of split elements.
;  =error       The error, if one occured, otherwise undefined.
;
; USAGE
;  IDL> out = strsplitter(array,' ',/extract)
;
; By D.Nidever  Feb. 2007
;-

function strsplitter, stringIn, pattern, _ref_extra=extra, error=error

undefine,error
nstr = n_elements(stringIn)

; Not enough inputs
if (nstr eq 0) then begin
  print,'Syntax - out = strsplitter(stringArrIn,pattern)'
  return,-1
endif


; Error Handling
;------------------
; Establish error handler. When errors occur, the index of the  
; error is returned in the variable Error_status:  
CATCH, Error_status 

;This statement begins the error handler:  
if (Error_status ne 0) then begin 
   print,'STRSPLITTER ERROR: ', !ERROR_STATE.MSG  
   error = !ERROR_STATE.MSG
   CATCH, /CANCEL 
   return,-1
endif


;ON_ERROR, 2  ; return to caller

ncol = 1
outarr = strarr(ncol,nstr)


; Looping through the array
for i=0.,nstr-1 do begin

  ; Splitting the string
  if n_elements(pattern) eq 0 then  out = STRTOK(stringIn[i], _STRICT_EXTRA=extra)  $
    else out = STRTOK(stringIn[i], pattern, _STRICT_EXTRA=extra)

  nout = n_elements(out)
     
  ; Do we need to make the array larger?
  if (nout gt ncol) then begin
    old = outarr
    outarr = strarr(nout,nstr)
    outarr[0,0] = old    ; fill in what we already had
        
    ncol = nout
  endif

  ; Fill in the split string
  outarr[0,i] = out

end ; for loop

return,outarr

end
