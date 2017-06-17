pro push,arr1,arr2,count=count,stp=stp

;+
;
; PUSH
;
; This function is like PERL's PUSH, which adds
; array elements at the end of another array
; Also see POP.
; Either or both arr1 and arr2 can be undefined.
;
; INPUTS:
;  arr1    The original array
;  arr2    The array to be concatenate with arr1 (at the end)
;  /stp    Stop at the end of the program.
;
; OUTPUTS:
;  arr1    The final array.
;  =count  The number of final elements in arr1.  This is set to -1
;             if there was an error.
;
; USAGE:
;  IDL>push,arr1,arr2
;
; By D.Nidever  2006?
;-

count = 0

; Error Handling
;------------------
; Establish error handler. When errors occur, the index of the  
; error is returned in the variable Error_status:  
CATCH, Error_status 

;This statement begins the error handler:  
if (Error_status ne 0) then begin 
   print,'PUSH ERROR: ', !ERROR_STATE.MSG  
   count = -1            ; There was an error
   CATCH, /CANCEL 
   return
endif


narr1 = n_elements(arr1)
narr2 = n_elements(arr2)

; ARR1 already exists
if (narr1 gt 0) then begin
  if narr2 gt 0 then $
    arr1 = [temporary(arr1),arr2]

; ARR1 does NOT exist
endif else begin
  if narr2 gt 0 then arr1 = arr2
endelse

; The number of final elements
count = n_elements(narr1)

if keyword_set(stp) then stop

end
