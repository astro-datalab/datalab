;+
; Project     : HESSI
;                  
; Name        : IS_STRING
;               
; Purpose     : return true is input is a non-blank string
;                             
; Category    : string utility
;               
; Syntax      : IDL> a=is_string(input)
;    
; Inputs      : INPUT_STR = input variable to check
;                              
; Outputs     : 0/1 if blank/nonblank
;
; Keywords    : return true (1), even if string is blank
;               
; Opt. Outputs: NONBLANK = noblank copies of input
;               (if input is array, then nonblanks are filtered out)
;             
; History     : 17-Nov-1999, Zarro (SM&A/GSFC)
;                5-Feb-2003, Zarro (EER/GSFC) - added /BLANK
;
; Contact     : dzarro@solar.stanford.edu
;-    


function is_string,input_str,nonblank,err=err,blank=blank,count=count

count=0l
err=''
nonblank=''
sz=size(input_str)
dtype=sz[n_elements(sz)-2]
if dtype ne 7 then return,0b
if keyword_set(blank) then return,1b

b1=strtrim(string(1b),2)
b0=strtrim(string(0b),2)
b32=strtrim(string(32b),2)
temp=strtrim(input_str,2)
chk=where( (temp ne b0) and (temp ne b1) and (temp ne b32),count)
if count eq 0 then begin
 err='Input strings are blank'
 return,0b
endif

nonblank=temp[chk]

if count eq 1 then nonblank=nonblank[0]
return,1b
end
