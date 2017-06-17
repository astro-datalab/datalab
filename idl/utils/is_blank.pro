;+
; Project     : HESSI
;                  
; Name        : IS_BLANK
;               
; Purpose     : return true is input is blank string
;                             
; Category    : string utility
;               
; Syntax      : IDL> a=is_blank(input)
;    
; Inputs      : INPUT= input string to check
;                              
; Outputs     : 1/0 if blank/nonblank
;                            
; History     : 15-Aug-2000, Zarro (EIT/GSFC)
;
; Contact     : dzarro@solar.stanford.edu
;-    


function is_blank,input

return,1b-is_string(input) 

end

