;+
;
; CSVPARSE
;
; Parse a CSV output with a header line to an IDL structure.
;
; INPUTS:
;  data     The comma-separated data with a header line
;            giving the column names.
;
; OUTPUTS:
;  str      The structure with the parsed data.
;
; USAGE:
;  IDL>str = csvparse(data)
;
; By D. Nidever  June 2017
;-

function csvparse,data

; Not enough inputs
if n_elements(data) eq 0 then begin
  print,'Syntax - str=csvparse(data)'
  return,-1
endif
  
; First line is column names
colnames = strsplit(data[0],',',/extract)
; Split the rest of the lines using the commas  
arr = strsplitter(data[1:*],',',/extract)
; Convert to structure
str = arr2str(arr,fieldnames=colnames)
return,str
end
