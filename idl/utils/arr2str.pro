;+
;
; ARR2STR.PRO
;
; This converts a 2D array to an IDL structure.
; This is very similar to the IMPORTASCII.PRO program.
; The array should have dimensions of [ntags,nrows].
; If it is the other way around then use TRANSPOSE to
; flip it.
;
; INPUT:
;  arr         Filename of the data file
;  /indef      Replace INDEFs with 999999
;  /allstr     Make all columns strings
;  /noprint    Don't print anything
;  fieldnames  Array of field names.
;  fieldtypes  Array of the IDL field types
;  /allfloat   All columns are floats (and no INDEFS) and can
;              be read more quickly with READF than READ_ASCII
;
; OUTPUTS:
;  str         The desired structure.
;
; USAGE:
;  IDL>str = arr2str(arr,fieldnames=fieldnames,fieldtypes=fieldtypes)
;
; By David Nidever  Mar. 2007
;-


Function datatypes,arr,stp=stp

;+
; This function figures out what data types strings have
;
; INPUT
;  arr   Array of strings
; 
; OUPUT
;  typearr   Array of the data types (3-Long Integer, 4-Float, 5-Double, 7-String)
;            See the documentation for SIZE for a description of data types.
;
; David Nidever   Feb.2006
;-

; No Parmaters input
if n_params() eq 0 then begin
  print,'Syntax - typearr = datatypes(array)'
  return,-1
endif



npar = n_elements(arr)
typearr = lonarr(npar)-1
validnum = valid_num(arr)
validint = valid_num(arr,/integer)
var = strtrim(arr,2)
nvar = strlen(var)

; Figuring out each column's data type
for i=0,npar-1 do begin
  ;var = strtrim(arr[i],2)
  ;bvar = byte(var)
  ;nvar = n_elements(bvar)

  ; String
  if validnum[i] eq 0 then type=7
  ; Float
  if validnum[i] eq 1 and validint[i] eq 0 then begin
    ; Float or Double?
    dec = first_el(strsplit(var[i],'.',/extract),/last)
    ndec = strlen(dec)
    ; What matters is the number of significant digits
    ;  not the decimal places
    ; subtract 1 to get rid of the decimal
    ndig = nvar[i]-1                   ; number of digits
    if strmid(var[i],0,1) eq '-' then ndig-=1   ; don't count the negative sign
    if ndig gt 7 then type=5 else type=4
  endif
  ; Long integer
  if validint[i] eq 1 and nvar[i] le 9 then type=3
  ; Long64 integer
  if validint[i] eq 1 and nvar[i] gt 9 then type=14
  ; NAN's are floats
  if strtrim(strupcase(var[i]),2) eq 'NAN' then type=4     ; float

  typearr[i] = type

;  ; If the string has only 0-9, -, +, ., E, e then it's a float, otherwise a string
;  ; 0-9 is 48-57 (in bytes)
;  ; "-" is 45
;  ; "+" is 43
;  ; "." is 46
;  ; "E" is 69
;  ; "e" is 101
;  ; "D" is 68
;  ; "d" is 100
;  bfloat = [bindgen(10)+48B,43B,45B,46B,69B,101B,68B,100B]
;  bint = [bindgen(10)+48B,43B,45B]
;
;  ; Checking each character in "bvar"
;  ; Are there any non-"number" characters?
;  badfloat = 0           ; float until proven otherwise
;  badint = 0
;  last = 0B
;  for j=0,nvar-1 do begin
;    ; Checking for float characters
;    g = where(bvar[j] eq bfloat,ng)  ; is this character a float characters
;    if ng eq 0 then badfloat=1
;
;    ; Checking for integer characters
;    g = where(bvar[j] eq bint,ng)    ; is this character an integer characters
;    if ng eq 0 then badint=1
;
;    ; Checking for plus or minus, must be at beginning, okay after 'E' or 'e'
;    if (bvar[j] eq 43B) or (bvar[j] eq 45B) then begin
;         if (j ne 0) and (last ne 69B) and (last ne 101B) then badfloat=1
;         badint = 1
;    endif
;
;    ; Checking for period, CAN'T be at beginning
;    if (bvar[j] eq 46B) then begin
;      if (j eq 0) then badfloat=1
;      badint = 1
;    endif
;
;    last = bvar[j]
;  endfor
;
;  ; Use VALID_NUM.PRO as a double-check
;  if valid_num(var) eq 1 then badfloat=0
;  if valid_num(var,/integer) eq 1 then badint=0
;
;  if valid_num(var) eq 0 then badfloat=1
;  if valid_num(var,/integer) eq 0 then badint=1
;
;  ; String
;  if (badfloat eq 1) then type = 7   ; String
;
;  ; Float
;  if (badfloat eq 0 and badint eq 1) then begin
;
;    ; Float or Double?
;    dec = first_el(strsplit(var,'.',/extract),/last)
;    ndec = strlen(dec)
;
;    ;; type = 5, Double
;    ;; type = 4, Float
;    ;if (ndec ge 6) then type=5 else type=4
;
;    ; What matters is the number of significant digits
;    ;  not the decimal places
;    ; subtract 1 to get rid of the decimal
;    ndig = nvar-1                   ; number of digits
;    if var[0] eq '-' then ndig-=1   ; don't count the negative sign
;    if ndig gt 7 then type=5 else type=4
;    ;if (nvar-1) gt 6 then type=5 else type=4
;
;  endif
;
;  ; Long Integer
;  if (badfloat eq 0 and badint eq 0) then type = 3   ; Integer (Long)
;
;  ; Long64 integer
;  if (badfloat eq 0 and badint eq 0 and nvar gt 9) then type = 14   ; Long64
;
;  ; NAN's are floats
;  if strtrim(strupcase(var),2) eq 'NAN' then type = 4     ; float
;
;  typearr[i] = type

endfor

return,typearr

end

;---------------------------------------------------------------------


Function arr2str,arr,fieldnames=fieldnames0,fieldtypes=fieldtypes,indef=indef,$
         noprint=noprint,stp=stp


; No parameters input
if n_params() eq 0 then begin
  print,'Syntax - str = arr2str(arr,fieldnames=fieldnames,fieldtypes=fieldtypes,/indef,'
  print,'                       /noprint,/stp)'
  return,-1
endif

if n_elements(indef) eq 0 then indef=1         ; By default remove indef

if keyword_set(fieldtypes) then typearr = fieldtypes
if keyword_set(fieldnames0) then fieldnames = fieldnames0


; Checking the dimensions of the input array
sz = size(arr)
if sz[0] lt 2 then begin
  print,'Must be a 2D array'
  return,-1
endif

ncol = sz[1]
nrow = sz[2]

arr1 = reform(arr[*,0])
if n_elements(typearr) eq 0 then typearr = datatypes(arr1)
;bd = where(typearr eq 7,nbd)
;if nbd eq 0 then allfloat=1

; Field Names
if not keyword_set(fieldnames) then $
  fieldnames = 'FIELD'+strtrim(sindgen(ncol),2)
if n_elements(fieldnames) ne ncol then begin
  print,'Input array of field names not of the correct size'
  fieldnames = 'FIELD'+strtrim(sindgen(ncol),2)
  dum=''
  read,dum,'Do you want to continue?'
  if strlowcase(strmid(strtrim(dum,2),0,1)) ne 'y' then stop
  ;stop
endif 

; Using READ_ASCII to read the data
IF not keyword_set(allfloat) THEN BEGIN

  START:

  ;; Making it into a "normal" structure
  ;cmd = 'dum = {'
  ;for i=0,ncol-1 do begin
  ;  if typearr(i) eq 3 then char = '0L'
  ;  if typearr(i) eq 4 then char = '0.0'
  ;  if typearr(i) eq 5 then char = '0.d0'
  ;  if typearr(i) eq 7 then char = '""'
  ;  if typearr(i) eq 14 then char = '0LL'
  ;  cmd=cmd+fieldnames(i)+':'+char
  ;  if i ne ncol-1 then cmd = cmd+', '
  ;end
  ;cmd = cmd+'}'
  ;
  ;ddd = execute(cmd)
  ;str = replicate(dum,nrow)

  dum = CREATE_STRUCT(fieldnames[0],fix(0,type=typearr[0]))
  for i=1,ncol-1 do $
    dum = CREATE_STRUCT(dum,fieldnames[i],fix(0,type=typearr[i]))
  str = replicate(dum,nrow)

  ; Transferring the data
  for i=0,ncol-1 do begin
    if typearr[i] ne 7 then str.(i)=fix(reform(arr[i,*]),type=typearr[i]) else $
      str.(i)=reform(arr[i,*])   ; just copy strings
  endfor
  ;undefine,arr

  ; Converting INDEF's to 999999
  if keyword_set(indef) then begin

    nbad = 0

    ; Looping through the fields
    for i=0,ncol-1 do begin
      col = strtrim(reform(str.(i)),2)
      bd = where(col eq 'INDEF' or col eq "'INDEF'",nbd)
      if nbd gt 0 then str(bd).(i) = '999999'
      nbad = nbad + nbd
    end

    if (not keyword_set(noprint)) then $
      if (nbad gt 0) then print,"INDEF's converted to 999999"

  endif

  ; Converting them to the proper types
  if not keyword_set(allstr) then begin

    ; Getting the data types
    if not keyword_set(fieldtypes) then begin
      for i=0,ncol-1 do begin

        ; Check the first 100 (or nrow) for the type
        ; Use the maximum type
        ; The higher types encompass all other lower ones.
        ; 7-string > 5-double > 4-float > 3-long > 2-int
        ;type = datatypes(str(0).(i))
        type100 = datatypes(str[0:99<(nrow-1)].(i))    ; using first 100 rows 
        type = max(type100)                            ; use the maximum 
        typearr[i] = type
      endfor
    endif else begin
      typearr = fieldtypes
    endelse

    ;; Making a new structure with the proper types
    ;cmd = 'dum = {'
    ;for i=0,ncol-1 do begin
    ;  if typearr(i) eq 3 then char = '0L'
    ;  if typearr(i) eq 4 then char = '0.0'
    ;  if typearr(i) eq 5 then char = '0.d0'
    ;  if typearr(i) eq 7 then char = '""'
    ;  if typearr(i) eq 14 then char = '0LL'
    ;  cmd=cmd+fieldnames(i)+':'+char
    ;  if i ne ncol-1 then cmd = cmd+', '
    ;end
    ;cmd = cmd+'}'
    ;
    ;ddd = execute(cmd)
    ;arr2 = replicate(dum,nrow)

    dum = CREATE_STRUCT(fieldnames[0],fix('',type=typearr[0]))
    for i=1,ncol-1 do $
      dum = CREATE_STRUCT(dum,fieldnames[i],fix('',type=typearr[i]))
    arr2 = replicate(dum,nrow)

    ; Transferring the data
    for i=0,ncol-1 do arr2.(i) = str.(i)
    str = arr2
  end


; /ALLFLOAT, Using READF to read the data
ENDIF ELSE BEGIN

  ; Checking the data types
  typearr = datatypes(arr1)
  bd = where(typearr eq 7,nbd)

  ; Some of the columns are strings
  if nbd gt 0 then begin
    print,'Some of the columns are strings'
    print,'Using READ_ASCII to read the data'
    goto,START
  endif

  ;; Creating the normal structure
  ;cmd = 'dum = {'
  ;for i=0,ncol-1 do begin
  ;  cmd=cmd+fieldnames(i)+':0.0'
  ;  if i ne ncol-1 then cmd = cmd+', '
  ;end
  ;cmd = cmd+'}'
  ;
  ;ddd = execute(cmd)
  ;str = replicate(dum,nrow)

  dum = CREATE_STRUCT(fieldnames[0],0.0)
  for i=1,ncol-1 do $
    dum = CREATE_STRUCT(dum,fieldnames[i],0.0)
  str = replicate(dum,nrow)


  ; Transferring the data
  for i=0,ncol-1 do str.(i) = reform(arr[i,*])

ENDELSE


; Report on the data
if not keyword_set(noprint) then $
  print,strtrim(ncol,2),' columns x ',strtrim(nrow,2),' rows'


if keyword_set(stp) then stop

return,str

end
