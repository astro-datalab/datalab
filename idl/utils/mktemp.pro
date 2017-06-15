function mktemp,prefix,directory=directory,error=error,outdir=outdir

;+
;
; MKTEMP.PRO
;
; PURPOSE:
;  This uses the unix MKTEMP function to create a temporary
;  file.  It will be created with 0 bytes.  The temporary file
;  or directory will be created in the current directory.
;
; INPUTS:
;  prefix      The prefix for the temporary filename (e.g. "temp")
;  /directory  This is for a directory.
;  =outdir     The output directory.  If not given then the
;                current directory is used.
;
; OUTPUTS:
;  temp      The filename of the temporary file or directory.
;
; USAGE:
;  IDL>tmp = mktemp('temp')
;
; By D.Nidever   April 2008
;-

; Add an option to give the desired directory.  Will need to
; CD there first before making the file.  Make sure that the
; directory exists first and you can write into it.

undefine,file,error

if n_elements(prefix) eq 0 then prefix='temp'

; Get current directory
CD,current=curdir

if n_elements(outdir) eq 0 then begin
  template = curdir+'/'+prefix+'.XXXXXX'
endif else begin
  template = outdir+'/'+prefix+'.XXXXXX'
endelse

; Use the UNIX "mktemp" to get the "random" portion
; this returns something like "prefix.zXlHW11740"
input = ['mktemp',template]
if keyword_set(directory) then input = ['mktemp','-d',template]
SPAWN,input,out,errout,/noshell     

; There was an error
if errout[0] ne '' then begin
  print,'MKTEMP ERROR - ',errout
  error = 'MKTEMP ERROR - '+errout
  return,'-1'
endif

file = reform(out[0])

; Make sure it's readable, change permissions to 644
FILE_CHMOD,file,'644'o

;stop

return,file

end
