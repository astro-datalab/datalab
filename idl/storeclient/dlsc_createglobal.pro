;+
;
; DLSC_CREATEGLOBAL
;
; Create the DL Store Client global system variable !dla.
;
; By D. Nidever  June 2017
;-

pro dlsc_createglobal

compile_opt idl2
On_error,2

DEF_SERVICE_URL = "https://dlsvcs.datalab.noao.edu/storage"
PROFILE = "default"
DEBUG = 0

DEFSYSV,'!dls',exists=exists
if exists eq 1 then return

; Initialize the DL Store global structure
dls = {svc_url: DEF_SERVICE_URL,$	        ; service URL
       svc_profile: PROFILE,$                   ; service prfile
       debug: DEBUG,$                           ; interface debug flag
       def_service_url: DEF_SERVICE_URL,$
       def_service_profile: PROFILE}
DEFSYSV, '!dls', dls
end
