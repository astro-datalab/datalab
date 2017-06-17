;+
;
; DLQC_CREATEGLOBAL
;
; Create the DL Query Client global system variable !dla.
;
; By D. Nidever  June 2017
;-

pro dlqc_createglobal

compile_opt idl2
On_error,2

VERSION = '20170614'  ; yyyymmdd
DEF_SERVICE_URL = "https://dlsvcs.datalab.noao.edu/query"
PROFILE = "default"
DEBUG = 0
TIMEOUT_REQUEST = 120    ; sync query timeout default (120sec)

DEFSYSV,'!dlq',exists=exists
if exists eq 1 then return

; Initialize the DL Query global structure
dlq = {svc_url: DEF_SERVICE_URL,$	        ; service URL
       svc_profile: PROFILE,$                   ; service prfile
       timeout_request: TIMEOUT_REQUEST,$
       version: VERSION,$
       debug: DEBUG,$           ; interface debug flag
       def_service_url: DEF_SERVICE_URL,$
       def_service_profile: PROFILE}
DEFSYSV, '!dlq', dlq
end
