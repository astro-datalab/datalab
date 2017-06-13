;+
;
; DLAC_CREATEGLOBAL
;
; Create the DL Auth Client global system variable !dla.
;
; By D. Nidever  June 2017
;-

pro dlac_createglobal

compile_opt idl2
On_error,2
  
; Pre-defined authentication tokens. These are fixed strings that provide
; limited access to Data Lab services, this access is controlled on the
; server-side so we don't need strict security here.
ANON_TOKEN = "anonymous.0.0.anon_access"
DEMO_TOKEN = "dldemo.99999.99999.demo_access"
TEST_TOKEN = "dltest.99998.99998.test_access"

; The URL of the AuthManager service to contact.  This may be changed by
; passing a new URL into the set_service() method before beginning.
DEF_SERVICE_URL = "https://dlsvcs.datalab.noao.edu/auth"

; The requested authentication "profile".  A profile refers to the specific
; machines and services used by the AuthManager on the server.
DEF_SERVICE_PROFILE = "default"

; Set the default user accounts for the authentication service.  We don't
; include privileged users so that account can remain secure.
DEF_USERS = {anonymous: ANON_TOKEN,dldemo: DEMO_TOKEN,dltest: TEST_TOKEN}

; API debug flag.
DEBUG = 0

DEFSYSV,'!dla',exists=exists
if exists eq 1 then return

; Initialize the DL Auth global structure
dla = {svc_url: DEF_SERVICE_URL,$	        ; service URL
       svc_profile: DEF_SERVICE_PROFILE,$   ; service prfile
       username: "",$                       ; default client logn user
       auth_token: "",$			; default client logn token
       home: expand_path('~')+'/.datalab',$ ; Get the $HOME/.datalab directory.
       debug: DEBUG,$                        ; interface debug flag
       def_service_url: DEF_SERVICE_URL,$
       def_service_profile: DEF_SERVICE_PROFILE,$
       def_users: DEF_USERS}
DEFSYSV, '!dla', dla
end
