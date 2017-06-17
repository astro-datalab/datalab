;
; Copyright (c) 2006-2009, ITT Visual Information Solutions. All
;       rights reserved. Unauthorized reproduction is prohibited.
;+
; NAME:
;   URL_PARSE
;
; PURPOSE:
;   This utility function can be used to break a URL into it's
;   individual componets.
;
;   This function returns a structure containing the individual components
;   of the url that can be used to set the corresponding properties on the
;   IDLnetUrl, IDLnetOgcWms and IDLnetOgcWcs objects.

;   URL Components:
;   URL_SCHEME://URL_USERNAME:URL_PASSWORD@URL_HOST:URL_PORT/URL_PATH?URL_QUERY
;
;   Example url:
;   http://me:mypw@host.com:8080/project/data/get_data.cgi?dataset=climate&date=01012006
;
; CATEGORY:
;   URL
;
; CALLING SEQUENCE:
;   Result = url_parse(url)
;
; INPUTS:
;   URL: A string containing a url
;
; KEYWORDS:
;   None
;
; OUTPUTS:
;   This function returns a structure containing the URL components.  The
;   returned structure has the following elements
;           Scheme
;           Username
;           Password
;           Host
;           Port
;           Path
;           Query
;
; EXAMPLE CALLING SEQUENCE:
;   url_struc = url_parse(url)
;
; MODIFICATION HISTORY:
;   Jan 2007 - Initial Version
;   26-jul-2011 - S.L.Freeland - do/remove QUERY 1st, since "special characters" intefere with original logic
;                                (for example, '@' is allowable JSOC
;                                query charcter which clobbered
;                                username logic).
;  11-Jul-2013, Zarro (ADNET) - renamed to URL_PARSE to avoid
;                               conflict with IDL library version.
;  24-Jul-2013, Zarro (ADNET) - added more error checking.
;  28-Jan-2015, Zarro (ADNET) - added check for ":" in path name.
;  12-Feb-2015, Zarro (ADNET) - removed default port=80.
;   2-Mar-2015, Zarro (ADNET) - added port=21 for FTP
;  17-Feb-2016, Zarro (ADNET) - ensure Unix path delimiters in URL
;  30-Aug-2016, Zarro (ADNET) - perform query check first
;  17-Sep-2016, Zarro (ADNET) - added port 443 for HTTPS
;  18-Dec-2016, Zarro (ADNET) - added check for appended port
;-

function urlparse, url

  ; create the return structure that will contain the url components
  xUrlProps = create_struct(    'Scheme',     '', $
                                'Username',   '', $
                                'Password',   '', $
                                'Host',       '', $
                                'Port',       '',$
                                'Path',       '', $
                                'Query',      '')

  if is_blank(url) then return, xUrlProps
  
  durl=strtrim(url,2)
  ;durl=str_replace(durl,'\','/')
  durl=repstr(durl,'\','/')

  ; remove QUERY first to allow free form queries
  
  query='' ; init to No Query
  if strpos(durl,'?') ne -1 then query=ssw_strsplit(durl,'?',/tail, head=durl) ; divide URL/QUERY
  durl=durl[0]
  query=query[0]

  ; find the beginning of the host component
  ; if a '://' was not found then assume HTTP

  iPos = strpos(durl, '://')
  if (iPos eq -1) then durl='http://'+durl

  iUrlLen=strlen(durl)
  iPos = strpos(durl, '://')
 
  ; store the scheme in the return structure
  xUrlProps.Scheme = strmid(durl, 0, iPos)

  ; move past the '://'
  iPos = iPos + 3

  ; extract a username and password if they are presnt
  iPosAt = strpos(durl, '@', iPos)
  if (iPosAt ne -1) then begin
     iPosPass = strpos(durl, ':', iPos)
     if ((iPosPass ne -1) && (iPosPass lt iPosAt)) then begin
         iLen = iPosAt - (iPosPass + 1)
         ; store the password component in the return structure
         xUrlProps.Password = strmid(durl, iPosPass+1, iLen)
     endif else begin
         iPosPass = iPosAt
     endelse
     iLen = iPosPass - iPos
     ; store the username component in the return structure
     xUrlProps.username = strmid(durl, iPos, iLen)
     iPos = iPosAt + 1
  endif

  ; find the start of the path if the url has a path
  iPosPath = strpos(durl, '/', iPos)
  if (iPosPath eq -1) then begin
     iPosPath = iUrlLen
  endif

  iHostEnd = iPosPath

  ; extract port number if present
  iPosPort = strpos(durl, ':', iPos)
  if (iPosPort ne -1) then begin
     iLen = iPosPath - (iPosPort + 1)
      ; store the port component in the return structure
     if iLen gt 0 then begin
      xUrlProps.Port = strmid(durl, iPosPort+1, iLen)
      iHostEnd = iPosPath - iLen -1
     endif 
  endif

;-- use defaults if no PORT found.

  if stregex(xUrlProps.scheme,'^https',/bool) && is_blank(xUrlProps.Port) then xUrlProps.Port='443'
  if stregex(xUrlProps.scheme,'^ftp',/bool) && is_blank(xUrlProps.Port) then xUrlProps.Port='21'

;-- use last port if multiple

  if stregex(xUrlProps.Port,':',/bool) then begin
   chk=strsplit(xUrlProps.Port,':',/extract,count=count)
   if count ne 0 then xUrlProps.Port=chk[count-1]
  endif
  if is_blank(xUrlProps.Port) then xUrlProps.Port='80'
  if stregex(xUrlProps.scheme,'^https',/bool) && (xUrlProps.Port eq '80') then xUrlProps.Port='443'

 
  ; store the host component in the return structure
  iLen = iHostEnd - iPos
  xUrlProps.Host = strmid(durl, iPos, iLen)

  ; store the path component in the return structure
  xUrlProps.Path = strmid(durl, iPosPath+1, strlen(durl))

  ; store the query component in the return structure
  xUrlProps.Query = query 

  ; return the url components in a structure
  return, xUrlProps

end


