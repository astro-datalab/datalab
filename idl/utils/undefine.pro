PRO UNDEFINE, var1, var2, var3, var4, var5, var6, var7, var8, var9, var10, var11, var12

  for i=1,12 do begin
    n = n_elements( (SCOPE_VARFETCH('var'+strtrim(i,2))) )
    if (n gt 0) then tempvar = SIZE(TEMPORARY( (SCOPE_VARFETCH('var'+strtrim(i,2))) ))
    ;cmd = 'n = n_elements(var'+strtrim(i,2)+')'
    ;dum = execute(cmd)
    ;cmd = 'if (n gt 0) then tempvar = SIZE(TEMPORARY(var'+strtrim(i,2)+'))'
    ;dum = execute(cmd)
  end
END
