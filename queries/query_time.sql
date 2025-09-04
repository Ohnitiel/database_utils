select age((now()), query_start) as age, a.waiting,
-- pg_cancel_backend(a.pid),
--    pg_terminate_backend(pid),
       a.*
FROM pg_stat_activity a
-- inner join pg_stat_activity b on a.query = b.query  
where 1 = 1
and a.application_name not ilike '%DBeaver 24.3.0 -%'
-- and a.application_name = 'Flyway by Redgate'
--and a.application_name ilike '%DBeaver%'
--  and a.application_name ilike '%pgAdmin%'
--   and a.waiting = true
-- and a.state = 'idle in transaction'
--   and a.state = 'idle'
    --and a.state = 'active'
-- and a.query ilike '%PRONTUARIO%'
--  and a.query ilike '%menuLog.id%'  
--  and a.query ilike '%atendimentos%'
--  and a.query ilike '%select this_.ATD_SEQ as ATD_SEQ1_3072_0_, this_.COR_GERME_MULTIRESISTENTE%'
-- and a.query ilike '%AGH.SCE_MOVIMENTO_MATERIAIS%'
--  and client_addr = '10.92.1.26'
-- and a.query ilike '%select serv.matricula, serv.vin_codigo,%'
-- and a.query ilike '%AGH.AIP_GRUPO_FAMILIAR_PACIENTES%'
-- and a.query ilike 'select serv.matricula%'
--  and (query_start) <= (now() - interval '2 minutes')
--and age((now()), query_start) >= (interval '2 minutes')
--   and application_name ilike 'app - 10.34.0.65%'
--  and a.pid in (6453)
-- and a.pid = 3803
--  and a.pid = 4565
--  and a.pid not in (4906)
-- and a.pid in (502)
-- and usename = 'ugen_quartz'
--   and usename = 'ugen_aghu'
--   and usename = 'postgres'    
--  and usename = 'ugen_integra'
--  and usename ilike '%integra%'
-- and usename <> 'postgres'
-- and a.waiting = true
-- and state = 'active'
--  and age((now()), query_start) >=  interval '10 minutes'
--   and query_start >= now() - interval '1440 minutes'
-- and a.query ilike '%alter%'
--   and upper(a.query) not SIMILAR TO upper('%(insert|update|delete|commit|rollback|alter|now()|autovacuum)%')
-- and a.query not ilike '%alter%'
-- and  datname= 'dbaghu_hcufu'
order by
1 desc
--, 9
;
 
