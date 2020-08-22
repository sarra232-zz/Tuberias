SELECT 
CONCAT(CAST(t1.anotrn AS STRING),'-',lpad(CAST(t1.mestrn AS STRING),2,'0') ,'-',lpad(CAST(t1.diatrn AS STRING),2,'0') ) AS f_trx,
TRIM(t1.canal) AS canal,
TRIM(CONCAT(TRIM(t1.canal),CAST(t1.cdgtrn AS STRING),t1.disposit)) AS trx_1,
COUNT(*) AS cantidad_trx
FROM s_canales.itc_itclibranl_itcffacmcn_part t1
WHERE t1.anotrn = {0}
AND lpad(CAST(t1.mestrn AS STRING),2,'0') ='{1}'
AND t1.diatrn = {2}
AND t1.cdgrpta = 0
AND t1.ingestion_year = {0}
AND t1.ingestion_month = {1}
AND t1.ingestion_day = {2}
GROUP BY 1,2,3
ORDER BY f_trx;
