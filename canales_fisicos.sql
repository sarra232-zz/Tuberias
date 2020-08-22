SELECT 
CONCAT(CAST(t1.bvanored AS STRING),'-',lpad(CAST (t1.bvmesred AS STRING),2,'0'),'-',lpad(CAST(t1.bvdiared AS STRING),2,'0')) AS f_trx,
"SUC" AS canal,
CONCAT('SUC',CAST(t1.bvcodtrn AS STRING),'SUC') AS llave_trx,
COUNT(*) AS cantidad_trx
FROM s_canales.sai_scilibramd_sciffmvdia_dop t1 
WHERE t1.bvanored = {0}
AND t1.bvmesred = {1}
AND t1.bvdiared = {2}
AND t1.bvtpotrn IN (0,1) 
AND t1.bvcodacp = 1
AND t1.ingestion_year = {0}
AND year = {0}
GROUP BY 1,2,3
ORDER BY f_trx
UNION ALL
SELECT 
CONCAT(CAST(t1.bvanored AS STRING),'-',lpad(CAST (t1.bvmesred AS STRING),2,'0'),'-',lpad(CAST(t1.bvdiared AS STRING),2,'0')) AS f_trx,
"SUC" AS Canal,
CONCAT('SUC',CAST(t1.bvcodtrn AS STRING),'SUC') AS llave_trx,
COUNT(*) AS cantidad_trx
FROM s_canales.sai_scilibramd_sciffmvdia_ofc t1 
WHERE t1.bvanored = {0}
AND t1.bvmesred = {1}
AND t1.bvdiared = {2}
AND t1.bvtpotrn IN (0,1) 
AND t1.bvcodacp = 1
AND t1.ingestion_year = {0}
AND year = {0}
GROUP BY 1,2,3
ORDER BY f_trx
UNION ALL
SELECT 
CONCAT(CAST(t1.cnanored AS STRING),'-',lpad(CAST (t1.cnmesred AS STRING),2,'0'),'-',lpad(CAST(t1.cndiared AS STRING),2,'0')) AS f_trx,
"CB" AS canal,
CONCAT('CB',CAST(t1.cncodtrx AS STRING),'CB') AS llave_trx,
COUNT(*) AS cantidad_trx
FROM s_canales.sai_scilibramd_sciffmovcn t1 
WHERE t1.cnanored = {0}
AND t1.cnmesred = {1}
AND t1.cndiared = {2}
AND t1.cnesttrx = 1
AND t1.ingestion_year = {0}
AND year = {0}
GROUP BY 1,2,3
ORDER BY f_trx;