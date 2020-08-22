	SELECT to_date(to_timestamp(CAST(A.f_trx AS STRING),'yyyyMMdd')) AS FECHA,
	       C.ecoraz,
	       COUNT(*) AS cantidad_trx,
	       SUM(A.mnt_trx) AS valor_total
	FROM resultados_vmp_dir_mp_col.tdd_fact_eab_cvht_ini A
	LEFT JOIN s_productos.eab_creco C ON CAST(A.cod_comercio AS BIGINT) = CAST(C.ecoces AS BIGINT)
	WHERE A.grp_trx='COMPRA'
	  AND A.ambiente_trx = "No Presente"
	  AND A.f_trx = {0}{1}{2}
	  AND C.ingestion_year={0}
	  AND C.ingestion_month={1}
	  AND C.ingestion_day={2}
	  AND C.year = {0}
	  AND A.year = {0}
	  AND A.month = {1}
	GROUP BY 1,2
;