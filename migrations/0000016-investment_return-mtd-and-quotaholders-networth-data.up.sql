DROP MATERIALIZED VIEW xf_with_irm_timeseries;

DROP FUNCTION icf_denom_social_unaccented(icf_with_xf_and_bf_and_iry_and_f_of_last_year);
DROP MATERIALIZED VIEW icf_with_xf_and_bf_and_iry_and_f_of_last_year;

DROP FUNCTION icf_denom_social_unaccented(icf_with_xf_and_iry_of_last_year);
DROP MATERIALIZED VIEW icf_with_xf_and_iry_of_last_year;

ALTER TABLE public.investment_return_daily
    ALTER COLUMN ird_networth TYPE DOUBLE PRECISION,
    ALTER COLUMN ird_quotaholders TYPE DOUBLE PRECISION;

ALTER TABLE public.investment_return_daily        
    ADD COLUMN ird_accumulated_networth DOUBLE PRECISION,
    ADD COLUMN ird_networth_mtd DOUBLE PRECISION,
    ADD COLUMN ird_networth_ytd DOUBLE PRECISION,
    ADD COLUMN ird_networth_1m DOUBLE PRECISION,
    ADD COLUMN ird_networth_3m DOUBLE PRECISION,
    ADD COLUMN ird_networth_6m DOUBLE PRECISION,
    ADD COLUMN ird_networth_1y DOUBLE PRECISION,
    ADD COLUMN ird_networth_2y DOUBLE PRECISION,
    ADD COLUMN ird_networth_3y DOUBLE PRECISION,    
    ADD COLUMN ird_accumulated_quotaholders DOUBLE PRECISION,
    ADD COLUMN ird_quotaholders_mtd DOUBLE PRECISION,
    ADD COLUMN ird_quotaholders_ytd DOUBLE PRECISION,
    ADD COLUMN ird_quotaholders_1m DOUBLE PRECISION,
    ADD COLUMN ird_quotaholders_3m DOUBLE PRECISION,
    ADD COLUMN ird_quotaholders_6m DOUBLE PRECISION,
    ADD COLUMN ird_quotaholders_1y DOUBLE PRECISION,
    ADD COLUMN ird_quotaholders_2y DOUBLE PRECISION,
    ADD COLUMN ird_quotaholders_3y DOUBLE PRECISION;

ALTER TABLE public.investment_return_monthly
    ALTER COLUMN irm_networth TYPE double precision,
    ALTER COLUMN irm_quotaholders TYPE double precision;

ALTER TABLE public.investment_return_monthly       
    ADD COLUMN irm_accumulated_networth DOUBLE PRECISION,
    ADD COLUMN irm_networth_mtd DOUBLE PRECISION,
    ADD COLUMN irm_networth_ytd DOUBLE PRECISION,
    ADD COLUMN irm_networth_1m DOUBLE PRECISION,
    ADD COLUMN irm_networth_3m DOUBLE PRECISION,
    ADD COLUMN irm_networth_6m DOUBLE PRECISION,
    ADD COLUMN irm_networth_1y DOUBLE PRECISION,
    ADD COLUMN irm_networth_2y DOUBLE PRECISION,
    ADD COLUMN irm_networth_3y DOUBLE PRECISION,    
    ADD COLUMN irm_accumulated_quotaholders DOUBLE PRECISION,
    ADD COLUMN irm_quotaholders_mtd DOUBLE PRECISION,
    ADD COLUMN irm_quotaholders_ytd DOUBLE PRECISION,
    ADD COLUMN irm_quotaholders_1m DOUBLE PRECISION,
    ADD COLUMN irm_quotaholders_3m DOUBLE PRECISION,
    ADD COLUMN irm_quotaholders_6m DOUBLE PRECISION,
    ADD COLUMN irm_quotaholders_1y DOUBLE PRECISION,
    ADD COLUMN irm_quotaholders_2y DOUBLE PRECISION,
    ADD COLUMN irm_quotaholders_3y DOUBLE PRECISION;

ALTER TABLE public.investment_return_yearly
    ALTER COLUMN iry_networth TYPE double precision,
    ALTER COLUMN iry_quotaholders TYPE double precision;

ALTER TABLE public.investment_return_yearly    
    ADD COLUMN iry_investment_return_mtd DOUBLE PRECISION,
    ADD COLUMN iry_cdi_investment_return_mtd DOUBLE PRECISION,    
    ADD COLUMN iry_bovespa_investment_return_mtd DOUBLE PRECISION,
    ADD COLUMN iry_ipca_investment_return_mtd DOUBLE PRECISION,
    ADD COLUMN iry_igpm_investment_return_mtd DOUBLE PRECISION,
    ADD COLUMN iry_igpdi_investment_return_mtd DOUBLE PRECISION,
    ADD COLUMN iry_dolar_investment_return_mtd DOUBLE PRECISION,
    ADD COLUMN iry_euro_investment_return_mtd DOUBLE PRECISION,
    ADD COLUMN iry_risk_mtd DOUBLE PRECISION,
    ADD COLUMN iry_cdi_sharpe_mtd DOUBLE PRECISION,
    ADD COLUMN iry_bovespa_sharpe_mtd DOUBLE PRECISION,
    ADD COLUMN iry_ipca_sharpe_mtd DOUBLE PRECISION,
    ADD COLUMN iry_igpm_sharpe_mtd DOUBLE PRECISION,
    ADD COLUMN iry_igpdi_sharpe_mtd DOUBLE PRECISION,
    ADD COLUMN iry_dolar_sharpe_mtd DOUBLE PRECISION,
    ADD COLUMN iry_euro_sharpe_mtd DOUBLE PRECISION,
    ADD COLUMN iry_cdi_consistency_mtd DOUBLE PRECISION,    
    ADD COLUMN iry_bovespa_consistency_mtd DOUBLE PRECISION,        
    ADD COLUMN iry_ipca_consistency_mtd DOUBLE PRECISION,
    ADD COLUMN iry_igpm_consistency_mtd DOUBLE PRECISION,
    ADD COLUMN iry_igpdi_consistency_mtd DOUBLE PRECISION,
    ADD COLUMN iry_dolar_consistency_mtd DOUBLE PRECISION,
    ADD COLUMN iry_euro_consistency_mtd DOUBLE PRECISION,
    ADD COLUMN iry_accumulated_networth DOUBLE PRECISION,
    ADD COLUMN iry_networth_mtd DOUBLE PRECISION,
    ADD COLUMN iry_networth_ytd DOUBLE PRECISION,
    ADD COLUMN iry_networth_1m DOUBLE PRECISION,
    ADD COLUMN iry_networth_3m DOUBLE PRECISION,
    ADD COLUMN iry_networth_6m DOUBLE PRECISION,
    ADD COLUMN iry_networth_1y DOUBLE PRECISION,
    ADD COLUMN iry_networth_2y DOUBLE PRECISION,
    ADD COLUMN iry_networth_3y DOUBLE PRECISION,    
    ADD COLUMN iry_accumulated_quotaholders DOUBLE PRECISION,
    ADD COLUMN iry_quotaholders_mtd DOUBLE PRECISION,
    ADD COLUMN iry_quotaholders_ytd DOUBLE PRECISION,
    ADD COLUMN iry_quotaholders_1m DOUBLE PRECISION,
    ADD COLUMN iry_quotaholders_3m DOUBLE PRECISION,
    ADD COLUMN iry_quotaholders_6m DOUBLE PRECISION,
    ADD COLUMN iry_quotaholders_1y DOUBLE PRECISION,
    ADD COLUMN iry_quotaholders_2y DOUBLE PRECISION,
    ADD COLUMN iry_quotaholders_3y DOUBLE PRECISION;

ALTER TABLE public.btgpactual_funds
    RENAME bf_investiment_quota TO bf_investment_quota;

CREATE MATERIALIZED VIEW icf_with_xf_and_bf_and_iry_and_f_of_last_year
AS
SELECT DISTINCT ON (icf_CNPJ_FUNDO) icf_CNPJ_FUNDO as DISTINCT_icf_CNPJ_FUNDO,* 
	FROM inf_cadastral_fi_fullname 
	LEFT JOIN xpi_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = xpi_funds.xf_cnpj
    LEFT JOIN btgpactual_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = btgpactual_funds.bf_cnpj
    LEFT JOIN funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = funds.f_cnpj    
	LEFT JOIN (
		SELECT DISTINCT ON (iry_cnpj_fundo) iry_cnpj_fundo as DISTINCT_iry_cnpj_fundo, * FROM investment_return_yearly ORDER BY iry_cnpj_fundo, iry_dt_comptc DESC
	) AS LAST_YEAR ON inf_cadastral_fi_fullname.icf_cnpj_fundo = LAST_YEAR.iry_cnpj_fundo
WITH DATA;

COMMENT ON MATERIALIZED VIEW public.icf_with_xf_and_bf_and_iry_and_f_of_last_year IS 
    $$View materializada com o cruzamento das tabelas inf_cadastral_fi (icf), xpi_funds (xf), btgpactual_funds (bf), funds (f) e investment_return_yearly (iry).

    Verifique a documentação dos campos das tabelas acima como referência.$$;

CREATE INDEX icf_with_xf_and_bf_and_iry_and_f_of_last_year_f_unaccent_idx ON icf_with_xf_and_bf_and_iry_and_f_of_last_year USING gin(private.f_unaccent(icf_denom_social) private.gin_trgm_ops);

CREATE INDEX icf_with_xf_and_bf_and_iry_and_f_of_last_year_f_icf_cnpj_fund ON icf_with_xf_and_bf_and_iry_and_f_of_last_year USING btree (icf_cnpj_fundo COLLATE pg_catalog."default") TABLESPACE pg_default;

CREATE FUNCTION icf_denom_social_unaccented(icf_with_xf_and_bf_and_iry_and_f_of_last_year) RETURNS text AS $$
  SELECT private.f_unaccent($1.icf_denom_social);
$$ LANGUAGE SQL; 

CREATE MATERIALIZED VIEW icf_with_xf_and_iry_of_last_year
AS
SELECT DISTINCT ON (icf_CNPJ_FUNDO) icf_CNPJ_FUNDO as DISTINCT_icf_CNPJ_FUNDO,* 
	FROM inf_cadastral_fi_fullname 
	LEFT JOIN xpi_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = xpi_funds.xf_cnpj
	LEFT JOIN (
		SELECT DISTINCT ON (iry_cnpj_fundo) iry_cnpj_fundo as DISTINCT_iry_cnpj_fundo, * FROM investment_return_yearly ORDER BY iry_cnpj_fundo, iry_dt_comptc DESC
	) AS LAST_YEAR ON inf_cadastral_fi_fullname.icf_cnpj_fundo = LAST_YEAR.iry_cnpj_fundo
WITH DATA;

CREATE INDEX inf_cadastral_fi_with_xpi_and_iry_of_last_year_f_unaccent_idx ON icf_with_xf_and_iry_of_last_year USING gin(private.f_unaccent(icf_denom_social) private.gin_trgm_ops);

CREATE INDEX inf_cadastral_fi_with_xpi_and_iry_of_last_year_f_icf_cnpj_fund ON icf_with_xf_and_iry_of_last_year USING btree (icf_cnpj_fundo COLLATE pg_catalog."default") TABLESPACE pg_default;

CREATE FUNCTION icf_denom_social_unaccented(icf_with_xf_and_iry_of_last_year) RETURNS text AS $$
  SELECT private.f_unaccent($1.icf_denom_social);
$$ LANGUAGE SQL;

COMMENT ON MATERIALIZED VIEW public.icf_with_xf_and_iry_of_last_year IS 
    $$View materializada com o cruzamento das tabelas inf_cadastral_fi (icf), xpi_funds (xf) e investment_return_yearly (iry).

    Verifique a documentação dos campos das tabelas acima como referência.
    
    Atenção: Essa view materializada está depreciada, utilize a view icf_with_xf_and_bf_and_iry_and_f_of_last_year.$$;

CREATE MATERIALIZED VIEW xf_with_irm_timeseries
AS 
SELECT 
	xf_name, 
	icf_classe, 
	last_day_of_month as irm_dt_comptc, 
	COALESCE(irm_accumulated_investment_return,0) as irm_accumulated_investment_return, 
	COALESCE(irm_accumulated_risk,0) as irm_accumulated_risk 
	FROM (SELECT * FROM xpi_funds
			LEFT JOIN inf_cadastral_fi_fullname ON xf_cnpj = icf_cnpj_fundo
			CROSS JOIN (SELECT CAST(date_trunc('month',ts) + interval '1 month - 1 day' AS DATE) AS last_day_of_month
			FROM generate_series(
				(SELECT MIN(irm_dt_comptc)::timestamp::date FROM investment_return_monthly),
				(SELECT MAX(irm_dt_comptc)::timestamp::date FROM investment_return_monthly),
				interval '1 month') as ts) REFERENCE_DATE_SERIES) AS DATE_SERIES	
	LEFT JOIN investment_return_monthly ON irm_cnpj_fundo = DATE_SERIES.xf_cnpj AND DATE_SERIES.last_day_of_month = irm_dt_comptc				
	ORDER BY irm_dt_comptc, xf_name, icf_classe
WITH DATA;