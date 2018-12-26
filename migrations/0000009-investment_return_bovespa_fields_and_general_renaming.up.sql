DROP MATERIALIZED VIEW xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized;

DROP VIEW xpi_funds_with_investment_return_monthly_complete_and_expanded;

DROP FUNCTION icf_denom_social_unaccented(inf_cadastral_fi_with_xpi_and_iryf_of_last_year);

DROP MATERIALIZED VIEW inf_cadastral_fi_with_xpi_and_iryf_of_last_year;

DROP VIEW investment_return_daily_fullname;

ALTER TABLE public.investment_return_daily RENAME COLUMN id TO ird_id;
ALTER TABLE public.investment_return_daily RENAME COLUMN cnpj_fundo TO ird_cnpj_fundo;
ALTER TABLE public.investment_return_daily RENAME COLUMN dt_comptc TO ird_dt_comptc;
ALTER TABLE public.investment_return_daily RENAME COLUMN investment_return TO ird_investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN investment_return_1y TO ird_investment_return_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN investment_return_2y TO ird_investment_return_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN investment_return_3y TO ird_investment_return_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN accumulated_investment_return TO ird_accumulated_investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN risk_1y TO ird_risk_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN risk_2y TO ird_risk_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN risk_3y TO ird_risk_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN accumulated_risk TO ird_accumulated_risk;
ALTER TABLE public.investment_return_daily RENAME COLUMN sharpe_1y TO ird_cdi_sharpe_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN sharpe_2y TO ird_cdi_sharpe_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN sharpe_3y TO ird_cdi_sharpe_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN accumulated_sharpe TO ird_cdi_accumulated_sharpe;
ALTER TABLE public.investment_return_daily RENAME COLUMN consistency_1y TO ird_cdi_consistency_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN consistency_2y TO ird_cdi_consistency_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN consistency_3y TO ird_cdi_consistency_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN networth TO ird_networth;
ALTER TABLE public.investment_return_daily RENAME COLUMN quotaholders TO ird_quotaholders;
ALTER TABLE public.investment_return_daily RENAME COLUMN cdi_investment_return TO ird_cdi_investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN cdi_investment_return_1y TO ird_cdi_investment_return_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN cdi_investment_return_2y TO ird_cdi_investment_return_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN cdi_investment_return_3y TO ird_cdi_investment_return_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN cdi_accumulated_investment_return TO ird_cdi_accumulated_investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN bovespa_investment_return TO ird_bovespa_investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN bovespa_investment_return_1y TO ird_bovespa_investment_return_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN bovespa_investment_return_2y TO ird_bovespa_investment_return_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN bovespa_investment_return_3y TO ird_bovespa_investment_return_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN bovespa_accumulated_investment_return TO ird_bovespa_accumulated_investment_return;
	
ALTER TABLE public.investment_return_daily
    ADD COLUMN ird_bovespa_sharpe_1y DOUBLE PRECISION,
    ADD COLUMN ird_bovespa_sharpe_2y DOUBLE PRECISION,
    ADD COLUMN ird_bovespa_sharpe_3y DOUBLE PRECISION,
    ADD COLUMN ird_bovespa_accumulated_sharpe DOUBLE PRECISION,
    ADD COLUMN ird_bovespa_consistency_1y DOUBLE PRECISION,
    ADD COLUMN ird_bovespa_consistency_2y DOUBLE PRECISION,
    ADD COLUMN ird_bovespa_consistency_3y DOUBLE PRECISION;

DROP VIEW investment_return_monthly_fullname;

ALTER TABLE public.investment_return_monthly RENAME COLUMN id TO irm_id;
ALTER TABLE public.investment_return_monthly RENAME COLUMN cnpj_fundo TO irm_cnpj_fundo;
ALTER TABLE public.investment_return_monthly RENAME COLUMN dt_comptc TO irm_dt_comptc;
ALTER TABLE public.investment_return_monthly RENAME COLUMN investment_return TO irm_investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN investment_return_1y TO irm_investment_return_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN investment_return_2y TO irm_investment_return_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN investment_return_3y TO irm_investment_return_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN accumulated_investment_return TO irm_accumulated_investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN risk TO irm_risk;
ALTER TABLE public.investment_return_monthly RENAME COLUMN risk_1y TO irm_risk_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN risk_2y TO irm_risk_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN risk_3y TO irm_risk_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN accumulated_risk TO irm_accumulated_risk;
ALTER TABLE public.investment_return_monthly RENAME COLUMN sharpe TO irm_cdi_sharpe;
ALTER TABLE public.investment_return_monthly RENAME COLUMN sharpe_1y TO irm_cdi_sharpe_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN sharpe_2y TO irm_cdi_sharpe_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN sharpe_3y TO irm_cdi_sharpe_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN accumulated_sharpe TO irm_cdi_accumulated_sharpe;
ALTER TABLE public.investment_return_monthly RENAME COLUMN consistency_1y TO irm_cdi_consistency_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN consistency_2y TO irm_cdi_consistency_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN consistency_3y TO irm_cdi_consistency_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN networth TO irm_networth;
ALTER TABLE public.investment_return_monthly RENAME COLUMN quotaholders TO irm_quotaholders;
ALTER TABLE public.investment_return_monthly RENAME COLUMN cdi_investment_return TO irm_cdi_investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN cdi_investment_return_1y TO irm_cdi_investment_return_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN cdi_investment_return_2y TO irm_cdi_investment_return_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN cdi_investment_return_3y TO irm_cdi_investment_return_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN cdi_accumulated_investment_return TO irm_cdi_accumulated_investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN bovespa_investment_return TO irm_bovespa_investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN bovespa_investment_return_1y TO irm_bovespa_investment_return_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN bovespa_investment_return_2y TO irm_bovespa_investment_return_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN bovespa_investment_return_3y TO irm_bovespa_investment_return_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN bovespa_accumulated_investment_return TO irm_bovespa_accumulated_investment_return;

ALTER TABLE public.investment_return_monthly    
    ADD COLUMN irm_bovespa_sharpe DOUBLE PRECISION,
    ADD COLUMN irm_bovespa_sharpe_1y DOUBLE PRECISION,
    ADD COLUMN irm_bovespa_sharpe_2y DOUBLE PRECISION,
    ADD COLUMN irm_bovespa_sharpe_3y DOUBLE PRECISION,
    ADD COLUMN irm_bovespa_accumulated_sharpe DOUBLE PRECISION,
    ADD COLUMN irm_bovespa_consistency_1y DOUBLE PRECISION,
    ADD COLUMN irm_bovespa_consistency_2y DOUBLE PRECISION,
    ADD COLUMN irm_bovespa_consistency_3y DOUBLE PRECISION;

DROP VIEW investment_return_yearly_fullname;

ALTER TABLE public.investment_return_yearly RENAME COLUMN id TO iry_id;
ALTER TABLE public.investment_return_yearly RENAME COLUMN cnpj_fundo TO iry_cnpj_fundo;
ALTER TABLE public.investment_return_yearly RENAME COLUMN dt_comptc TO iry_dt_comptc;
ALTER TABLE public.investment_return_yearly RENAME COLUMN investment_return TO iry_investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN investment_return_1y TO iry_investment_return_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN investment_return_2y TO iry_investment_return_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN investment_return_3y TO iry_investment_return_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN accumulated_investment_return TO iry_accumulated_investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN risk TO iry_risk;
ALTER TABLE public.investment_return_yearly RENAME COLUMN risk_1y TO iry_risk_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN risk_2y TO iry_risk_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN risk_3y TO iry_risk_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN accumulated_risk TO iry_accumulated_risk;
ALTER TABLE public.investment_return_yearly RENAME COLUMN sharpe TO iry_cdi_sharpe;
ALTER TABLE public.investment_return_yearly RENAME COLUMN sharpe_1y TO iry_cdi_sharpe_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN sharpe_2y TO iry_cdi_sharpe_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN sharpe_3y TO iry_cdi_sharpe_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN accumulated_sharpe TO iry_cdi_accumulated_sharpe;
ALTER TABLE public.investment_return_yearly RENAME COLUMN consistency_1y TO iry_cdi_consistency_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN consistency_2y TO iry_cdi_consistency_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN consistency_3y TO iry_cdi_consistency_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN networth TO iry_networth;
ALTER TABLE public.investment_return_yearly RENAME COLUMN quotaholders TO iry_quotaholders;
ALTER TABLE public.investment_return_yearly RENAME COLUMN cdi_investment_return TO iry_cdi_investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN cdi_investment_return_1y TO iry_cdi_investment_return_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN cdi_investment_return_2y TO iry_cdi_investment_return_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN cdi_investment_return_3y TO iry_cdi_investment_return_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN cdi_accumulated_investment_return TO iry_cdi_accumulated_investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN bovespa_investment_return TO iry_bovespa_investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN bovespa_investment_return_1y TO iry_bovespa_investment_return_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN bovespa_investment_return_2y TO iry_bovespa_investment_return_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN bovespa_investment_return_3y TO iry_bovespa_investment_return_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN bovespa_accumulated_investment_return TO iry_bovespa_accumulated_investment_return;

ALTER TABLE public.investment_return_yearly    
    ADD COLUMN iry_bovespa_sharpe DOUBLE PRECISION,
    ADD COLUMN iry_bovespa_sharpe_1y DOUBLE PRECISION,
    ADD COLUMN iry_bovespa_sharpe_2y DOUBLE PRECISION,
    ADD COLUMN iry_bovespa_sharpe_3y DOUBLE PRECISION,
    ADD COLUMN iry_bovespa_accumulated_sharpe DOUBLE PRECISION,
    ADD COLUMN iry_bovespa_consistency_1y DOUBLE PRECISION,
    ADD COLUMN iry_bovespa_consistency_2y DOUBLE PRECISION,
    ADD COLUMN iry_bovespa_consistency_3y DOUBLE PRECISION;

DROP VIEW xpi_funds_fullname;

ALTER TABLE public.xpi_funds RENAME COLUMN id TO xf_id;
ALTER TABLE public.xpi_funds RENAME COLUMN CNPJ TO xf_cnpj;
ALTER TABLE public.xpi_funds RENAME COLUMN XPI_ID TO xf_xpi_id;
ALTER TABLE public.xpi_funds RENAME COLUMN FORMAL_RISK TO xf_formal_risk;
ALTER TABLE public.xpi_funds RENAME COLUMN MORNINGSTAR TO xf_morningstar;
ALTER TABLE public.xpi_funds RENAME COLUMN NAME TO xf_name;
ALTER TABLE public.xpi_funds RENAME COLUMN INITIAL_INVESTMENT TO xf_initial_investment;
ALTER TABLE public.xpi_funds RENAME COLUMN REDEMPTION_DELAY_IN_DAYS TO xf_redemption_delay_in_days;
ALTER TABLE public.xpi_funds RENAME COLUMN STATE TO xf_state;
ALTER TABLE public.xpi_funds RENAME COLUMN ADM_FEE TO xf_adm_fee;
ALTER TABLE public.xpi_funds RENAME COLUMN PERF_FEE TO xf_perf_fee;
ALTER TABLE public.xpi_funds RENAME COLUMN BENCHMARK TO xf_benchmark;
ALTER TABLE public.xpi_funds RENAME COLUMN TYPE TO xf_type;

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

UPDATE inf_diario_fi SET pending_statistic_at = NOW() WHERE pending_statistic_at IS NULL