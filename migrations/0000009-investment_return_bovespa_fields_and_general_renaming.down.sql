DROP MATERIALIZED VIEW xf_with_irm_timeseries;

DROP FUNCTION icf_denom_social_unaccented(icf_with_xf_and_iry_of_last_year);

DROP MATERIALIZED VIEW icf_with_xf_and_iry_of_last_year;

ALTER TABLE public.investment_return_daily RENAME COLUMN ird_id TO id;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cnpj_fundo TO cnpj_fundo;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_dt_comptc TO dt_comptc;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_investment_return TO investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_investment_return_1y TO investment_return_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_investment_return_2y TO investment_return_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_investment_return_3y TO investment_return_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_accumulated_investment_return TO accumulated_investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_risk_1y TO risk_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_risk_2y TO risk_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_risk_3y TO risk_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_accumulated_risk TO accumulated_risk;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_sharpe_1y TO sharpe_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_sharpe_2y TO sharpe_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_sharpe_3y TO sharpe_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_accumulated_sharpe TO accumulated_sharpe;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_consistency_1y TO consistency_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_consistency_2y TO consistency_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_consistency_3y TO consistency_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_networth TO networth;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_quotaholders TO quotaholders;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_investment_return TO cdi_investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_investment_return_1y TO cdi_investment_return_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_investment_return_2y TO cdi_investment_return_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_investment_return_3y TO cdi_investment_return_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_cdi_accumulated_investment_return TO cdi_accumulated_investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_bovespa_investment_return TO bovespa_investment_return;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_bovespa_investment_return_1y TO bovespa_investment_return_1y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_bovespa_investment_return_2y TO bovespa_investment_return_2y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_bovespa_investment_return_3y TO bovespa_investment_return_3y;
ALTER TABLE public.investment_return_daily RENAME COLUMN ird_bovespa_accumulated_investment_return TO bovespa_accumulated_investment_return;
	
ALTER TABLE public.investment_return_daily
    DROP COLUMN ird_bovespa_risk_1y,
    DROP COLUMN ird_bovespa_risk_2y,
    DROP COLUMN ird_bovespa_risk_3y,
    DROP COLUMN ird_bovespa_accumulated_risk,
    DROP COLUMN ird_bovespa_sharpe_1y,
    DROP COLUMN ird_bovespa_sharpe_2y,
    DROP COLUMN ird_bovespa_sharpe_3y,
    DROP COLUMN ird_bovespa_accumulated_sharpe,
    DROP COLUMN ird_bovespa_consistency_1y,
    DROP COLUMN ird_bovespa_consistency_2y,
    DROP COLUMN ird_bovespa_consistency_3y;

ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_id TO id;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cnpj_fundo TO cnpj_fundo;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_dt_comptc TO dt_comptc;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_investment_return TO investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_investment_return_1y TO investment_return_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_investment_return_2y TO investment_return_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_investment_return_3y TO investment_return_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_accumulated_investment_return TO accumulated_investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_risk TO risk;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_risk_1y TO risk_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_risk_2y TO risk_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_risk_3y TO risk_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_accumulated_risk TO accumulated_risk;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_sharpe TO sharpe;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_sharpe_1y TO sharpe_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_sharpe_2y TO sharpe_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_sharpe_3y TO sharpe_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_accumulated_sharpe TO accumulated_sharpe;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_consistency_1y TO consistency_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_consistency_2y TO consistency_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_consistency_3y TO consistency_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_networth TO networth;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_quotaholders TO quotaholders;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_investment_return TO cdi_investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_investment_return_1y TO cdi_investment_return_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_investment_return_2y TO cdi_investment_return_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_investment_return_3y TO cdi_investment_return_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_cdi_accumulated_investment_return TO cdi_accumulated_investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_bovespa_investment_return TO bovespa_investment_return;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_bovespa_investment_return_1y TO bovespa_investment_return_1y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_bovespa_investment_return_2y TO bovespa_investment_return_2y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_bovespa_investment_return_3y TO bovespa_investment_return_3y;
ALTER TABLE public.investment_return_monthly RENAME COLUMN irm_bovespa_accumulated_investment_return TO bovespa_accumulated_investment_return;

ALTER TABLE public.investment_return_monthly
    DROP COLUMN irm_bovespa_risk_1y,
    DROP COLUMN irm_bovespa_risk_2y,
    DROP COLUMN irm_bovespa_risk_3y,
    DROP COLUMN irm_bovespa_accumulated_risk,
    DROP COLUMN irm_bovespa_sharpe_1y,
    DROP COLUMN irm_bovespa_sharpe_2y,
    DROP COLUMN irm_bovespa_sharpe_3y,
    DROP COLUMN irm_bovespa_accumulated_sharpe,
    DROP COLUMN irm_bovespa_consistency_1y,
    DROP COLUMN irm_bovespa_consistency_2y,
    DROP COLUMN irm_bovespa_consistency_3y;

ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_id TO id;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cnpj_fundo TO cnpj_fundo;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_dt_comptc TO dt_comptc;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_investment_return TO investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_investment_return_1y TO investment_return_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_investment_return_2y TO investment_return_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_investment_return_3y TO investment_return_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_accumulated_investment_return TO accumulated_investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_risk TO risk;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_risk_1y TO risk_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_risk_2y TO risk_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_risk_3y TO risk_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_accumulated_risk TO accumulated_risk;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_sharpe TO sharpe;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_sharpe_1y TO sharpe_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_sharpe_2y TO sharpe_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_sharpe_3y TO sharpe_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_accumulated_sharpe TO accumulated_sharpe;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_consistency_1y TO consistency_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_consistency_2y TO consistency_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_consistency_3y TO consistency_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_networth TO networth;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_quotaholders TO quotaholders;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_investment_return TO cdi_investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_investment_return_1y TO cdi_investment_return_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_investment_return_2y TO cdi_investment_return_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_investment_return_3y TO cdi_investment_return_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_cdi_accumulated_investment_return TO cdi_accumulated_investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_bovespa_investment_return TO bovespa_investment_return;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_bovespa_investment_return_1y TO bovespa_investment_return_1y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_bovespa_investment_return_2y TO bovespa_investment_return_2y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_bovespa_investment_return_3y TO bovespa_investment_return_3y;
ALTER TABLE public.investment_return_yearly RENAME COLUMN iry_bovespa_accumulated_investment_return TO bovespa_accumulated_investment_return;

ALTER TABLE public.investment_return_yearly
    DROP COLUMN iry_bovespa_risk_1y,
    DROP COLUMN iry_bovespa_risk_2y,
    DROP COLUMN iry_bovespa_risk_3y,
    DROP COLUMN iry_bovespa_accumulated_risk,
    DROP COLUMN iry_bovespa_sharpe_1y,
    DROP COLUMN iry_bovespa_sharpe_2y,
    DROP COLUMN iry_bovespa_sharpe_3y,
    DROP COLUMN iry_bovespa_accumulated_sharpe,
    DROP COLUMN iry_bovespa_consistency_1y,
    DROP COLUMN iry_bovespa_consistency_2y,
    DROP COLUMN iry_bovespa_consistency_3y;

ALTER TABLE public.xpi_funds RENAME COLUMN xf_id TO id;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_CNPJ TO cnpj;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_XPI_ID TO xpi_id;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_FORMAL_RISK TO formal_risk;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_MORNINGSTAR TO morningstar;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_NAME TO name;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_INITIAL_INVESTMENT TO initial_investment;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_REDEMPTION_DELAY_IN_DAYS TO redemption_delay_in_days;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_STATE TO state;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_ADM_FEE TO adm_fee;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_PERF_FEE TO perf_fee;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_BENCHMARK TO benchmark;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_TYPE TO type;

CREATE OR REPLACE VIEW investment_return_daily_fullname AS
SELECT
    id as ird_id,
    cnpj_fundo as ird_cnpj_fundo,
    dt_comptc as ird_dt_comptc,
    investment_return as ird_investment_return,	
    investment_return_1y as ird_investment_return_1y,
    investment_return_2y as ird_investment_return_2y,
    investment_return_3y as ird_investment_return_3y,
    accumulated_investment_return as ird_accumulated_investment_return, 
    risk_1y as ird_risk_1y,
    risk_2y as ird_risk_2y,
    risk_3y as ird_risk_3y,
    accumulated_risk as ird_accumulated_risk,
    sharpe_1y as ird_sharpe_1y,
    sharpe_2y as ird_sharpe_2y,
    sharpe_3y as ird_sharpe_3y,
    accumulated_sharpe as ird_accumulated_sharpe,    
    consistency_1y as ird_consistency_1y,
    consistency_2y as ird_consistency_2y,
    consistency_3y as ird_consistency_3y,
    networth as ird_networth,
    quotaholders as ird_quotaholders,
    cdi_investment_return as ird_cdi_investment_return,
    cdi_investment_return_1y as ird_cdi_investment_return_1y,
    cdi_investment_return_2y as ird_cdi_investment_return_2y,
    cdi_investment_return_3y as ird_cdi_investment_return_3y,
    cdi_accumulated_investment_return as ird_cdi_accumulated_investment_return,
    bovespa_investment_return as ird_bovespa_investment_return,
    bovespa_investment_return_1y as ird_bovespa_investment_return_1y,
    bovespa_investment_return_2y as ird_bovespa_investment_return_2y,
    bovespa_investment_return_3y as ird_bovespa_investment_return_3y,
    bovespa_accumulated_investment_return as ird_bovespa_accumulated_investment_return
FROM investment_return_daily;

CREATE OR REPLACE VIEW investment_return_monthly_fullname AS
SELECT
    id as irm_id,
    cnpj_fundo as irm_cnpj_fundo,
    dt_comptc as irm_dt_comptc,
    investment_return as irm_investment_return,
    investment_return_1y as irm_investment_return_1y,
    investment_return_2y as irm_investment_return_2y,
    investment_return_3y as irm_investment_return_3y,
    accumulated_investment_return as irm_accumulated_investment_return,
    risk as irm_risk,
    risk_1y as irm_risk_1y,
    risk_2y as irm_risk_2y,
    risk_3y as irm_risk_3y,
    accumulated_risk as irm_accumulated_risk,
    sharpe as irm_sharpe,
    sharpe_1y as irm_sharpe_1y,
    sharpe_2y as irm_sharpe_2y,
    sharpe_3y as irm_sharpe_3y,
    accumulated_sharpe as irm_accumulated_sharpe,
    consistency_1y as irm_consistency_1y,
    consistency_2y as irm_consistency_2y,
    consistency_3y as irm_consistency_3y,
    networth as irm_networth,
    quotaholders as irm_quotaholders,
    cdi_investment_return as irm_cdi_investment_return,
    cdi_investment_return_1y as irm_cdi_investment_return_1y,
    cdi_investment_return_2y as irm_cdi_investment_return_2y,
    cdi_investment_return_3y as irm_cdi_investment_return_3y,
    cdi_accumulated_investment_return as ird_cdi_accumulated_investment_return,
    bovespa_investment_return as ird_bovespa_investment_return,
    bovespa_investment_return_1y as ird_bovespa_investment_return_1y,
    bovespa_investment_return_2y as ird_bovespa_investment_return_2y,
    bovespa_investment_return_3y as ird_bovespa_investment_return_3y,
    bovespa_accumulated_investment_return as ird_bovespa_accumulated_investment_return
FROM investment_return_monthly;

CREATE OR REPLACE VIEW investment_return_yearly_fullname AS
SELECT 
    id as iry_id,
    cnpj_fundo as iry_cnpj_fundo,
    dt_comptc as iry_dt_comptc,
    investment_return as iry_investment_return,	
    investment_return_1y as iry_investment_return_1y,
    investment_return_2y as iry_investment_return_2y,
    investment_return_3y as iry_investment_return_3y,
    accumulated_investment_return as iry_accumulated_investment_return,
    risk as iry_risk,
    risk_1y as iry_risk_1y,
    risk_2y as iry_risk_2y,
    risk_3y as iry_risk_3y,
    accumulated_risk as iry_accumulated_risk,
    sharpe as iry_sharpe,
    sharpe_1y as iry_sharpe_1y,
    sharpe_2y as iry_sharpe_2y,
    sharpe_3y as iry_sharpe_3y,
    accumulated_sharpe as iry_accumulated_sharpe,
    consistency_1y as iry_consistency_1y,
    consistency_2y as iry_consistency_2y,
    consistency_3y as iry_consistency_3y,
    networth as iry_networth,
    quotaholders as iry_quotaholders,
    cdi_investment_return as iry_cdi_investment_return,
    cdi_investment_return_1y as iry_cdi_investment_return_1y,
    cdi_investment_return_2y as iry_cdi_investment_return_2y,
    cdi_investment_return_3y as iry_cdi_investment_return_3y,
    cdi_accumulated_investment_return as iry_cdi_accumulated_investment_return,
    bovespa_investment_return as ird_bovespa_investment_return,
    bovespa_investment_return_1y as ird_bovespa_investment_return_1y,
    bovespa_investment_return_2y as ird_bovespa_investment_return_2y,
    bovespa_investment_return_3y as ird_bovespa_investment_return_3y,
    bovespa_accumulated_investment_return as ird_bovespa_accumulated_investment_return
FROM investment_return_yearly;

CREATE VIEW xpi_funds_fullname
AS
SELECT 
    id as xf_id, 
    CNPJ as xf_CNPJ, 
    XPI_ID as xf_XPI_ID, 
    FORMAL_RISK as xf_FORMAL_RISK, 
    MORNINGSTAR as xf_MORNINGSTAR, 
    NAME as xf_NAME, 
    INITIAL_INVESTMENT as xf_INITIAL_INVESTMENT, 
    REDEMPTION_DELAY_IN_DAYS as xf_REDEMPTION_DELAY_IN_DAYS,
    STATE as xf_STATE,
    ADM_FEE as xf_ADM_FEE,
    PERF_FEE as xf_PERF_FEE,
    BENCHMARK as xf_BENCHMARK,
    TYPE as xf_TYPE
	FROM xpi_funds;

CREATE VIEW xpi_funds_with_investment_return_monthly_complete_and_expanded
AS 
SELECT 
	xf_name, 
	icf_classe, 
	last_day_of_month as irm_dt_comptc, 
	COALESCE(irm_accumulated_investment_return,0) as irm_accumulated_investment_return, 
	COALESCE(irm_accumulated_risk,0) as irm_accumulated_risk 
	FROM (SELECT * FROM xpi_funds_fullname
			LEFT JOIN inf_cadastral_fi_fullname ON xf_cnpj = icf_cnpj_fundo
			CROSS JOIN (SELECT CAST(date_trunc('month',ts) + interval '1 month - 1 day' AS DATE) AS last_day_of_month
			FROM generate_series(
				(SELECT MIN(irm_dt_comptc)::timestamp::date FROM investment_return_monthly_fullname),
				(SELECT MAX(irm_dt_comptc)::timestamp::date FROM investment_return_monthly_fullname),
				interval '1 month') as ts) REFERENCE_DATE_SERIES) AS DATE_SERIES	
	LEFT JOIN investment_return_monthly_fullname ON irm_cnpj_fundo = DATE_SERIES.xf_cnpj AND DATE_SERIES.last_day_of_month = irm_dt_comptc				
	ORDER BY irm_dt_comptc, xf_name, icf_classe;

CREATE MATERIALIZED VIEW xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized
AS
SELECT * FROM xpi_funds_with_investment_return_monthly_complete_and_expanded
WITH DATA;

CREATE MATERIALIZED VIEW inf_cadastral_fi_with_xpi_and_iryf_of_last_year
AS
SELECT DISTINCT ON (icf_CNPJ_FUNDO) icf_CNPJ_FUNDO as DISTINCT_icf_CNPJ_FUNDO,* 
	FROM inf_cadastral_fi_fullname 
	LEFT JOIN xpi_funds_fullname ON inf_cadastral_fi_fullname.icf_cnpj_fundo = xpi_funds_fullname.xf_cnpj
	LEFT JOIN (
		SELECT DISTINCT ON (iry_cnpj_fundo) iry_cnpj_fundo as DISTINCT_iry_cnpj_fundo, * FROM investment_return_yearly_fullname ORDER BY iry_cnpj_fundo, iry_dt_comptc DESC
	) AS LAST_YEAR ON inf_cadastral_fi_fullname.icf_cnpj_fundo = LAST_YEAR.iry_cnpj_fundo
WITH DATA;

CREATE OR REPLACE FUNCTION icf_denom_social_unaccented(inf_cadastral_fi_with_xpi_and_iryf_of_last_year) RETURNS text AS $$
  SELECT private.f_unaccent($1.icf_denom_social);
$$ LANGUAGE SQL;

CREATE INDEX IF NOT EXISTS inf_cadastral_fi_with_xpi_and_iryf_of_last_year_f_unaccent_idx ON inf_cadastral_fi_with_xpi_and_iryf_of_last_year USING gin(private.f_unaccent(icf_denom_social) private.gin_trgm_ops);

CREATE INDEX IF NOT EXISTS inf_cadastral_fi_with_xpi_and_iryf_of_last_year_f_icf_cnpj_fund ON inf_cadastral_fi_with_xpi_and_iryf_of_last_year USING btree (icf_cnpj_fundo COLLATE pg_catalog."default") TABLESPACE pg_default;