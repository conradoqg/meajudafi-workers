DROP MATERIALIZED VIEW irm_timeseries;

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