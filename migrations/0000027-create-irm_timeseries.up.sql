DROP MATERIALIZED VIEW xf_with_irm_timeseries;

CREATE MATERIALIZED VIEW irm_timeseries
AS 
SELECT
	f_name,
	f_short_name,
	f_cnpj,	
	icf_classe, 	
	last_day_of_month as irm_dt_comptc,
	COALESCE(irm_accumulated_investment_return,0) as irm_accumulated_investment_return, 
	COALESCE(irm_investment_return_mtd,0) as irm_investment_return_mtd, 
	COALESCE(irm_investment_return_ytd,0) as irm_investment_return_ytd, 
	COALESCE(irm_investment_return_1m,0) as irm_investment_return_1m, 
	COALESCE(irm_investment_return_3m,0) as irm_investment_return_3m, 
	COALESCE(irm_investment_return_6m,0) as irm_investment_return_6m, 
	COALESCE(irm_investment_return_1y,0) as irm_investment_return_1y, 
	COALESCE(irm_investment_return_2y,0) as irm_investment_return_2y, 
	COALESCE(irm_investment_return_3y,0) as irm_investment_return_3y, 
	COALESCE(irm_accumulated_risk,0) as irm_accumulated_risk, 
	COALESCE(irm_risk_mtd,0) as irm_risk_mtd, 
	COALESCE(irm_risk_ytd,0) as irm_risk_ytd, 
	COALESCE(irm_risk_1m,0) as irm_risk_1m, 
	COALESCE(irm_risk_3m,0) as irm_risk_3m, 
	COALESCE(irm_risk_6m,0) as irm_risk_6m,
	COALESCE(irm_risk_1y,0) as irm_risk_1y, 
	COALESCE(irm_risk_2y,0) as irm_risk_2y, 
	COALESCE(irm_risk_3y,0) as irm_risk_3y, 
	COALESCE(irm_cdi_accumulated_sharpe,0) as irm_cdi_accumulated_sharpe, 
	COALESCE(irm_cdi_sharpe_mtd,0) as irm_cdi_sharpe_mtd, 
	COALESCE(irm_cdi_sharpe_ytd,0) as irm_cdi_sharpe_ytd, 
	COALESCE(irm_cdi_sharpe_1m,0) as irm_cdi_sharpe_1m, 
	COALESCE(irm_cdi_sharpe_3m,0) as irm_cdi_sharpe_3m, 
	COALESCE(irm_cdi_sharpe_6m,0) as irm_cdi_sharpe_6m,
	COALESCE(irm_cdi_sharpe_1y,0) as irm_cdi_sharpe_1y, 
	COALESCE(irm_cdi_sharpe_2y,0) as irm_cdi_sharpe_2y, 
	COALESCE(irm_cdi_sharpe_3y,0) as irm_cdi_sharpe_3y,	
	COALESCE(irm_cdi_consistency_mtd,0) as irm_cdi_consistency_mtd, 
	COALESCE(irm_cdi_consistency_ytd,0) as irm_cdi_consistency_ytd, 
	COALESCE(irm_cdi_consistency_1m,0) as irm_cdi_consistency_1m, 
	COALESCE(irm_cdi_consistency_3m,0) as irm_cdi_consistency_3m, 
	COALESCE(irm_cdi_consistency_6m,0) as irm_cdi_consistency_6m,
	COALESCE(irm_cdi_consistency_1y,0) as irm_cdi_consistency_1y, 
	COALESCE(irm_cdi_consistency_2y,0) as irm_cdi_consistency_2y, 
	COALESCE(irm_cdi_consistency_3y,0) as irm_cdi_consistency_3y,
	COALESCE(irm_accumulated_networth,0) as irm_accumulated_networth, 	
	COALESCE(irm_accumulated_quotaholders,0) as irm_accumulated_quotaholders 		
	FROM (SELECT * FROM icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year 
			CROSS JOIN (SELECT CAST(date_trunc('month',ts) + interval '1 month - 1 day' AS DATE) AS last_day_of_month
			FROM generate_series(
				(SELECT MIN(irm_dt_comptc)::timestamp::date FROM investment_return_monthly),
				(SELECT MAX(irm_dt_comptc)::timestamp::date FROM investment_return_monthly),
				interval '1 month') as ts) REFERENCE_DATE_SERIES
		  	WHERE 
				(icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year.xf_id IS NOT NULL OR
				icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year.bf_id IS NOT NULL OR
				icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year.mf_id IS NOT NULL) AND
				icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year.icf_sit <> 'CANCELADA'
	) AS DATE_SERIES					
	LEFT JOIN investment_return_monthly ON irm_cnpj_fundo = DATE_SERIES.f_cnpj AND DATE_SERIES.last_day_of_month = irm_dt_comptc				
	ORDER BY irm_dt_comptc, xf_name, icf_classe
WITH DATA;

CREATE INDEX irm_timeseries_irm_dt_comptc
    ON public.irm_timeseries USING btree
    (irm_dt_comptc)
    TABLESPACE pg_default;

GRANT SELECT ON TABLE public.irm_timeseries TO readonly;