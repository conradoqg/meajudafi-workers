DROP MATERIALIZED VIEW public.irm_timeseries;
DROP MATERIALIZED VIEW public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year;
DROP MATERIALIZED VIEW public.icf_with_xf_and_bf_and_iry_and_f_of_last_year;
DROP MATERIALIZED VIEW public.funds_enhanced;

DROP INDEX xpi_funds_xf_id_xf_cnpj_index;
ALTER TABLE public.xpi_funds DROP CONSTRAINT xpi_funds_pkey;

DELETE FROM public.xpi_funds;

ALTER TABLE public.xpi_funds DROP COLUMN xf_id;
ALTER TABLE public.xpi_funds ADD COLUMN xf_id uuid;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_formal_risk TO xf_risk;
ALTER TABLE public.xpi_funds ALTER COLUMN xf_risk TYPE int4 USING xf_risk::int4;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_initial_investment TO xf_minimal_initial_investment;
ALTER TABLE public.xpi_funds DROP COLUMN xf_state;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_adm_fee TO xf_administration_rate;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_perf_fee TO xf_performance_rate;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_type TO xf_classification_xp;
ALTER TABLE public.xpi_funds DROP COLUMN xf_minimal_movement;
ALTER TABLE public.xpi_funds DROP COLUMN xf_minimal_amount_to_stay;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_investment_quota TO xf_investment_quotation_days;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_rescue_quota TO xf_redemption_quotation_days;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_rescue_financial_settlement TO xf_redemption_settlement_days;
ALTER TABLE public.xpi_funds DROP COLUMN xf_investment_rescue_time;
ALTER TABLE public.xpi_funds DROP COLUMN xf_anbima_rating;
ALTER TABLE public.xpi_funds DROP COLUMN xf_anbima_code;
ALTER TABLE public.xpi_funds DROP COLUMN xf_custody;
ALTER TABLE public.xpi_funds DROP COLUMN xf_auditing;
ALTER TABLE public.xpi_funds DROP COLUMN xf_manager;
ALTER TABLE public.xpi_funds DROP COLUMN xf_administrator;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_startdate TO xf_start_date;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_net_equity_1y TO xf_average_net_equity_12m;
ALTER TABLE public.xpi_funds RENAME COLUMN xf_max_adm_fee TO xf_max_administration_rate;
ALTER TABLE public.xpi_funds DROP COLUMN xf_tax_text;
ALTER TABLE public.xpi_funds DROP COLUMN xf_iof_text;
ALTER TABLE public.xpi_funds DROP COLUMN xf_date;
ALTER TABLE public.xpi_funds ADD xf_trading_account integer NULL;
ALTER TABLE public.xpi_funds ADD xf_category_code integer NULL;
ALTER TABLE public.xpi_funds ADD xf_risk_genius integer NULL;
ALTER TABLE public.xpi_funds ADD xf_risk_genius_suitability text NULL;
ALTER TABLE public.xpi_funds ADD xf_risk_genius_color text NULL;
ALTER TABLE public.xpi_funds ADD xf_risk_genius_description text NULL;
ALTER TABLE public.xpi_funds ADD xf_profitability_month double precision NULL;
ALTER TABLE public.xpi_funds ADD xf_profitability_12 double precision NULL;
ALTER TABLE public.xpi_funds ADD xf_profitability_year double precision NULL;
ALTER TABLE public.xpi_funds ADD xf_funding_blocked boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_funding_block_justification text NULL;
ALTER TABLE public.xpi_funds ADD xf_has_lockup boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_classification_cvm text NULL;
ALTER TABLE public.xpi_funds ADD xf_max_performance_rate double precision NULL;
ALTER TABLE public.xpi_funds ADD xf_equity integer NULL;
ALTER TABLE public.xpi_funds ADD xf_category_equity integer NULL;
ALTER TABLE public.xpi_funds ADD xf_block_us_person boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_is_suggested boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_allow_employee boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_allow_general boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_allow_linked boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_only_qualified boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_only_professional boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_document_promotional_material text NULL;
ALTER TABLE public.xpi_funds ADD xf_document_regulation text NULL;
ALTER TABLE public.xpi_funds ADD xf_alkanza boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_category_name text NULL;
ALTER TABLE public.xpi_funds ADD xf_management_policy text NULL;
ALTER TABLE public.xpi_funds ADD xf_initials text NULL;
ALTER TABLE public.xpi_funds ADD xf_redemption_blocked boolean NULL;
ALTER TABLE public.xpi_funds ADD xf_objective text NULL;
ALTER TABLE public.xpi_funds ADD xf_objective_commercial text NULL;
ALTER TABLE public.xpi_funds ADD xf_administration_transfer_rate double precision NULL;
ALTER TABLE public.xpi_funds ADD xf_return_on_assets double precision NULL;

ALTER TABLE public.xpi_funds ADD PRIMARY KEY (xf_id);
CREATE INDEX xpi_funds_xf_id_xf_cnpj_index ON public.xpi_funds USING btree (xf_id, xf_cnpj);

DROP TRIGGER audit_trigger_row on public.xpi_funds;
DROP TRIGGER audit_trigger_stm on public.xpi_funds;

SELECT audit.audit_table('xpi_funds',  'true', 'false', '{xf_date, xf_net_equity, xf_average_net_equity_12m}'::text[]);

CREATE MATERIALIZED VIEW public.funds_enhanced
TABLESPACE pg_default
AS SELECT 
    funds.*,
    btgpactual_funds.*,
    xpi_funds.*,
    modalmais_funds.*,
    inf_cadastral_fi_fullname.*
   FROM funds
     LEFT JOIN btgpactual_funds ON funds.f_cnpj = btgpactual_funds.bf_cnpj
     LEFT JOIN xpi_funds ON funds.f_cnpj = xpi_funds.xf_cnpj
     LEFT JOIN modalmais_funds ON funds.f_cnpj = modalmais_funds.mf_cnpj
     LEFT JOIN inf_cadastral_fi_fullname ON funds.f_cnpj = inf_cadastral_fi_fullname.icf_cnpj_fundo
WITH DATA;

CREATE MATERIALIZED VIEW public.icf_with_xf_and_bf_and_iry_and_f_of_last_year
TABLESPACE pg_default
AS SELECT DISTINCT ON (inf_cadastral_fi_fullname.icf_cnpj_fundo) inf_cadastral_fi_fullname.icf_cnpj_fundo AS distinct_icf_cnpj_fundo, *    
   FROM inf_cadastral_fi_fullname
     LEFT JOIN xpi_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = xpi_funds.xf_cnpj
     LEFT JOIN btgpactual_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = btgpactual_funds.bf_cnpj
     LEFT JOIN funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = funds.f_cnpj
     LEFT JOIN ( SELECT DISTINCT ON (investment_return_yearly.iry_cnpj_fundo) investment_return_yearly.iry_cnpj_fundo AS distinct_iry_cnpj_fundo, *
           FROM investment_return_yearly
          ORDER BY investment_return_yearly.iry_cnpj_fundo, investment_return_yearly.iry_dt_comptc DESC) last_year ON inf_cadastral_fi_fullname.icf_cnpj_fundo = last_year.iry_cnpj_fundo
WITH DATA;

COMMENT ON MATERIALIZED VIEW public.icf_with_xf_and_bf_and_iry_and_f_of_last_year IS 
    $$View materializada com o cruzamento das tabelas inf_cadastral_fi (icf), xpi_funds (xf), btgpactual_funds (bf), funds (f) e investment_return_yearly (iry).

    Verifique a documentação dos campos das tabelas acima como referência.$$;

CREATE INDEX icf_with_xf_and_bf_and_iry_and_f_of_last_year_common_filter ON public.icf_with_xf_and_bf_and_iry_and_f_of_last_year USING btree (iry_dt_comptc, icf_sit, icf_condom, icf_fundo_exclusivo, iry_accumulated_networth);
CREATE INDEX icf_with_xf_and_bf_and_iry_and_f_of_last_year_f_icf_cnpj_fund ON public.icf_with_xf_and_bf_and_iry_and_f_of_last_year USING btree (icf_cnpj_fundo);
CREATE INDEX icf_with_xf_and_bf_and_iry_and_f_of_last_year_f_unaccented_name ON public.icf_with_xf_and_bf_and_iry_and_f_of_last_year USING gin (f_unaccented_name gin_trgm_ops);
CREATE INDEX icf_with_xf_and_bf_and_iry_and_f_of_last_year_f_unaccented_shor ON public.icf_with_xf_and_bf_and_iry_and_f_of_last_year USING gin (f_unaccented_short_name gin_trgm_ops);

CREATE MATERIALIZED VIEW public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year
TABLESPACE pg_default
AS SELECT DISTINCT ON (inf_cadastral_fi_fullname.icf_cnpj_fundo) inf_cadastral_fi_fullname.icf_cnpj_fundo AS distinct_icf_cnpj_fundo, *
   FROM inf_cadastral_fi_fullname
     LEFT JOIN xpi_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = xpi_funds.xf_cnpj
     LEFT JOIN btgpactual_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = btgpactual_funds.bf_cnpj
     LEFT JOIN modalmais_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = modalmais_funds.mf_cnpj
     LEFT JOIN funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = funds.f_cnpj
     LEFT JOIN ( SELECT DISTINCT ON (investment_return_yearly.iry_cnpj_fundo) investment_return_yearly.iry_cnpj_fundo AS distinct_iry_cnpj_fundo, *            
           FROM investment_return_yearly
          ORDER BY investment_return_yearly.iry_cnpj_fundo, investment_return_yearly.iry_dt_comptc DESC) last_year ON inf_cadastral_fi_fullname.icf_cnpj_fundo = last_year.iry_cnpj_fundo
WITH DATA;

COMMENT ON MATERIALIZED VIEW public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year
    IS 'View materializada com o cruzamento das tabelas inf_cadastral_fi (icf), xpi_funds (xf), btgpactual_funds (bf), modalmais_funds (mf), funds (f) e investment_return_yearly (iry).

    Verifique a documentação dos campos das tabelas acima como referência.';

CREATE INDEX icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year_common_fil ON public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year USING btree (iry_dt_comptc, icf_sit, icf_condom, icf_fundo_exclusivo, iry_accumulated_networth);
CREATE INDEX icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year_f_icf_cnpj ON public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year USING btree (icf_cnpj_fundo);
CREATE INDEX icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year_f_un_name ON public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year USING gin (f_unaccented_name gin_trgm_ops);
CREATE INDEX icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year_f_un_short ON public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year USING gin (f_unaccented_short_name gin_trgm_ops);

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