DROP VIEW public.changed_funds;

CREATE OR REPLACE VIEW public.changed_funds AS
 SELECT logged_actions.table_name,
    logged_actions.action,
    logged_actions.action_tstamp_stm,    
    funds.f_cnpj,
    funds.f_short_name,	
	json_strip_nulls(json_build_object(
		'xf_state', changed_fields -> 'xf_state',		
		'xf_formal_risk', changed_fields -> 'xf_formal_risk',		
		'xf_initial_investment', changed_fields -> 'xf_initial_investment',
		'xf_rescue_financial_settlement', changed_fields -> 'xf_rescue_financial_settlement',
		'bf_is_blacklist', changed_fields -> 'bf_is_blacklist',
		'bf_inactive', changed_fields -> 'bf_inactive',
		'bf_risk_name', changed_fields -> 'bf_risk_name',
		'bf_minimum_initial_investment', changed_fields -> 'bf_minimum_initial_investment',
		'bf_rescue_financial_settlement', changed_fields -> 'bf_rescue_financial_settlement',
		'bf_investor_type', changed_fields -> 'bf_investor_type',
		'mf_risk_level', changed_fields -> 'mf_risk_level',
		'mf_minimum_initial_investment', changed_fields -> 'mf_minimum_initial_investment',
		'mf_rescue_quota', changed_fields -> 'mf_rescue_quota',
		'mf_active', changed_fields -> 'mf_active'
	)) as changed_fields,
	json_strip_nulls(json_build_object(
		'xf_state', row_data -> 'xf_state',		
		'xf_formal_risk', row_data -> 'xf_formal_risk',		
		'xf_initial_investment', row_data -> 'xf_initial_investment',
		'xf_rescue_financial_settlement', row_data -> 'xf_rescue_financial_settlement',
		'bf_is_blacklist', row_data -> 'bf_is_blacklist',
		'bf_inactive', row_data -> 'bf_inactive',
		'bf_risk_name', row_data -> 'bf_risk_name',
		'bf_minimum_initial_investment', row_data -> 'bf_minimum_initial_investment',
		'bf_rescue_financial_settlement', row_data -> 'bf_rescue_financial_settlement',
		'bf_investor_type', row_data -> 'bf_investor_type',
		'mf_risk_level', row_data -> 'mf_risk_level',
		'mf_minimum_initial_investment', row_data -> 'mf_minimum_initial_investment',
		'mf_rescue_quota', row_data -> 'mf_rescue_quota',
		'mf_active', row_data -> 'mf_active'
	)) as row_data
   FROM audit.logged_actions
     LEFT JOIN funds ON (logged_actions.row_data -> 'bf_cnpj'::text) = funds.f_cnpj OR (logged_actions.row_data -> 'xf_cnpj'::text) = funds.f_cnpj OR (logged_actions.row_data -> 'mf_cnpj'::text) = funds.f_cnpj
  		WHERE (logged_actions.row_data -> 'bf_cnpj'::text) IS NOT NULL OR (logged_actions.row_data -> 'xf_cnpj'::text) IS NOT NULL OR (logged_actions.row_data -> 'mf_cnpj'::text) IS NOT NULL;

GRANT SELECT ON TABLE public.changed_funds TO readonly;