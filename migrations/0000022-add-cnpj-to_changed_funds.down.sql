DROP VIEW changed_funds;

CREATE VIEW changed_funds AS
	SELECT table_name, action, changed_fields, row_data, funds.f_short_name FROM audit.logged_actions
		LEFT JOIN funds ON logged_actions.row_data->'bf_cnpj' = funds.f_cnpj OR logged_actions.row_data->'xf_cnpj' = funds.f_cnpj	
		WHERE logged_actions.row_data->'bf_cnpj' IS NOT NULL OR logged_actions.row_data->'xf_cnpj' IS NOT NULL;

DROP MATERIALIZED VIEW funds_enhanced;

CREATE MATERIALIZED VIEW funds_enhanced AS
	SELECT * FROM funds
		LEFT JOIN btgpactual_funds ON funds.f_cnpj = btgpactual_funds.bf_cnpj
		LEFT JOIN xpi_funds ON funds.f_cnpj = xpi_funds.xf_cnpj
		LEFT JOIN inf_cadastral_fi ON funds.f_cnpj = inf_cadastral_fi.cnpj_fundo
WITH DATA;
