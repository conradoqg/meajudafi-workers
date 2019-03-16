ALTER TABLE public.xpi_funds
    ADD COLUMN xf_date date;
	
ALTER TABLE public.btgpactual_funds
    ADD COLUMN bf_date date;

CREATE VIEW changed_funds AS
	SELECT table_name, action, changed_fields, row_data, funds.f_short_name FROM audit.logged_actions
		LEFT JOIN funds ON logged_actions.row_data->'bf_cnpj' = funds.f_cnpj OR logged_actions.row_data->'xf_cnpj' = funds.f_cnpj	
		WHERE logged_actions.row_data->'bf_cnpj' IS NOT NULL OR logged_actions.row_data->'xf_cnpj' IS NOT NULL;
	
UPDATE xpi_funds SET xf_date = NOW();
UPDATE btgpactual_funds SET bf_date = NOW();