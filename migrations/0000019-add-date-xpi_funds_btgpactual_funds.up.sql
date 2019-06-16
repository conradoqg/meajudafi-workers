DROP TRIGGER audit_trigger_row ON btgpactual_funds;
DROP TRIGGER audit_trigger_stm ON btgpactual_funds;
DROP TRIGGER audit_trigger_row ON xpi_funds;
DROP TRIGGER audit_trigger_stm ON xpi_funds;

ALTER ROLE postgres
    SET search_path TO public,private;

ALTER TABLE public.xpi_funds
    ADD COLUMN xf_date date;
	
ALTER TABLE public.btgpactual_funds
    ADD COLUMN bf_date date;

SET search_path to public,private;

CREATE VIEW changed_funds AS
	SELECT table_name, action, action_tstamp_stm, changed_fields, row_data, funds.f_short_name FROM audit.logged_actions
		LEFT JOIN funds ON logged_actions.row_data->'bf_cnpj' = funds.f_cnpj OR logged_actions.row_data->'xf_cnpj' = funds.f_cnpj	
		WHERE logged_actions.row_data->'bf_cnpj' IS NOT NULL OR logged_actions.row_data->'xf_cnpj' IS NOT NULL;

UPDATE xpi_funds SET xf_date = NOW();
UPDATE btgpactual_funds SET bf_date = NOW();

SELECT audit.audit_table('btgpactual_funds', 'true', 'false');
SELECT audit.audit_table('xpi_funds',  'true', 'false');