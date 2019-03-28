DROP TRIGGER audit_trigger_row ON btgpactual_funds;
DROP TRIGGER audit_trigger_stm ON btgpactual_funds;
DROP TRIGGER audit_trigger_row ON xpi_funds;
DROP TRIGGER audit_trigger_stm ON xpi_funds;

DROP VIEW changed_funds;

ALTER TABLE public.xpi_funds
    DROP COLUMN xf_date;
	
ALTER TABLE public.btgpactual_funds
    DROP COLUMN bf_date;

SELECT audit.audit_table('btgpactual_funds', 'true', 'false', '{bf_date, xf_date, xf_net_equity, xf_net_equity_1y}'::text[]);
SELECT audit.audit_table('xpi_funds',  'true', 'false', '{bf_date, xf_date, xf_net_equity, xf_net_equity_1y}'::text[]);