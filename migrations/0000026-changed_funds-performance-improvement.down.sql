DROP VIEW public.changed_funds;

CREATE OR REPLACE VIEW public.changed_funds AS
 SELECT logged_actions.table_name,
    logged_actions.action,
    logged_actions.action_tstamp_stm,
    logged_actions.changed_fields,
    logged_actions.row_data,
    funds.f_cnpj,
    funds.f_short_name
   FROM audit.logged_actions
     LEFT JOIN funds ON (logged_actions.row_data -> 'bf_cnpj'::text) = funds.f_cnpj OR (logged_actions.row_data -> 'xf_cnpj'::text) = funds.f_cnpj OR (logged_actions.row_data -> 'mf_cnpj'::text) = funds.f_cnpj
  WHERE (logged_actions.row_data -> 'bf_cnpj'::text) IS NOT NULL OR (logged_actions.row_data -> 'xf_cnpj'::text) IS NOT NULL OR (logged_actions.row_data -> 'mf_cnpj'::text) IS NOT NULL;
  
  GRANT SELECT ON TABLE public.changed_funds TO readonly;