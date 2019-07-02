CREATE MATERIALIZED VIEW public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year
AS
 SELECT DISTINCT ON (inf_cadastral_fi_fullname.icf_cnpj_fundo) inf_cadastral_fi_fullname.icf_cnpj_fundo AS distinct_icf_cnpj_fundo, *
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

GRANT SELECT ON TABLE public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year TO readonly;

CREATE INDEX icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year_common_filter
    ON public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year USING btree
    (iry_dt_comptc, icf_sit COLLATE pg_catalog."default", icf_condom COLLATE pg_catalog."default", icf_fundo_exclusivo COLLATE pg_catalog."default", iry_accumulated_networth)
    TABLESPACE pg_default;
CREATE INDEX icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year_f_icf_cnpj_fund
    ON public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year USING btree
    (icf_cnpj_fundo COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year_f_un_name
    ON public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year USING gin
    (f_unaccented_name COLLATE pg_catalog."default" gin_trgm_ops)
    TABLESPACE pg_default;
CREATE INDEX icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year_f_un_short
    ON public.icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year USING gin
    (f_unaccented_short_name COLLATE pg_catalog."default" gin_trgm_ops)
    TABLESPACE pg_default;