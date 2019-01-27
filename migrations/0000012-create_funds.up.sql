CREATE TABLE funds (
    f_id UUID NOT NULL UNIQUE,
    f_cnpj TEXT NOT NULL UNIQUE,
    f_short_name TEXT,
    f_name TEXT,
    PRIMARY KEY(f_id, f_cnpj)
);

COMMENT ON TABLE public.funds IS 
    $$Dados trabalhados adicionais de fundos.$$;
COMMENT ON COLUMN public.funds.f_id IS 'ID de registro do fundo';
COMMENT ON COLUMN public.funds.f_cnpj IS 'CNPJ do fundo';
COMMENT ON COLUMN public.funds.f_short_name IS 'Nome curto do fundo';
COMMENT ON COLUMN public.funds.f_name IS 'Nome do fundo';

CREATE INDEX funds_f_cnpj_index
    ON funds USING btree
    (f_cnpj ASC);

CREATE MATERIALIZED VIEW icf_with_xf_and_iry_and_f_of_last_year
AS
SELECT DISTINCT ON (icf_CNPJ_FUNDO) icf_CNPJ_FUNDO as DISTINCT_icf_CNPJ_FUNDO,* 
	FROM inf_cadastral_fi_fullname 
	LEFT JOIN xpi_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = xpi_funds.xf_cnpj
    LEFT JOIN funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = funds.f_cnpj
	LEFT JOIN (
		SELECT DISTINCT ON (iry_cnpj_fundo) iry_cnpj_fundo as DISTINCT_iry_cnpj_fundo, * FROM investment_return_yearly ORDER BY iry_cnpj_fundo, iry_dt_comptc DESC
	) AS LAST_YEAR ON inf_cadastral_fi_fullname.icf_cnpj_fundo = LAST_YEAR.iry_cnpj_fundo
WITH DATA;

COMMENT ON MATERIALIZED VIEW public.icf_with_xf_and_iry_and_f_of_last_year IS 
    $$View materializada com o cruzamento das tabelas inf_cadastral_fi (icf), xpi_funds (xf), funds (f) e investment_return_yearly (iry).

    Verifique a documentação dos campos das tabelas acima como referência.$$;

CREATE INDEX icf_with_xf_and_iry_and_f_of_last_year_f_unaccent_idx ON icf_with_xf_and_iry_and_f_of_last_year USING gin(private.f_unaccent(icf_denom_social) private.gin_trgm_ops);

CREATE INDEX icf_with_xf_and_iry_and_f_of_last_year_f_icf_cnpj_fund ON icf_with_xf_and_iry_and_f_of_last_year USING btree (icf_cnpj_fundo COLLATE pg_catalog."default") TABLESPACE pg_default;

CREATE FUNCTION icf_denom_social_unaccented(icf_with_xf_and_iry_and_f_of_last_year) RETURNS text AS $$
  SELECT private.f_unaccent($1.icf_denom_social);
$$ LANGUAGE SQL; 