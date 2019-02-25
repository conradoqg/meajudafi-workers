DROP MATERIALIZED VIEW xf_with_irm_timeseries;

DROP FUNCTION icf_denom_social_unaccented(icf_with_xf_and_bf_and_iry_and_f_of_last_year);
DROP MATERIALIZED VIEW icf_with_xf_and_bf_and_iry_and_f_of_last_year;

DROP FUNCTION icf_denom_social_unaccented(icf_with_xf_and_iry_of_last_year);
DROP MATERIALIZED VIEW icf_with_xf_and_iry_of_last_year;

DROP TABLE public.xpi_funds;

CREATE TABLE public.xpi_funds
(
    xf_id integer NOT NULL,
    xf_cnpj text COLLATE pg_catalog."default",
    xf_formal_risk integer,
    xf_morningstar integer,
    xf_name text COLLATE pg_catalog."default",
    xf_initial_investment double precision,
    xf_state text COLLATE pg_catalog."default",
    xf_adm_fee double precision,
    xf_perf_fee double precision,
    xf_benchmark text COLLATE pg_catalog."default",
    xf_type text COLLATE pg_catalog."default",
    xf_minimal_movement double precision,
    xf_minimal_amount_to_stay double precision,
    xf_investment_quota integer,
    xf_rescue_quota integer,
    xf_rescue_financial_settlement integer,
    xf_investment_rescue_time text COLLATE pg_catalog."default",
    xf_anbima_rating text COLLATE pg_catalog."default",
    xf_anbima_code text COLLATE pg_catalog."default",
    xf_custody text COLLATE pg_catalog."default",
    xf_auditing text COLLATE pg_catalog."default",
    xf_manager text COLLATE pg_catalog."default",
    xf_administrator text COLLATE pg_catalog."default",
    xf_startdate date,
    xf_net_equity double precision,
    xf_net_equity_1y double precision,
    xf_max_adm_fee double precision,
    xf_tax_text text COLLATE pg_catalog."default",
    xf_iof_text text COLLATE pg_catalog."default",
    CONSTRAINT xpi_funds_pkey PRIMARY KEY (xf_id)
);

COMMENT ON TABLE public.xpi_funds IS 
    $$Lista de fundos da XP Investimentos.

    Fonte: https://institucional.xpi.com.br/investimentos/fundos-de-investimento/lista-de-fundos-de-investimento.aspx$$;
COMMENT ON COLUMN public.xpi_funds.xf_id IS 'ID do fundo na XPI';
COMMENT ON COLUMN public.xpi_funds.xf_cnpj IS 'CNPJ do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_formal_risk IS 'Risco formal na XPI';
COMMENT ON COLUMN public.xpi_funds.xf_morningstar IS 'Classificação Morningstar';
COMMENT ON COLUMN public.xpi_funds.xf_name IS 'Nome do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_initial_investment IS 'Valor do investimento inicial';
COMMENT ON COLUMN public.xpi_funds.xf_state IS 'Situação do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_adm_fee IS 'Taxa de administração';
COMMENT ON COLUMN public.xpi_funds.xf_perf_fee IS 'Taxa de performance';
COMMENT ON COLUMN public.xpi_funds.xf_benchmark IS 'Benchmark do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_type IS 'Tipo do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_minimal_movement IS 'Movimentação mínima do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_minimal_amount_to_stay IS 'Valor mínimo para se manter no fundo';
COMMENT ON COLUMN public.xpi_funds.xf_investment_quota IS 'Dias para cotização da aplicação no fundo';
COMMENT ON COLUMN public.xpi_funds.xf_rescue_quota IS 'Dias para cotização do resgate do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_rescue_financial_settlement IS 'Dias para a liquidação financeira do resgate do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_investment_rescue_time IS 'Horário máximo de solicitação de resgate';
COMMENT ON COLUMN public.xpi_funds.xf_anbima_rating IS 'Classificação Ambima do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_anbima_code IS 'Código da classificação Ambima do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_custody IS 'Custodiante do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_auditing IS 'Auditor do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_manager IS 'Gestor do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_administrator IS 'Administrador do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_startdate IS 'Data de início do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_net_equity IS 'Patrimônio líquido do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_net_equity_1y IS 'Patrimônio líquido médio do fundo em 1 ano';
COMMENT ON COLUMN public.xpi_funds.xf_max_adm_fee IS 'Taxa máxima de administração';
COMMENT ON COLUMN public.xpi_funds.xf_tax_text IS 'Modelo de IR';
COMMENT ON COLUMN public.xpi_funds.xf_iof_text IS 'Modelo de IOF';

ALTER TABLE public.btgpactual_funds
    ALTER COLUMN bf_cnpj DROP NOT NULL;

CREATE INDEX xpi_funds_xf_id_xf_cnpj_index
    ON public.xpi_funds USING btree
    (xf_id ASC NULLS LAST, xf_cnpj ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE MATERIALIZED VIEW icf_with_xf_and_bf_and_iry_and_f_of_last_year
AS
SELECT DISTINCT ON (icf_CNPJ_FUNDO) icf_CNPJ_FUNDO as DISTINCT_icf_CNPJ_FUNDO,* 
	FROM inf_cadastral_fi_fullname 
	LEFT JOIN xpi_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = xpi_funds.xf_cnpj
    LEFT JOIN btgpactual_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = btgpactual_funds.bf_cnpj
    LEFT JOIN funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = funds.f_cnpj    
	LEFT JOIN (
		SELECT DISTINCT ON (iry_cnpj_fundo) iry_cnpj_fundo as DISTINCT_iry_cnpj_fundo, * FROM investment_return_yearly ORDER BY iry_cnpj_fundo, iry_dt_comptc DESC
	) AS LAST_YEAR ON inf_cadastral_fi_fullname.icf_cnpj_fundo = LAST_YEAR.iry_cnpj_fundo
WITH DATA;

COMMENT ON MATERIALIZED VIEW public.icf_with_xf_and_bf_and_iry_and_f_of_last_year IS 
    $$View materializada com o cruzamento das tabelas inf_cadastral_fi (icf), xpi_funds (xf), btgpactual_funds (bf), funds (f) e investment_return_yearly (iry).

    Verifique a documentação dos campos das tabelas acima como referência.$$;

CREATE INDEX icf_with_xf_and_bf_and_iry_and_f_of_last_year_f_unaccent_idx ON icf_with_xf_and_bf_and_iry_and_f_of_last_year USING gin(private.f_unaccent(icf_denom_social) private.gin_trgm_ops);

CREATE INDEX icf_with_xf_and_bf_and_iry_and_f_of_last_year_f_icf_cnpj_fund ON icf_with_xf_and_bf_and_iry_and_f_of_last_year USING btree (icf_cnpj_fundo COLLATE pg_catalog."default") TABLESPACE pg_default;

CREATE FUNCTION icf_denom_social_unaccented(icf_with_xf_and_bf_and_iry_and_f_of_last_year) RETURNS text AS $$
  SELECT private.f_unaccent($1.icf_denom_social);
$$ LANGUAGE SQL; 

CREATE MATERIALIZED VIEW icf_with_xf_and_iry_of_last_year
AS
SELECT DISTINCT ON (icf_CNPJ_FUNDO) icf_CNPJ_FUNDO as DISTINCT_icf_CNPJ_FUNDO,* 
	FROM inf_cadastral_fi_fullname 
	LEFT JOIN xpi_funds ON inf_cadastral_fi_fullname.icf_cnpj_fundo = xpi_funds.xf_cnpj
	LEFT JOIN (
		SELECT DISTINCT ON (iry_cnpj_fundo) iry_cnpj_fundo as DISTINCT_iry_cnpj_fundo, * FROM investment_return_yearly ORDER BY iry_cnpj_fundo, iry_dt_comptc DESC
	) AS LAST_YEAR ON inf_cadastral_fi_fullname.icf_cnpj_fundo = LAST_YEAR.iry_cnpj_fundo
WITH DATA;

CREATE INDEX inf_cadastral_fi_with_xpi_and_iry_of_last_year_f_unaccent_idx ON icf_with_xf_and_iry_of_last_year USING gin(private.f_unaccent(icf_denom_social) private.gin_trgm_ops);

CREATE INDEX inf_cadastral_fi_with_xpi_and_iry_of_last_year_f_icf_cnpj_fund ON icf_with_xf_and_iry_of_last_year USING btree (icf_cnpj_fundo COLLATE pg_catalog."default") TABLESPACE pg_default;

CREATE FUNCTION icf_denom_social_unaccented(icf_with_xf_and_iry_of_last_year) RETURNS text AS $$
  SELECT private.f_unaccent($1.icf_denom_social);
$$ LANGUAGE SQL;

COMMENT ON MATERIALIZED VIEW public.icf_with_xf_and_iry_of_last_year IS 
    $$View materializada com o cruzamento das tabelas inf_cadastral_fi (icf), xpi_funds (xf) e investment_return_yearly (iry).

    Verifique a documentação dos campos das tabelas acima como referência.
    
    Atenção: Essa view materializada está depreciada, utilize a view icf_with_xf_and_bf_and_iry_and_f_of_last_year.$$;

CREATE MATERIALIZED VIEW xf_with_irm_timeseries
AS 
SELECT 
	xf_name, 
	icf_classe, 
	last_day_of_month as irm_dt_comptc, 
	COALESCE(irm_accumulated_investment_return,0) as irm_accumulated_investment_return, 
	COALESCE(irm_accumulated_risk,0) as irm_accumulated_risk 
	FROM (SELECT * FROM xpi_funds
			LEFT JOIN inf_cadastral_fi_fullname ON xf_cnpj = icf_cnpj_fundo
			CROSS JOIN (SELECT CAST(date_trunc('month',ts) + interval '1 month - 1 day' AS DATE) AS last_day_of_month
			FROM generate_series(
				(SELECT MIN(irm_dt_comptc)::timestamp::date FROM investment_return_monthly),
				(SELECT MAX(irm_dt_comptc)::timestamp::date FROM investment_return_monthly),
				interval '1 month') as ts) REFERENCE_DATE_SERIES) AS DATE_SERIES	
	LEFT JOIN investment_return_monthly ON irm_cnpj_fundo = DATE_SERIES.xf_cnpj AND DATE_SERIES.last_day_of_month = irm_dt_comptc				
	ORDER BY irm_dt_comptc, xf_name, icf_classe
WITH DATA;