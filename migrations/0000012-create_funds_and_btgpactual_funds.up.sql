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

CREATE TABLE btgpactual_funds (    
    bf_id INTEGER NOT NULL UNIQUE,
    bf_cnpj TEXT,
    bf_product TEXT,
    bf_description TEXT,
    bf_type TEXT,
    bf_risk_level INTEGER,
    bf_risk_name TEXT,
    bf_minimum_initial_investment DOUBLE PRECISION,
    bf_investor_type TEXT,
    bf_minimum_moviment DOUBLE PRECISION,
    bf_minimum_balance_remain DOUBLE PRECISION,
    bf_administration_fee DOUBLE PRECISION,
    bf_performance_fee DOUBLE PRECISION,
    bf_number_of_days_financial_investment INTEGER,
    bf_investiment_quota INTEGER,
    bf_investment_financial_settlement INTEGER,
    bf_rescue_quota INTEGER,
    bf_rescue_financial_settlement INTEGER,
    bf_anbima_rating TEXT,
    bf_anbima_code TEXT,
    bf_category_description TEXT,
    bf_category_code INTEGER,                            
    bf_custody TEXT,
    bf_auditing TEXT,
    bf_manager TEXT,
    bf_administrator TEXT,
    bf_quotaRule TEXT,
    bf_net_equity DOUBLE PRECISION,
    bf_inactive BOOLEAN,
    bf_issuer_name TEXT,
    bf_external_issuer TEXT,
    bf_is_recent_fund BOOLEAN,                            
    bf_is_blacklist BOOLEAN,
    bf_is_whitelist BOOLEAN,
    PRIMARY KEY(bf_id)
);

COMMENT ON TABLE public.btgpactual_funds IS 
    $$Lista de fundos da BTG Pactual.

    Fonte: https://www.btgpactualdigital.com/investimentos/fundos-de-investimento/produtos$$;
COMMENT ON COLUMN public.btgpactual_funds.bf_id IS 'ID de registro do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_cnpj IS 'CNPJ do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_product IS 'Nome do produto';
COMMENT ON COLUMN public.btgpactual_funds.bf_description IS 'Descrição do produto';
COMMENT ON COLUMN public.btgpactual_funds.bf_type IS 'Tipo do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_risk_level IS 'Nível do risco do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_risk_name IS 'Descrição do risco do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_minimum_initial_investment IS 'Investimento mínimo inicial do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_investor_type IS 'Tipo de investidor do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_minimum_moviment IS 'Movimentação mínima do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_minimum_balance_remain IS 'Valor mínimo para se manter no fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_administration_fee IS 'Taxa de administração do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_performance_fee IS 'Taxa de desempenho do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_number_of_days_financial_investment IS '';
COMMENT ON COLUMN public.btgpactual_funds.bf_investiment_quota IS 'Dias para cotização da aplicação no fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_investment_financial_settlement IS 'Dias para a liquidação financeira da aplicação no fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_rescue_quota IS 'Dias para cotização do resgate do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_rescue_financial_settlement IS 'Dias para a liquidação financeira do resgate do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_anbima_rating IS 'Classificação Ambima do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_anbima_code IS 'Código da classificação Ambima do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_category_description IS 'Descrição da categoria do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_category_code IS 'Código da categoria do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_custody IS 'Custodiante do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_auditing IS 'Auditor do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_manager IS 'Gestor do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_administrator IS 'Administrador do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_quotaRule IS 'Regra de cotização do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_net_equity IS 'Patrimônio líquido do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_inactive IS 'Se o fundo é inatívo';
COMMENT ON COLUMN public.btgpactual_funds.bf_issuer_name IS 'Distribuidor do fundo';
COMMENT ON COLUMN public.btgpactual_funds.bf_external_issuer IS 'Se é distribuidor externo';
COMMENT ON COLUMN public.btgpactual_funds.bf_is_recent_fund IS 'Se é um fundo recente';
COMMENT ON COLUMN public.btgpactual_funds.bf_is_blacklist IS 'Se o fundo está na lista negra';
COMMENT ON COLUMN public.btgpactual_funds.bf_is_whitelist IS 'Se o fundo está na lista branca';

CREATE INDEX funds_bf_id_bf_cnpj_index
    ON btgpactual_funds USING btree
    (bf_id, bf_cnpj);

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

COMMENT ON MATERIALIZED VIEW public.icf_with_xf_and_iry_of_last_year IS 
    $$View materializada com o cruzamento das tabelas inf_cadastral_fi (icf), xpi_funds (xf) e investment_return_yearly (iry).

    Verifique a documentação dos campos das tabelas acima como referência.
    
    Atenção: Essa view materializada está depreciada, utilize a view icf_with_xf_and_bf_and_iry_and_f_of_last_year.$$;