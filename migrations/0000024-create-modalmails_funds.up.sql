CREATE TABLE public.modalmais_funds
(
    mf_id INTEGER NOT NULL,
    mf_cnpj TEXT,
    mf_date DATE,    
    mf_name TEXT,
    mf_risk_name TEXT,
    mf_risk_level INTEGER,
    mf_minimum_initial_investment DOUBLE PRECISION,
    mf_administration_fee DOUBLE PRECISION,
    mf_start_date DATE,
    mf_target_audience TEXT,
    mf_profile TEXT,
    mf_net_equity DOUBLE PRECISION,
    mf_net_equity_1y DOUBLE PRECISION,
    mf_benchmark TEXT,    
    mf_rescue_quota INTEGER,
    mf_rescue_financial_settlement INTEGER,    
    mf_minimum_moviment DOUBLE PRECISION,
    mf_investment_quota INTEGER,
    mf_minimal_amount_to_stay DOUBLE PRECISION,    
    mf_max_administration_fee DOUBLE PRECISION,
    mf_performance_fee DOUBLE PRECISION,
    mf_tax_text TEXT,
    mf_description TEXT,
    mf_detail_link TEXT,
    mf_active BOOLEAN,    
    CONSTRAINT modalmais_funds_pkey PRIMARY KEY (mf_id)
);

CREATE INDEX funds_mf_id_mf_cnpj_index
    ON public.modalmais_funds USING btree
    (mf_id, mf_cnpj COLLATE pg_catalog."default")
    TABLESPACE pg_default;

GRANT SELECT ON TABLE public.modalmais_funds TO readonly;

SELECT audit.audit_table('modalmais_funds',  'true', 'false');

COMMENT ON TABLE public.modalmais_funds
    IS 'Lista de fundos da Modal Mais.

    Fonte: https://www.modalmais.com.br/investimentos/fundos-de-investimentos-modal/lista-de-fundos';

COMMENT ON COLUMN public.modalmais_funds.mf_id
    IS 'ID de registro do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_cnpj
    IS 'CNPJ do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_name
    IS 'Nome do produto';

COMMENT ON COLUMN public.modalmais_funds.mf_date
    IS 'Data de atualização do registro';

COMMENT ON COLUMN public.modalmais_funds.mf_risk_name
    IS 'Descrição do risco do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_risk_level
    IS 'Nível do risco do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_minimum_initial_investment
    IS 'Investimento mínimo inicial do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_administration_fee
    IS 'Taxa de administração do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_start_date
    IS 'Data de início do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_target_audience
    IS 'Público alvo do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_profile
    IS 'Perfil do investidor';

COMMENT ON COLUMN public.modalmais_funds.mf_net_equity
    IS 'Patrimônio líquido do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_net_equity_1y
    IS 'Patrimônio líquido médio do fundo em 1 ano';

COMMENT ON COLUMN public.modalmais_funds.mf_benchmark
    IS 'Benchmark do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_rescue_quota
    IS 'Dias para cotização do resgate do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_rescue_financial_settlement
    IS 'Dias para a liquidação financeira do resgate do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_minimum_moviment
    IS 'Movimentação mínima do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_investment_quota
    IS 'Dias para cotização da aplicação no fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_minimal_amount_to_stay
    IS 'Valor mínimo para se manter no fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_max_administration_fee
    IS 'Taxa máxima de administração';

COMMENT ON COLUMN public.modalmais_funds.mf_performance_fee
    IS 'Taxa de desempenho do fundo';

COMMENT ON COLUMN public.modalmais_funds.mf_tax_text
    IS 'Modelo de IR';

COMMENT ON COLUMN public.modalmais_funds.mf_description
    IS 'Descrição do produto';

COMMENT ON COLUMN public.modalmais_funds.mf_detail_link
    IS 'Link com os detalhes do fundo no site da Modal Mais';

COMMENT ON COLUMN public.modalmais_funds.mf_active
    IS 'Se o fundo esta atívo';

DROP VIEW changed_funds;

CREATE VIEW changed_funds AS
	SELECT table_name, action, action_tstamp_stm, changed_fields, row_data, funds.f_cnpj, funds.f_short_name FROM audit.logged_actions
		LEFT JOIN funds ON logged_actions.row_data->'bf_cnpj' = funds.f_cnpj OR logged_actions.row_data->'xf_cnpj' = funds.f_cnpj	OR logged_actions.row_data->'mf_cnpj' = funds.f_cnpj	
		WHERE logged_actions.row_data->'bf_cnpj' IS NOT NULL OR logged_actions.row_data->'xf_cnpj' IS NOT NULL OR logged_actions.row_data->'mf_cnpj' IS NOT NULL;

DROP MATERIALIZED VIEW funds_enhanced;

CREATE MATERIALIZED VIEW funds_enhanced AS
	SELECT * FROM funds
		LEFT JOIN btgpactual_funds ON funds.f_cnpj = btgpactual_funds.bf_cnpj
		LEFT JOIN xpi_funds ON funds.f_cnpj = xpi_funds.xf_cnpj
    LEFT JOIN modalmais_funds ON funds.f_cnpj = modalmais_funds.mf_cnpj
		LEFT JOIN inf_cadastral_fi_fullname ON funds.f_cnpj = inf_cadastral_fi_fullname.icf_cnpj_fundo
WITH DATA;
