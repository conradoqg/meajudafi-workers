CREATE TABLE public.cota_hist_a
(
    id uuid NOT NULL,
    codneg text COLLATE pg_catalog."default" NOT NULL,
    data date NOT NULL,
    codbdi text COLLATE pg_catalog."default",
    tpmerc integer,
    nomres text COLLATE pg_catalog."default",
    especi text COLLATE pg_catalog."default",
    preabe double precision,
    premax double precision,
    premin double precision,
    premed double precision,
    preult double precision,
    preofc double precision,
    preofv double precision,
    totneg integer,
    quatot bigint,
    voltot double precision,
    preexe double precision,
    indopc integer,
    datven date,
    fatcot integer,
    ptoexe double precision,
    codisi text COLLATE pg_catalog."default",
    dismes integer,
    modref text COLLATE pg_catalog."default",
    prazot text COLLATE pg_catalog."default",
    CONSTRAINT cota_hist_a_pkey PRIMARY KEY (id, codneg, data),
    CONSTRAINT cota_hist_a_codneg_data_key UNIQUE (codneg, data)
,
    CONSTRAINT cota_hist_a_id_key UNIQUE (id)

)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.cota_hist_a
    OWNER to postgres;

GRANT ALL ON TABLE public.cota_hist_a TO postgres;

GRANT SELECT ON TABLE public.cota_hist_a TO readonly;

COMMENT ON TABLE public.cota_hist_a
    IS 'Ações: Séries Históricas da B3

    Séries Históricas da B3, contendo os seguintes dados:
    
    - Data do pregão;
    - Código BDI;
    - Código de negociação do papel;
    - Tipo de mercado;
    - Nome resumido da empresa emissora do papel;
    - Especificação do papel;
    - Prazo em dias do mercado a termo;
    - Moeda de referência;
    - Preço de abertura do papel-mercado no pregão;
    - Preço máximo do papel-mercado no pregão;
    - Preço mínimo do papel-mercado no pregão;
    - Preço médio do papel-mercado no pregão;
    - Preço do último negócio do papel-mercado no pregão;
    - Preço da melhor oferta de compra do papel-mercado no pregão;
    - Preço da melhor oferta de venda do papel-mercado no pregão;
    - Número de negócios efetuados com o papel-mercado no pregão;
    - Quantidade total de títulos negociados neste papel-mercado;
    - Volume total de títulos negociados neste papel-mercado;
    - Preço de exercício para o mercado de opções ou valor do contrato para o mercado de termo secundário;
    - Indicador de correção de preços de exercícios ou valores de contrato para os mercados de opções ou termo secundário;
    - Data do vencimento para os mercados de opções ou termo secundário;
    - Fator de cotação do papel;
    - Preço de exercício em pontos para opções referenciadas em dólar ou valor de contrato em pontos para termo secundário;
    - Código do papel no sistema ISIN ou código interno do papel;
    - Número de distribuição do papel.

    Fonte: http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/';

COMMENT ON COLUMN public.cota_hist_a.id
    IS 'ID de registro da cotação do papel-mercado';

COMMENT ON COLUMN public.cota_hist_a.codneg
    IS 'Código de negociação do papel';

COMMENT ON COLUMN public.cota_hist_a.data
    IS 'Data do pregão';

COMMENT ON COLUMN public.cota_hist_a.codbdi
    IS 'Código BDI';

COMMENT ON COLUMN public.cota_hist_a.tpmerc
    IS 'Tipo de mercado';

COMMENT ON COLUMN public.cota_hist_a.nomres
    IS 'Nome resumido da empresa emissora do papel';

COMMENT ON COLUMN public.cota_hist_a.especi
    IS 'Especificação do papel';

COMMENT ON COLUMN public.cota_hist_a.preabe
    IS 'Preço de abertura do papel-mercado no pregão';

COMMENT ON COLUMN public.cota_hist_a.premax
    IS 'Preço máximo do papel-mercado no pregão';

COMMENT ON COLUMN public.cota_hist_a.premin
    IS 'Preço mínimo do papel-mercado no pregão';

COMMENT ON COLUMN public.cota_hist_a.premed
    IS 'Preço médio do papel-mercado no pregão';

COMMENT ON COLUMN public.cota_hist_a.preult
    IS 'Preço do último negócio do papel-mercado no pregão';

COMMENT ON COLUMN public.cota_hist_a.preofc
    IS 'Preço da melhor oferta de compra do papel-mercado no pregão';

COMMENT ON COLUMN public.cota_hist_a.preofv
    IS 'Preço da melhor oferta de venda do papel-mercado no pregão';

COMMENT ON COLUMN public.cota_hist_a.totneg
    IS 'Número de negócios efetuados com o papel-mercado no pregão';

COMMENT ON COLUMN public.cota_hist_a.quatot
    IS 'Quantidade total de títulos negociados neste papel-mercado';

COMMENT ON COLUMN public.cota_hist_a.voltot
    IS 'Volume total de títulos negociados neste papel-mercado';

COMMENT ON COLUMN public.cota_hist_a.preexe
    IS 'Preço de exercício para o mercado de opções ou valor do contrato para o mercado de termo secundário';

COMMENT ON COLUMN public.cota_hist_a.indopc
    IS 'Indicador de correção de preços de exercícios ou valores de contrato para os mercados de opções ou termo secundário';

COMMENT ON COLUMN public.cota_hist_a.datven
    IS 'Data do vencimento para os mercados de opções ou termo secundário';

COMMENT ON COLUMN public.cota_hist_a.fatcot
    IS 'Fator de cotação do papel';

COMMENT ON COLUMN public.cota_hist_a.ptoexe
    IS 'Preço de exercício em pontos para opções referenciadas em dólar ou valor de contrato em pontos para termo secundário';

COMMENT ON COLUMN public.cota_hist_a.codisi
    IS 'Código do papel no sistema ISIN ou código interno do papel';

COMMENT ON COLUMN public.cota_hist_a.dismes
    IS 'Número de distribuição do papel';

COMMENT ON COLUMN public.cota_hist_a.modref
    IS 'Moeda de referência';

COMMENT ON COLUMN public.cota_hist_a.prazot
    IS 'Prazo em dias do mercado a termo';

CREATE INDEX cota_hist_a_codneg_data_index
    ON public.cota_hist_a USING btree
    (codneg COLLATE pg_catalog."default", data)
    TABLESPACE pg_default;