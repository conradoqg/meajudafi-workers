CREATE TABLE yahoo_data (
    ID UUID NOT NULL UNIQUE,
	SYMBOL TEXT NOT NULL,
    DATE DATE NOT NULL,
    OPEN DOUBLE PRECISION,	
	CLOSE DOUBLE PRECISION,	
	"adj close" DOUBLE PRECISION,
	HIGH DOUBLE PRECISION,	
	LOW DOUBLE PRECISION,	
	VOLUME DOUBLE PRECISION,	
    PRIMARY KEY(id, SYMBOL, DATE),
    CONSTRAINT yahoo_data_SYMBOL_DATE UNIQUE (SYMBOL, DATE)
);
CREATE INDEX yahoo_data_date_index
    ON yahoo_data USING btree
    (SYMBOL ASC NULLS LAST, DATE ASC NULLS LAST);

COMMENT ON TABLE public.yahoo_data IS 
	$$Histórico de dados
	
	Fonte: Yahoo Data Data (https://finance.yahoo.com/lookup).$$;
COMMENT ON COLUMN public.yahoo_data.id IS 'ID da medição';
COMMENT ON COLUMN public.yahoo_data.symbol IS 'Símbolo do dado histórico';
COMMENT ON COLUMN public.yahoo_data.date IS 'Data do dado histórico';
COMMENT ON COLUMN public.yahoo_data.open IS 'Valor de abertura do dado histórico';
COMMENT ON COLUMN public.yahoo_data.close IS 'Valor de fechamento do dado histórico';
COMMENT ON COLUMN public.yahoo_data."adj close" IS 'Valor de fechamento ajustado do dado histórico';
COMMENT ON COLUMN public.yahoo_data.high IS 'Valor máximo do dado histórico';
COMMENT ON COLUMN public.yahoo_data.low IS 'Valor mínimo do dado histórico';
COMMENT ON COLUMN public.yahoo_data.volume IS 'Volume do dado histórico';