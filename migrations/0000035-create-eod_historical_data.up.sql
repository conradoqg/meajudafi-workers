CREATE TABLE eod_historial_data (
    ID UUID NOT NULL UNIQUE,
	SYMBOL TEXT NOT NULL,
    DATE DATE NOT NULL,
    OPEN DOUBLE PRECISION,	
	CLOSE DOUBLE PRECISION,	
	ADJUSTED_CLOSE DOUBLE PRECISION,
	HIGH DOUBLE PRECISION,	
	LOW DOUBLE PRECISION,	
	VOLUME DOUBLE PRECISION,	
    PRIMARY KEY(id, SYMBOL, DATE),
    CONSTRAINT eod_historial_data_SYMBOL_DATE UNIQUE (SYMBOL, DATE)
);
CREATE INDEX eod_historial_data_date_index
    ON eod_historial_data USING btree
    (SYMBOL ASC NULLS LAST, DATE ASC NULLS LAST);

COMMENT ON TABLE public.eod_historial_data IS 
	$$Histórico de dados
	
	Fonte: EOD Historical Data (https://eodhistoricaldata.com).$$;
COMMENT ON COLUMN public.eod_historial_data.id IS 'ID da medição';
COMMENT ON COLUMN public.eod_historial_data.symbol IS 'Símbolo do dado histórico';
COMMENT ON COLUMN public.eod_historial_data.date IS 'Data do dado histórico';
COMMENT ON COLUMN public.eod_historial_data.open IS 'Valor de abertura do dado histórico';
COMMENT ON COLUMN public.eod_historial_data.close IS 'Valor de fechamento do dado histórico';
COMMENT ON COLUMN public.eod_historial_data.adjusted_close IS 'Valor de fechamento ajustado do dado histórico';
COMMENT ON COLUMN public.eod_historial_data.high IS 'Valor máximo do dado histórico';
COMMENT ON COLUMN public.eod_historial_data.low IS 'Valor mínimo do dado histórico';
COMMENT ON COLUMN public.eod_historial_data.volume IS 'Volume do dado histórico';