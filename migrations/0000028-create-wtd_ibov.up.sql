CREATE TABLE wtd_ibov (
    id UUID NOT NULL UNIQUE,
    DATE DATE NOT NULL,
    OPEN DOUBLE PRECISION,	
	CLOSE DOUBLE PRECISION,	
	HIGH DOUBLE PRECISION,	
	LOW DOUBLE PRECISION,	
	VOLUME DOUBLE PRECISION,	
    PRIMARY KEY(id, DATE),
    CONSTRAINT wtd_ibov_DATE UNIQUE (DATE)
);
CREATE INDEX wtd_ibov_date_index
    ON wtd_ibov USING btree
    (DATE ASC NULLS LAST);

COMMENT ON TABLE public.wtd_ibov IS 
	$$Histórico do Ibovespa diário
	
	Fonte: WorldTradingData.com (https://www.worldtradingdata.com).$$;
COMMENT ON COLUMN public.wtd_ibov.id IS 'ID da medição do Ibovespa';
COMMENT ON COLUMN public.wtd_ibov.date IS 'Data da medição do Ibovespa';
COMMENT ON COLUMN public.wtd_ibov.open IS 'Valor de abertura da medição do Ibovespa';
COMMENT ON COLUMN public.wtd_ibov.close IS 'Valor de fechamento da medição do Ibovespa';
COMMENT ON COLUMN public.wtd_ibov.high IS 'Valor máximo da medição do Ibovespa';
COMMENT ON COLUMN public.wtd_ibov.low IS 'Valor mínimo da medição do Ibovespa';
COMMENT ON COLUMN public.wtd_ibov.volume IS 'Volume da medição do Ibovespa';

COMMENT ON TABLE public.fbcdata_sgs_1i IS
	$$Histórico do indicador Dólar diário
	
	Fonte: SGS - Sistema Gerenciador de Séries Temporais (https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries).$$;
COMMENT ON COLUMN public.fbcdata_sgs_1i.id IS 'ID da medição da Dólar';
COMMENT ON COLUMN public.fbcdata_sgs_1i.data IS 'Data da medição da Dólar';
COMMENT ON COLUMN public.fbcdata_sgs_1i.valor IS 'Valor da medição da Dólar';

COMMENT ON TABLE public.fbcdata_sgs_7i IS 
	$$Histórico do Ibovespa diário
	
	Fonte: SGS - Sistema Gerenciador de Séries Temporais (https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries).$$;
COMMENT ON COLUMN public.fbcdata_sgs_7i.id IS 'ID da medição do Ibovespa';
COMMENT ON COLUMN public.fbcdata_sgs_7i.data IS 'Data da medição do Ibovespa';
COMMENT ON COLUMN public.fbcdata_sgs_7i.valor IS 'Valor da medição do Ibovespa';

COMMENT ON TABLE public.fbcdata_sgs_11i IS 
	$$Histórico do indicador SELIC diário
	
	Fonte: SGS - Sistema Gerenciador de Séries Temporais (https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries).$$;
COMMENT ON COLUMN public.fbcdata_sgs_11i.id IS 'ID da medição da SELIC';
COMMENT ON COLUMN public.fbcdata_sgs_11i.data IS 'Data da medição da SELIC';
COMMENT ON COLUMN public.fbcdata_sgs_11i.valor IS 'Valor da medição da SELIC';

COMMENT ON TABLE public.fbcdata_sgs_12i IS 
	$$Histórico do indicador CDI diário

	Fonte: SGS - Sistema Gerenciador de Séries Temporais (https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries).$$;
COMMENT ON COLUMN public.fbcdata_sgs_12i.id IS 'ID da medição da CDI';
COMMENT ON COLUMN public.fbcdata_sgs_12i.data IS 'Data da medição da CDI';
COMMENT ON COLUMN public.fbcdata_sgs_12i.valor IS 'Valor da medição da CDI';

COMMENT ON TABLE public.fbcdata_sgs_189i IS 
	$$Histórico do indicador IGP-M mensal
	
	Fonte: SGS - Sistema Gerenciador de Séries Temporais (https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries).$$;
COMMENT ON COLUMN public.fbcdata_sgs_189i.id IS 'ID da medição da IGP-M';
COMMENT ON COLUMN public.fbcdata_sgs_189i.data IS 'Data da medição da IGP-M';
COMMENT ON COLUMN public.fbcdata_sgs_189i.valor IS 'Valor da medição da IGP-M';

COMMENT ON TABLE public.fbcdata_sgs_190i IS 
	$$Histórico do indicador IGP-DI mensal 
	
	Fonte: SGS - Sistema Gerenciador de Séries Temporais (https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries).$$;
COMMENT ON COLUMN public.fbcdata_sgs_190i.id IS 'ID da medição da IGP-DI';
COMMENT ON COLUMN public.fbcdata_sgs_190i.data IS 'Data da medição da IGP-DI';
COMMENT ON COLUMN public.fbcdata_sgs_190i.valor IS 'Valor da medição da IGP-DI';

COMMENT ON TABLE public.fbcdata_sgs_433i IS 
	$$Histórico do indicador IPCA mensal 
	
	Fonte: SGS - Sistema Gerenciador de Séries Temporais (https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries).$$;
COMMENT ON COLUMN public.fbcdata_sgs_433i.id IS 'ID da medição da IPCA';
COMMENT ON COLUMN public.fbcdata_sgs_433i.data IS 'Data da medição da IPCA';
COMMENT ON COLUMN public.fbcdata_sgs_433i.valor IS 'Valor da medição da IPCA';

COMMENT ON TABLE public.fbcdata_sgs_21619i IS 
	$$Histórico do indicador Euro diário 
	
	Fonte: SGS - Sistema Gerenciador de Séries Temporais (https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries).$$;
COMMENT ON COLUMN public.fbcdata_sgs_21619i.id IS 'ID da medição da Euro';
COMMENT ON COLUMN public.fbcdata_sgs_21619i.data IS 'Data da medição da Euro';
COMMENT ON COLUMN public.fbcdata_sgs_21619i.valor IS 'Valor da medição da Euro';