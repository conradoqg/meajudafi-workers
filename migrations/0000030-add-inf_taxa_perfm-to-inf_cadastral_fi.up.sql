ALTER TABLE public.inf_cadastral_fi
    ADD COLUMN inf_taxa_perfm TEXT,
	ADD COLUMN cnpj_custodiante TEXT,
	ADD COLUMN custodiante	TEXT,
	ADD COLUMN cnpj_controlador TEXT,
	ADD COLUMN controlador	TEXT;

COMMENT ON COLUMN public.inf_cadastral_fi.inf_taxa_perfm IS 'Informações Adicionais (Taxa de performance)';
COMMENT ON COLUMN public.inf_cadastral_fi.cnpj_custodiante IS 'CNPJ do Custodiante';
COMMENT ON COLUMN public.inf_cadastral_fi.custodiante IS 'Nome do Custodiante';
COMMENT ON COLUMN public.inf_cadastral_fi.cnpj_controlador IS 'CNPJ do Controlador';
COMMENT ON COLUMN public.inf_cadastral_fi.controlador IS 'Nome do Controlador';