ALTER TABLE public.inf_cadastral_fi
    ADD COLUMN taxa_adm DOUBLE PRECISION,
    ADD COLUMN inf_taxa_adm TEXT;

COMMENT ON COLUMN public.inf_cadastral_fi.taxa_adm IS 'Taxa de administração';
COMMENT ON COLUMN public.inf_cadastral_fi.inf_taxa_adm IS 'Informações Adicionais (Taxa de administração)';