ALTER TABLE public.inf_cadastral_fi
    ADD COLUMN invest_prof text;

COMMENT ON COLUMN public.inf_cadastral_fi.invest_prof
    IS 'Indica se é destinado a investidores profissionais';