ALTER TABLE public.inf_diario_fi
    ADD COLUMN tp_fundo text;

COMMENT ON COLUMN public.inf_diario_fi.tp_fundo
    IS 'Tipo de fundo';