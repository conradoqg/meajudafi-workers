ALTER TABLE public.inf_diario_fi
    ADD COLUMN tp_fundo text;

COMMENT ON COLUMN public.inf_diario_fi
    IS 'Tipo de fundo';