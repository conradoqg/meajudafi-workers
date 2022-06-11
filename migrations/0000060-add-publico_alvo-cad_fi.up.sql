ALTER TABLE public.inf_cadastral_fi
    ADD COLUMN publico_alvo text;

COMMENT ON COLUMN public.inf_cadastral_fi.publico_alvo IS 'Público-alvo';