ALTER TABLE public.inf_cadastral_fi
    ADD COLUMN tp_fundo text;

COMMENT ON COLUMN public.inf_cadastral_fi.tp_fundo
    IS 'Tipo de fundo';

ALTER TABLE public.inf_cadastral_fi
    ADD COLUMN cd_cvm integer;

COMMENT ON COLUMN public.inf_cadastral_fi.cd_cvm
    IS 'Código CVM';

ALTER TABLE public.inf_cadastral_fi
    ADD COLUMN entid_invest text;

COMMENT ON COLUMN public.inf_cadastral_fi.entid_invest
    IS 'Indica se o fundo é entidade de investimento';