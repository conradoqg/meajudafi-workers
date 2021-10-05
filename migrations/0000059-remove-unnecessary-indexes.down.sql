CREATE INDEX inf_diario_fi_cnpj_fundo_dt_comptc_index
    ON public.inf_diario_fi USING btree
    (cnpj_fundo COLLATE pg_catalog."default" ASC NULLS LAST, dt_comptc ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX inf_diario_fi_cnpj_fundo_pending_statistic_at_index
    ON public.inf_diario_fi USING btree
    (cnpj_fundo COLLATE pg_catalog."default" ASC NULLS LAST, pending_statistic_at ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX investment_return_daily_dt_comptc_index
    ON public.investment_return_daily USING btree
    (ird_dt_comptc ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX cota_hist_a_codneg_data_index
    ON public.cota_hist_a USING btree
    (codneg COLLATE pg_catalog."default" ASC NULLS LAST, data ASC NULLS LAST)
    TABLESPACE pg_default;