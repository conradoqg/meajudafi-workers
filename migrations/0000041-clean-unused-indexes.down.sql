CREATE INDEX investment_return_daily_cnpj_fundo_dt_comptc_index
    ON public.investment_return_daily USING btree
    (ird_cnpj_fundo COLLATE pg_catalog."default" ASC NULLS LAST, ird_dt_comptc ASC NULLS LAST)
    TABLESPACE pg_default;
ALTER TABLE public.wtd_ibov
    ADD CONSTRAINT wtd_ibov_id_key UNIQUE (id);
ALTER TABLE public.investment_return_yearly
    ADD CONSTRAINT investment_return_yearly_id_key UNIQUE (iry_id);
ALTER TABLE public.investment_return_monthly
    ADD CONSTRAINT investment_return_monthly_id_key UNIQUE (irm_id);
ALTER TABLE public.investment_return_daily
    ADD CONSTRAINT investment_return_daily_id_key UNIQUE (ird_id);
ALTER TABLE public.inf_diario_fi
    ADD CONSTRAINT inf_diario_fi_id_key UNIQUE (id);
ALTER TABLE public.inf_cadastral_fi
    ADD CONSTRAINT inf_cadastral_fi_id_key UNIQUE (id);
ALTER TABLE public.funds
    ADD CONSTRAINT funds_f_id_key UNIQUE (f_id);
ALTER TABLE public.fbcdata_sgs_7i
    ADD CONSTRAINT fbcdata_sgs_7i_id_key UNIQUE (id);
ALTER TABLE public.fbcdata_sgs_433i
    ADD CONSTRAINT fbcdata_sgs_433i_id_key UNIQUE (id);
ALTER TABLE public.fbcdata_sgs_21619i
    ADD CONSTRAINT fbcdata_sgs_21619i_id_key UNIQUE (id);
ALTER TABLE public.fbcdata_sgs_1i
    ADD CONSTRAINT fbcdata_sgs_1i_id_key UNIQUE (id);
ALTER TABLE public.fbcdata_sgs_190i
    ADD CONSTRAINT fbcdata_sgs_190i_id_key UNIQUE (id);
ALTER TABLE public.fbcdata_sgs_189i
    ADD CONSTRAINT fbcdata_sgs_189i_id_key UNIQUE (id);
ALTER TABLE public.fbcdata_sgs_12i
    ADD CONSTRAINT fbcdata_sgs_12i_id_key UNIQUE (id);
ALTER TABLE public.fbcdata_sgs_11i
    ADD CONSTRAINT fbcdata_sgs_11i_id_key UNIQUE (id);
ALTER TABLE public.eod_historial_data
    ADD CONSTRAINT eod_historial_data_id_key UNIQUE (id);
ALTER TABLE public.cota_hist_a
    ADD CONSTRAINT cota_hist_a_id_key UNIQUE (id);