CREATE INDEX icf_with_xf_and_bf_and_iry_and_f_of_last_year_common_filter
    ON public.icf_with_xf_and_bf_and_iry_and_f_of_last_year USING btree
    (iry_dt_comptc, icf_sit COLLATE pg_catalog."default", icf_condom COLLATE pg_catalog."default", icf_fundo_exclusivo COLLATE pg_catalog."default", iry_accumulated_networth)
    TABLESPACE pg_default;