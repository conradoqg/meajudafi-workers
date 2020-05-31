CREATE INDEX running_days_with_indicators_dt_comptc
    ON public.running_days_with_indicators USING btree
    (dt_comptc ASC NULLS LAST)
;