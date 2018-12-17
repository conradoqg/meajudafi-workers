ALTER TABLE public.inf_diario_fi
    DROP COLUMN pending_statistic_at;

DROP TRIGGER update_inf_diario_fi_pending_statistic_at ON public.inf_diario_fi;

DROP FUNCTION public.update_pending_statistic_at;