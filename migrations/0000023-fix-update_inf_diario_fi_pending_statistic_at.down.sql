DROP TRIGGER update_inf_diario_fi_pending_statistic_at ON public.inf_diario_fi;

CREATE TRIGGER update_inf_diario_fi_pending_statistic_at
  BEFORE UPDATE ON public.inf_diario_fi
  FOR EACH ROW EXECUTE PROCEDURE public.update_pending_statistic_at();