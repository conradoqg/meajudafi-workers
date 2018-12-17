ALTER TABLE public.inf_diario_fi ADD COLUMN pending_statistic_at TIMESTAMP;
ALTER TABLE public.inf_diario_fi ALTER COLUMN pending_statistic_at SET DEFAULT now();

CREATE FUNCTION public.update_pending_statistic_at()
  RETURNS TRIGGER AS $$
  BEGIN
    IF NEW.pending_statistic_at = '0001-01-01' THEN
		NEW.pending_statistic_at = NULL;
        RETURN NEW;
	ELSE
		NEW.pending_statistic_at = now();
		RETURN NEW;    
    END IF;    
  END;
  $$ language 'plpgsql';

CREATE TRIGGER update_inf_diario_fi_pending_statistic_at
  BEFORE UPDATE ON public.inf_diario_fi
  FOR EACH ROW EXECUTE PROCEDURE public.update_pending_statistic_at();