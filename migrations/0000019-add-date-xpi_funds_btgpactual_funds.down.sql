DROP VIEW changed_funds;

ALTER TABLE public.xpi_funds
    DROP COLUMN xf_date;
	
ALTER TABLE public.btgpactual_funds
    DROP COLUMN bf_date;