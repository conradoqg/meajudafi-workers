CREATE MATERIALIZED VIEW running_days AS
SELECT DT_COMPTC FROM inf_diario_fi GROUP BY inf_diario_fi.DT_COMPTC
WITH DATA;

CREATE MATERIALIZED VIEW running_days_with_indicators  AS
SELECT 
	running_days.DT_COMPTC,
	fbcdata_sgs_1i.data as dolar_data, fbcdata_sgs_1i.valor as dolar_valor,
	fbcdata_sgs_7i.data as bovespa_data, fbcdata_sgs_7i.valor as bovespa_valor,
	fbcdata_sgs_11i.data as selic_data, fbcdata_sgs_11i.valor as selic_valor,
	fbcdata_sgs_12i.data as cdi_data, fbcdata_sgs_12i.valor as cdi_valor,
	fbcdata_sgs_189i.data as igpm_data, fbcdata_sgs_189i.valor as igpm_valor,
	fbcdata_sgs_190i.data as igpdi_data, fbcdata_sgs_190i.valor as igpdi_valor,
	fbcdata_sgs_433i.data as ipca_data, fbcdata_sgs_433i.valor as ipca_valor,
	fbcdata_sgs_21619i.data as euro_data, fbcdata_sgs_21619i.valor as euro_valor
FROM running_days
	LEFT JOIN fbcdata_sgs_1i ON running_days.DT_COMPTC = fbcdata_sgs_1i.DATA
	LEFT JOIN fbcdata_sgs_7i ON running_days.DT_COMPTC = fbcdata_sgs_7i.DATA
	LEFT JOIN fbcdata_sgs_11i ON running_days.DT_COMPTC = fbcdata_sgs_11i.DATA
	LEFT JOIN fbcdata_sgs_12i ON running_days.DT_COMPTC = fbcdata_sgs_12i.DATA
	LEFT JOIN fbcdata_sgs_189i ON date_part('year', running_days.DT_COMPTC) = date_part('year', fbcdata_sgs_189i.DATA) AND date_part('month', running_days.DT_COMPTC) = date_part('month', fbcdata_sgs_189i.DATA)
	LEFT JOIN fbcdata_sgs_190i ON date_part('year', running_days.DT_COMPTC) = date_part('year', fbcdata_sgs_190i.DATA) AND date_part('month', running_days.DT_COMPTC) = date_part('month', fbcdata_sgs_190i.DATA)
	LEFT JOIN fbcdata_sgs_433i ON date_part('year', running_days.DT_COMPTC) = date_part('year', fbcdata_sgs_433i.DATA) AND date_part('month', running_days.DT_COMPTC) = date_part('month', fbcdata_sgs_433i.DATA)
	LEFT JOIN fbcdata_sgs_21619i ON running_days.DT_COMPTC = fbcdata_sgs_21619i.DATA
WITH DATA;