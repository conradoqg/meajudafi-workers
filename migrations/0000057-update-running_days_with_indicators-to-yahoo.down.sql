DROP MATERIALIZED VIEW running_days_with_indicators;

CREATE MATERIALIZED VIEW running_days_with_indicators  AS
SELECT 
	running_days.DT_COMPTC,
	fbcdata_sgs_1i.data as dolar_data, fbcdata_sgs_1i.valor as dolar_valor,
	eod_historial_data.date as bovespa_data, eod_historial_data.close as bovespa_valor,
	fbcdata_sgs_11i.data as selic_data, fbcdata_sgs_11i.valor as selic_valor,
	fbcdata_sgs_12i.data as cdi_data, fbcdata_sgs_12i.valor as cdi_valor,
	fbcdata_sgs_189i.data as igpm_data, fbcdata_sgs_189i.valor as igpm_valor,
	fbcdata_sgs_190i.data as igpdi_data, fbcdata_sgs_190i.valor as igpdi_valor,
	fbcdata_sgs_433i.data as ipca_data, fbcdata_sgs_433i.valor as ipca_valor,
	fbcdata_sgs_21619i.data as euro_data, fbcdata_sgs_21619i.valor as euro_valor
FROM running_days
	LEFT JOIN fbcdata_sgs_1i ON running_days.DT_COMPTC = fbcdata_sgs_1i.DATA
	LEFT JOIN eod_historial_data ON eod_historial_data.SYMBOL = 'BVSP.INDX' AND running_days.DT_COMPTC = eod_historial_data.DATE
	LEFT JOIN fbcdata_sgs_11i ON running_days.DT_COMPTC = fbcdata_sgs_11i.DATA
	LEFT JOIN fbcdata_sgs_12i ON running_days.DT_COMPTC = fbcdata_sgs_12i.DATA
	LEFT JOIN fbcdata_sgs_189i ON date_part('year', running_days.DT_COMPTC) = date_part('year', fbcdata_sgs_189i.DATA) AND date_part('month', running_days.DT_COMPTC) = date_part('month', fbcdata_sgs_189i.DATA)
	LEFT JOIN fbcdata_sgs_190i ON date_part('year', running_days.DT_COMPTC) = date_part('year', fbcdata_sgs_190i.DATA) AND date_part('month', running_days.DT_COMPTC) = date_part('month', fbcdata_sgs_190i.DATA)
	LEFT JOIN fbcdata_sgs_433i ON date_part('year', running_days.DT_COMPTC) = date_part('year', fbcdata_sgs_433i.DATA) AND date_part('month', running_days.DT_COMPTC) = date_part('month', fbcdata_sgs_433i.DATA)
	LEFT JOIN fbcdata_sgs_21619i ON running_days.DT_COMPTC = fbcdata_sgs_21619i.DATA
WITH DATA;