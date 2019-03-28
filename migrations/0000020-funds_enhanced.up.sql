CREATE MATERIALIZED VIEW funds_enhanced AS
	SELECT * FROM funds
		LEFT JOIN btgpactual_funds ON funds.f_cnpj = btgpactual_funds.bf_cnpj
		LEFT JOIN xpi_funds ON funds.f_cnpj = xpi_funds.xf_cnpj
		LEFT JOIN inf_cadastral_fi ON funds.f_cnpj = inf_cadastral_fi.cnpj_fundo
WITH DATA