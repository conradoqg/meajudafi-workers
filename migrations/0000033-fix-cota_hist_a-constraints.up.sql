ALTER TABLE cota_hist_a DROP CONSTRAINT cota_hist_a_codneg_data_key;
ALTER TABLE cota_hist_a ADD CONSTRAINT cota_hist_a_codneg_data_prazot_key UNIQUE (codneg, data, prazot);