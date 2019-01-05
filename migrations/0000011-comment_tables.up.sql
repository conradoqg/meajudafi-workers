COMMENT ON TABLE public.fbcdata_sgs_1i IS 'Histórico do indicador Dólar diário.';
COMMENT ON COLUMN public.fbcdata_sgs_1i.id IS 'ID da medição da Dólar';
COMMENT ON COLUMN public.fbcdata_sgs_1i.data IS 'Data da medição da Dólar';
COMMENT ON COLUMN public.fbcdata_sgs_1i.valor IS 'Valor da medição da Dólar';

COMMENT ON TABLE public.fbcdata_sgs_7i IS 'Histórico do indicador Bovespa diário.';
COMMENT ON COLUMN public.fbcdata_sgs_7i.id IS 'ID da medição da Bovespa';
COMMENT ON COLUMN public.fbcdata_sgs_7i.data IS 'Data da medição da Bovespa';
COMMENT ON COLUMN public.fbcdata_sgs_7i.valor IS 'Valor da medição da Bovespa';

COMMENT ON TABLE public.fbcdata_sgs_11i IS 'Histórico do indicador SELIC diário.';
COMMENT ON COLUMN public.fbcdata_sgs_11i.id IS 'ID da medição da SELIC';
COMMENT ON COLUMN public.fbcdata_sgs_11i.data IS 'Data da medição da SELIC';
COMMENT ON COLUMN public.fbcdata_sgs_11i.valor IS 'Valor da medição da SELIC';

COMMENT ON TABLE public.fbcdata_sgs_12i IS 'Histórico do indicador CDI diário.';
COMMENT ON COLUMN public.fbcdata_sgs_12i.id IS 'ID da medição da CDI';
COMMENT ON COLUMN public.fbcdata_sgs_12i.data IS 'Data da medição da CDI';
COMMENT ON COLUMN public.fbcdata_sgs_12i.valor IS 'Valor da medição da CDI';

COMMENT ON TABLE public.fbcdata_sgs_189i IS 'Histórico do indicador IGP-M mensal.';
COMMENT ON COLUMN public.fbcdata_sgs_189i.id IS 'ID da medição da IGP-M';
COMMENT ON COLUMN public.fbcdata_sgs_189i.data IS 'Data da medição da IGP-M';
COMMENT ON COLUMN public.fbcdata_sgs_189i.valor IS 'Valor da medição da IGP-M';

COMMENT ON TABLE public.fbcdata_sgs_190i IS 'Histórico do indicador IGP-DI mensal.';
COMMENT ON COLUMN public.fbcdata_sgs_190i.id IS 'ID da medição da IGP-DI';
COMMENT ON COLUMN public.fbcdata_sgs_190i.data IS 'Data da medição da IGP-DI';
COMMENT ON COLUMN public.fbcdata_sgs_190i.valor IS 'Valor da medição da IGP-DI';

COMMENT ON TABLE public.fbcdata_sgs_433i IS 'Histórico do indicador IPCA mensal.';
COMMENT ON COLUMN public.fbcdata_sgs_433i.id IS 'ID da medição da IPCA';
COMMENT ON COLUMN public.fbcdata_sgs_433i.data IS 'Data da medição da IPCA';
COMMENT ON COLUMN public.fbcdata_sgs_433i.valor IS 'Valor da medição da IPCA';

COMMENT ON TABLE public.fbcdata_sgs_21619i IS 'Histórico do indicador Euro diário.';
COMMENT ON COLUMN public.fbcdata_sgs_21619i.id IS 'ID da medição da Euro';
COMMENT ON COLUMN public.fbcdata_sgs_21619i.data IS 'Data da medição da Euro';
COMMENT ON COLUMN public.fbcdata_sgs_21619i.valor IS 'Valor da medição da Euro';

COMMENT ON TABLE public.inf_cadastral_fi IS 
    $$Fundos de Investimento: Informação Cadastral

    Dados cadastrais de fundos de investimento referentes à instrução da CVM número 555, como CNPJ, data de registro e situação do fundo.
    
    Fonte: http://dados.cvm.gov.br/dataset/fi-cad$$;
COMMENT ON COLUMN public.inf_cadastral_fi.id IS 'ID de registro do fundo';
COMMENT ON COLUMN public.inf_cadastral_fi.cnpj_fundo IS 'CNPJ do fundo';
COMMENT ON COLUMN public.inf_cadastral_fi.denom_social IS 'Denominação Social';
COMMENT ON COLUMN public.inf_cadastral_fi.dt_reg IS 'Data de registro';
COMMENT ON COLUMN public.inf_cadastral_fi.dt_const IS 'Data de constituição';
COMMENT ON COLUMN public.inf_cadastral_fi.dt_cancel IS 'Data de cancelamento';
COMMENT ON COLUMN public.inf_cadastral_fi.sit IS 'Situação';
COMMENT ON COLUMN public.inf_cadastral_fi.dt_ini_sit IS 'Data início da situação';
COMMENT ON COLUMN public.inf_cadastral_fi.dt_ini_ativ IS 'Data de início de atividade';
COMMENT ON COLUMN public.inf_cadastral_fi.dt_ini_exerc IS 'Data início do exercício social';
COMMENT ON COLUMN public.inf_cadastral_fi.dt_fim_exerc IS 'Data fim do exercício social';
COMMENT ON COLUMN public.inf_cadastral_fi.classe IS 'Classe';
COMMENT ON COLUMN public.inf_cadastral_fi.dt_ini_classe IS 'Data de início na classe';
COMMENT ON COLUMN public.inf_cadastral_fi.rentab_fundo IS 'Forma de rentabilidade do fundo (indicador de desempenho)';
COMMENT ON COLUMN public.inf_cadastral_fi.condom IS 'Forma de condomínio (Aberto/Fechado)';
COMMENT ON COLUMN public.inf_cadastral_fi.fundo_cotas IS 'Indica se é fundo de cotas (S/N)';
COMMENT ON COLUMN public.inf_cadastral_fi.fundo_exclusivo IS 'Indica se é fundo exclusivo (S/N)';
COMMENT ON COLUMN public.inf_cadastral_fi.trib_lprazo IS 'Indica se possui tributação de longo prazo (S/N)';
COMMENT ON COLUMN public.inf_cadastral_fi.invest_qualif IS 'Indica se é destinado a investidores qualificados (S/N)';
COMMENT ON COLUMN public.inf_cadastral_fi.taxa_perfm IS 'Taxa de performance';
COMMENT ON COLUMN public.inf_cadastral_fi.vl_patrim_liq IS 'Valor do patrimônio líquido';
COMMENT ON COLUMN public.inf_cadastral_fi.dt_patrim_liq IS 'Data do patrimônio líquido';
COMMENT ON COLUMN public.inf_cadastral_fi.diretor IS 'Nome do Diretor Responsável';
COMMENT ON COLUMN public.inf_cadastral_fi.cnpj_admin IS 'CNPJ do Administrador';
COMMENT ON COLUMN public.inf_cadastral_fi.admin IS 'Nome do Administrador';
COMMENT ON COLUMN public.inf_cadastral_fi.pf_pj_gestor IS 'Indica se o gestor é pessoa física ou jurídica (PF/PJ)';
COMMENT ON COLUMN public.inf_cadastral_fi.cpf_cnpj_gestor IS 'Informa o código de identificação do gestor pessoa física ou jurídica';
COMMENT ON COLUMN public.inf_cadastral_fi.gestor IS 'Nome do Gestor';
COMMENT ON COLUMN public.inf_cadastral_fi.cnpj_auditor IS 'CNPJ do Auditor';
COMMENT ON COLUMN public.inf_cadastral_fi.auditor IS 'Nome do Auditor';

COMMENT ON TABLE public.inf_diario_fi IS 
    $$Fundos de Investimento: Documentos: Informe Diário

    O INFORME DIÁRIO é um demonstrativo que contém as seguintes informações do fundo, relativas à data de competência:

    - Valor total da carteira do fundo;
    - Patrimônio líquido;
    - Valor da cota;
    - Captações realizadas no dia;
    - Resgates pagos no dia;
    - Número de cotistas
    
    Fonte: http://dados.cvm.gov.br/dataset/fi-doc-inf_diario$$;
COMMENT ON COLUMN public.inf_diario_fi.id IS 'ID de registro do informe do fundo';
COMMENT ON COLUMN public.inf_diario_fi.cnpj_fundo IS 'CNPJ do fundo';
COMMENT ON COLUMN public.inf_diario_fi.dt_comptc IS 'Data de competência do documento';
COMMENT ON COLUMN public.inf_diario_fi.vl_total IS 'Valor total da carteira';
COMMENT ON COLUMN public.inf_diario_fi.vl_quota IS 'Valor da cota';
COMMENT ON COLUMN public.inf_diario_fi.vl_patrim_liq IS 'Valor do patrimônio líquido';
COMMENT ON COLUMN public.inf_diario_fi.captc_dia IS 'Captação do dia';
COMMENT ON COLUMN public.inf_diario_fi.resg_dia IS 'Resgate no dia';
COMMENT ON COLUMN public.inf_diario_fi.nr_cotst IS 'Número de cotistas';
COMMENT ON COLUMN public.inf_diario_fi.pending_statistic_at IS 'Data da última atualização (Utilizado para recalcular o retorno do investimento de fundos com atualização passada de seus dados)';

COMMENT ON TABLE public.investment_return_daily IS 
    $$Histórico do retorno de investimento diário por fundo (incluindo indicadores).

    Dados calculados com base na tabela inf_cadastral_fi com a adição das informações dos indicadores.$$;
COMMENT ON COLUMN public.investment_return_daily.ird_id IS 'ID de registro do rendimento do dia do fundo';
COMMENT ON COLUMN public.investment_return_daily.ird_cnpj_fundo IS 'CNPJ do fundo';
COMMENT ON COLUMN public.investment_return_daily.ird_dt_comptc IS 'Data de competência do documento';
COMMENT ON COLUMN public.investment_return_daily.ird_investment_return IS 'Retorno do fundo do dia';
COMMENT ON COLUMN public.investment_return_daily.ird_investment_return_1y IS 'Retorno de 1 ano';
COMMENT ON COLUMN public.investment_return_daily.ird_investment_return_2y IS 'Retorno de 2 anos';
COMMENT ON COLUMN public.investment_return_daily.ird_investment_return_3y IS 'Retorno de 3 anos';
COMMENT ON COLUMN public.investment_return_daily.ird_accumulated_investment_return IS 'Retorno acumulado desde o início do fundo';
COMMENT ON COLUMN public.investment_return_daily.ird_risk_1y IS 'Risco de 1 ano';
COMMENT ON COLUMN public.investment_return_daily.ird_risk_2y IS 'Risco de 2 anos';
COMMENT ON COLUMN public.investment_return_daily.ird_risk_3y IS 'Risco de 3 anos';
COMMENT ON COLUMN public.investment_return_daily.ird_accumulated_risk IS 'Risco acumulado desde o início do fundo';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_sharpe_1y IS 'Sharpe de 1 ano com base no CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_sharpe_2y IS 'Sharpe de 2 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_sharpe_3y IS 'Sharpe de 3 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_accumulated_sharpe IS 'Sharpe acumulado desde o início do fundo com base no CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_consistency_1y IS 'Consistência de 1 ano com base no CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_consistency_2y IS 'Consistência de 2 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_consistency_3y IS 'Consistência de 3 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_networth IS 'Patrinônio do fundo calculado com base no histórico diário';
COMMENT ON COLUMN public.investment_return_daily.ird_quotaholders IS 'Cotistas do fundo calculado com base no histórico diário';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_investment_return IS 'Retorno do dia do CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_investment_return_1y IS 'Retorno de 1 ano do CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_investment_return_2y IS 'Retorno de 2 anos do CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_investment_return_3y IS 'Retorno de 3 ano3 do CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_cdi_accumulated_investment_return IS 'Retorno acumulado desde o início do fundo do CDI';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_investment_return IS 'Retorno do dia do bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_investment_return_1y IS 'Retorno de 1 ano do bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_investment_return_2y IS 'Retorno de 2 anos do bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_investment_return_3y IS 'Retorno de 3 anos do bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_accumulated_investment_return IS 'Retorno acumulado desde o início do fundo do Bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_sharpe_1y IS 'Sharpe de 1 ano com base no Bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_sharpe_2y IS 'Sharpe de 2 anos com base no Bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_sharpe_3y IS 'Sharpe de 3 anos com base no Bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_accumulated_sharpe IS 'Sharpe acumulado desde o início do fundo com base no Bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_consistency_1y IS 'Consistência de 1 ano com base no Bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_consistency_2y IS 'Consistência de 2 anos com base no Bovespa';
COMMENT ON COLUMN public.investment_return_daily.ird_bovespa_consistency_3y IS 'Consistência de 3 anos com base no Bovespa';

COMMENT ON TABLE public.investment_return_monthly IS 
    $$Histórico do retorno de investimento mensal por fundo (incluindo indicadores).

    Dados calculados com base na tabela inf_cadastral_fi com a adição das informações dos indicadores.$$;
COMMENT ON COLUMN public.investment_return_monthly.irm_id IS 'ID de registro do rendimento do mês do fundo';
COMMENT ON COLUMN public.investment_return_monthly.irm_cnpj_fundo IS 'CNPJ do fundo';
COMMENT ON COLUMN public.investment_return_monthly.irm_dt_comptc IS 'Data de competência do documento';
COMMENT ON COLUMN public.investment_return_monthly.irm_investment_return IS 'Retorno do fundo do mês';
COMMENT ON COLUMN public.investment_return_monthly.irm_investment_return_1y IS 'Retorno de 1 ano';
COMMENT ON COLUMN public.investment_return_monthly.irm_investment_return_2y IS 'Retorno de 2 anos';
COMMENT ON COLUMN public.investment_return_monthly.irm_investment_return_3y IS 'Retorno de 3 anos';
COMMENT ON COLUMN public.investment_return_monthly.irm_accumulated_investment_return IS 'Retorno acumulado desde o início do fundo';
COMMENT ON COLUMN public.investment_return_monthly.irm_risk_1y IS 'Risco de 1 ano';
COMMENT ON COLUMN public.investment_return_monthly.irm_risk_2y IS 'Risco de 2 anos';
COMMENT ON COLUMN public.investment_return_monthly.irm_risk_3y IS 'Risco de 3 anos';
COMMENT ON COLUMN public.investment_return_monthly.irm_accumulated_risk IS 'Risco acumulado desde o início do fundo';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_sharpe_1y IS 'Sharpe de 1 ano com base no CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_sharpe_2y IS 'Sharpe de 2 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_sharpe_3y IS 'Sharpe de 3 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_accumulated_sharpe IS 'Sharpe acumulado desde o início do fundo com base no CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_consistency_1y IS 'Consistência de 1 ano com base no CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_consistency_2y IS 'Consistência de 2 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_consistency_3y IS 'Consistência de 3 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_networth IS 'Patrinônio do fundo calculado com base no histórico diário';
COMMENT ON COLUMN public.investment_return_monthly.irm_quotaholders IS 'Cotistas do fundo calculado com base no histórico diário';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_investment_return IS 'Retorno do dia do CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_investment_return_1y IS 'Retorno de 1 ano do CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_investment_return_2y IS 'Retorno de 2 anos do CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_investment_return_3y IS 'Retorno de 3 ano3 do CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_cdi_accumulated_investment_return IS 'Retorno acumulado desde o início do fundo do CDI';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_investment_return IS 'Retorno do dia do bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_investment_return_1y IS 'Retorno de 1 ano do bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_investment_return_2y IS 'Retorno de 2 anos do bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_investment_return_3y IS 'Retorno de 3 anos do bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_accumulated_investment_return IS 'Retorno acumulado desde o início do fundo do Bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_sharpe_1y IS 'Sharpe de 1 ano com base no Bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_sharpe_2y IS 'Sharpe de 2 anos com base no Bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_sharpe_3y IS 'Sharpe de 3 anos com base no Bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_accumulated_sharpe IS 'Sharpe acumulado desde o início do fundo com base no Bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_consistency_1y IS 'Consistência de 1 ano com base no Bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_consistency_2y IS 'Consistência de 2 anos com base no Bovespa';
COMMENT ON COLUMN public.investment_return_monthly.irm_bovespa_consistency_3y IS 'Consistência de 3 anos com base no Bovespa';

COMMENT ON TABLE public.investment_return_yearly IS 
    $$Histórico do retorno de investimento anual por fundo (incluindo indicadores).

    Dados calculados com base na tabela inf_cadastral_fi com a adição das informações dos indicadores.$$;
COMMENT ON COLUMN public.investment_return_yearly.iry_id IS 'ID de registro do rendimento do ano do fundo';
COMMENT ON COLUMN public.investment_return_yearly.iry_cnpj_fundo IS 'CNPJ do fundo';
COMMENT ON COLUMN public.investment_return_yearly.iry_dt_comptc IS 'Data de competência do documento';
COMMENT ON COLUMN public.investment_return_yearly.iry_investment_return IS 'Retorno do fundo do ano';
COMMENT ON COLUMN public.investment_return_yearly.iry_investment_return_1y IS 'Retorno de 1 ano';
COMMENT ON COLUMN public.investment_return_yearly.iry_investment_return_2y IS 'Retorno de 2 anos';
COMMENT ON COLUMN public.investment_return_yearly.iry_investment_return_3y IS 'Retorno de 3 anos';
COMMENT ON COLUMN public.investment_return_yearly.iry_accumulated_investment_return IS 'Retorno acumulado desde o início do fundo';
COMMENT ON COLUMN public.investment_return_yearly.iry_risk_1y IS 'Risco de 1 ano';
COMMENT ON COLUMN public.investment_return_yearly.iry_risk_2y IS 'Risco de 2 anos';
COMMENT ON COLUMN public.investment_return_yearly.iry_risk_3y IS 'Risco de 3 anos';
COMMENT ON COLUMN public.investment_return_yearly.iry_accumulated_risk IS 'Risco acumulado desde o início do fundo';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_sharpe_1y IS 'Sharpe de 1 ano com base no CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_sharpe_2y IS 'Sharpe de 2 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_sharpe_3y IS 'Sharpe de 3 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_accumulated_sharpe IS 'Sharpe acumulado desde o início do fundo com base no CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_consistency_1y IS 'Consistência de 1 ano com base no CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_consistency_2y IS 'Consistência de 2 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_consistency_3y IS 'Consistência de 3 anos com base no CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_networth IS 'Patrinônio do fundo calculado com base no histórico diário';
COMMENT ON COLUMN public.investment_return_yearly.iry_quotaholders IS 'Cotistas do fundo calculado com base no histórico diário';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_investment_return IS 'Retorno do dia do CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_investment_return_1y IS 'Retorno de 1 ano do CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_investment_return_2y IS 'Retorno de 2 anos do CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_investment_return_3y IS 'Retorno de 3 ano3 do CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_cdi_accumulated_investment_return IS 'Retorno acumulado desde o início do fundo do CDI';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_investment_return IS 'Retorno do dia do bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_investment_return_1y IS 'Retorno de 1 ano do bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_investment_return_2y IS 'Retorno de 2 anos do bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_investment_return_3y IS 'Retorno de 3 anos do bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_accumulated_investment_return IS 'Retorno acumulado desde o início do fundo do Bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_sharpe_1y IS 'Sharpe de 1 ano com base no Bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_sharpe_2y IS 'Sharpe de 2 anos com base no Bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_sharpe_3y IS 'Sharpe de 3 anos com base no Bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_accumulated_sharpe IS 'Sharpe acumulado desde o início do fundo com base no Bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_consistency_1y IS 'Consistência de 1 ano com base no Bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_consistency_2y IS 'Consistência de 2 anos com base no Bovespa';
COMMENT ON COLUMN public.investment_return_yearly.iry_bovespa_consistency_3y IS 'Consistência de 3 anos com base no Bovespa';

COMMENT ON TABLE public.migrations IS 'Histórico de migrações do banco de dados';
COMMENT ON COLUMN public.migrations.id IS 'ID da migração';
COMMENT ON COLUMN public.migrations.name IS 'Nome da migração';
COMMENT ON COLUMN public.migrations.datetime IS 'Data e hora da migração';

COMMENT ON TABLE public.xpi_funds IS 
    $$Lista de fundos da XP Investimentos.

    Fonte: https://institucional.xpi.com.br/investimentos/fundos-de-investimento/lista-de-fundos-de-investimento.aspx$$;
COMMENT ON COLUMN public.xpi_funds.xf_id IS 'ID de registro do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_cnpj IS 'CNPJ do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_xpi_id IS 'ID do fundo na XPI';
COMMENT ON COLUMN public.xpi_funds.xf_formal_risk IS 'Risco formal na XPI';
COMMENT ON COLUMN public.xpi_funds.xf_morningstar IS 'Classificação Morningstar';
COMMENT ON COLUMN public.xpi_funds.xf_name IS 'Nome do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_initial_investment IS 'Valor do investimento inicial';
COMMENT ON COLUMN public.xpi_funds.xf_redemption_delay_in_days IS 'Dias para resgate da cota';
COMMENT ON COLUMN public.xpi_funds.xf_state IS 'Situação do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_adm_fee IS 'Taxa de administração';
COMMENT ON COLUMN public.xpi_funds.xf_perf_fee IS 'Taxa de performance';
COMMENT ON COLUMN public.xpi_funds.xf_benchmark IS 'Benchmark do fundo';
COMMENT ON COLUMN public.xpi_funds.xf_type IS 'Tipo do fundo';

COMMENT ON VIEW public.inf_cadastral_fi_fullname IS 'Tabela inf_cadastral_fi mas com os nomes dos campos alterados para começarem com icf_.';

COMMENT ON MATERIALIZED VIEW public.icf_with_xf_and_iry_of_last_year IS 
    $$View materializada com o cruzamento das tabelas inf_cadastral_fi (icf), xpi_funds (xf) e investment_return_yearly (iry).

    Verifique a documentação dos campos das tabelas acima como referência.$$;

COMMENT ON MATERIALIZED VIEW public.running_days IS 'View materializada com os dias úteis com base no histórico dos fundos.';

COMMENT ON MATERIALIZED VIEW public.running_days_with_indicators IS 'View materializada com dias úteis com base no histórico dos fundos e indicadores.';