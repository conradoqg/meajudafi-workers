ALTER TABLE public.investment_return_daily
    ADD COLUMN cdi_investment_return double precision,
    ADD COLUMN cdi_investment_return_1y double precision,
    ADD COLUMN cdi_investment_return_2y double precision,
    ADD COLUMN cdi_investment_return_3y double precision,
    ADD COLUMN cdi_accumulated_investment_return double precision;

CREATE OR REPLACE VIEW investment_return_daily_fullname AS
SELECT
    id as ird_id,
    cnpj_fundo as ird_cnpj_fundo,
    dt_comptc as ird_dt_comptc,
    investment_return as ird_investment_return,	
    investment_return_1y as ird_investment_return_1y,
    investment_return_2y as ird_investment_return_2y,
    investment_return_3y as ird_investment_return_3y,
    accumulated_investment_return as ird_accumulated_investment_return, 
    risk_1y as ird_risk_1y,
    risk_2y as ird_risk_2y,
    risk_3y as ird_risk_3y,
    accumulated_risk as ird_accumulated_risk,
    sharpe_1y as ird_sharpe_1y,
    sharpe_2y as ird_sharpe_2y,
    sharpe_3y as ird_sharpe_3y,
    accumulated_sharpe as ird_accumulated_sharpe,    
    consistency_1y as ird_consistency_1y,
    consistency_2y as ird_consistency_2y,
    consistency_3y as ird_consistency_3y,
    networth as ird_networth,
    quotaholders as ird_quotaholders,
    cdi_investment_return as ird_cdi_investment_return,
    cdi_investment_return_1y as ird_cdi_investment_return_1y,
    cdi_investment_return_2y as ird_cdi_investment_return_2y,
    cdi_investment_return_3y as ird_cdi_investment_return_3y,
    cdi_accumulated_investment_return as ird_cdi_accumulated_investment_return
FROM investment_return_daily;

ALTER TABLE public.investment_return_monthly
    ADD COLUMN cdi_investment_return double precision,
    ADD COLUMN cdi_investment_return_1y double precision,
    ADD COLUMN cdi_investment_return_2y double precision,
    ADD COLUMN cdi_investment_return_3y double precision,
    ADD COLUMN cdi_accumulated_investment_return double precision;

CREATE OR REPLACE VIEW investment_return_monthly_fullname AS
SELECT
    id as irm_id,
    cnpj_fundo as irm_cnpj_fundo,
    dt_comptc as irm_dt_comptc,
    investment_return as irm_investment_return,
    investment_return_1y as irm_investment_return_1y,
    investment_return_2y as irm_investment_return_2y,
    investment_return_3y as irm_investment_return_3y,
    accumulated_investment_return as irm_accumulated_investment_return,
    risk as irm_risk,
    risk_1y as irm_risk_1y,
    risk_2y as irm_risk_2y,
    risk_3y as irm_risk_3y,
    accumulated_risk as irm_accumulated_risk,
    sharpe as irm_sharpe,
    sharpe_1y as irm_sharpe_1y,
    sharpe_2y as irm_sharpe_2y,
    sharpe_3y as irm_sharpe_3y,
    accumulated_sharpe as irm_accumulated_sharpe,
    consistency_1y as irm_consistency_1y,
    consistency_2y as irm_consistency_2y,
    consistency_3y as irm_consistency_3y,
    networth as irm_networth,
    quotaholders as irm_quotaholders,
    cdi_investment_return as irm_cdi_investment_return,
    cdi_investment_return_1y as irm_cdi_investment_return_1y,
    cdi_investment_return_2y as irm_cdi_investment_return_2y,
    cdi_investment_return_3y as irm_cdi_investment_return_3y,
    cdi_accumulated_investment_return as ird_cdi_accumulated_investment_return
FROM investment_return_monthly;

ALTER TABLE public.investment_return_yearly
    ADD COLUMN cdi_investment_return double precision,
    ADD COLUMN cdi_investment_return_1y double precision,
    ADD COLUMN cdi_investment_return_2y double precision,
    ADD COLUMN cdi_investment_return_3y double precision,
    ADD COLUMN cdi_accumulated_investment_return double precision;

CREATE OR REPLACE VIEW investment_return_yearly_fullname AS
SELECT 
    id as iry_id,
    cnpj_fundo as iry_cnpj_fundo,
    dt_comptc as iry_dt_comptc,
    investment_return as iry_investment_return,	
    investment_return_1y as iry_investment_return_1y,
    investment_return_2y as iry_investment_return_2y,
    investment_return_3y as iry_investment_return_3y,
    accumulated_investment_return as iry_accumulated_investment_return,
    risk as iry_risk,
    risk_1y as iry_risk_1y,
    risk_2y as iry_risk_2y,
    risk_3y as iry_risk_3y,
    accumulated_risk as iry_accumulated_risk,
    sharpe as iry_sharpe,
    sharpe_1y as iry_sharpe_1y,
    sharpe_2y as iry_sharpe_2y,
    sharpe_3y as iry_sharpe_3y,
    accumulated_sharpe as iry_accumulated_sharpe,
    consistency_1y as iry_consistency_1y,
    consistency_2y as iry_consistency_2y,
    consistency_3y as iry_consistency_3y,
    networth as iry_networth,
    quotaholders as iry_quotaholders,
    cdi_investment_return as iry_cdi_investment_return,
    cdi_investment_return_1y as iry_cdi_investment_return_1y,
    cdi_investment_return_2y as iry_cdi_investment_return_2y,
    cdi_investment_return_3y as iry_cdi_investment_return_3y,
    cdi_accumulated_investment_return as iry_cdi_accumulated_investment_return
FROM investment_return_yearly;