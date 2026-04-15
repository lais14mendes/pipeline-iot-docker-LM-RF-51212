-- ============================================================
-- views.sql
-- Definição das 3 views analíticas do Pipeline IoT
-- Disciplina: Disruptive Architectures: IoT, Big Data e IA
-- UNIFECAF | Lais Goncalves Mendes | RA: 51212
-- ============================================================
-- OBSERVAÇÃO: As views são criadas automaticamente pelo script
-- load_data.py. Este arquivo serve como documentação e
-- versionamento das consultas SQL no repositório.
-- ============================================================

-- View 1: Temperatura média por dispositivo
-- Objetivo: identificar sensores com temperaturas elevadas
--           para ações de manutenção preditiva
CREATE OR REPLACE VIEW avg_temp_por_dispositivo AS
SELECT
    device_id,
    ROUND(AVG(temp)::numeric, 2) AS avg_temp,
    COUNT(*)                     AS total_leituras
FROM temperature_readings
GROUP BY device_id
ORDER BY avg_temp DESC;


-- View 2: Distribuição de leituras por hora do dia
-- Objetivo: identificar padrões temporais de monitoramento
--           e otimizar frequência de amostragem dos sensores
CREATE OR REPLACE VIEW leituras_por_hora AS
SELECT
    EXTRACT(HOUR FROM noted_date)::int AS hora,
    COUNT(*)                            AS contagem,
    ROUND(AVG(temp)::numeric, 2)        AS temp_media
FROM temperature_readings
GROUP BY hora
ORDER BY hora;


-- View 3: Amplitude térmica diária
-- Objetivo: monitorar variação diária de temperatura
--           e detectar anomalias térmicas por data
CREATE OR REPLACE VIEW temp_max_min_por_dia AS
SELECT
    DATE(noted_date)             AS data,
    ROUND(MAX(temp)::numeric, 2) AS temp_max,
    ROUND(MIN(temp)::numeric, 2) AS temp_min,
    ROUND(AVG(temp)::numeric, 2) AS temp_media
FROM temperature_readings
GROUP BY DATE(noted_date)
ORDER BY data;
