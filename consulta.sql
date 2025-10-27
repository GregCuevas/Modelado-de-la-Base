-- ============================================
-- CONSULTAS SQL PARA ANÁLISIS DE VENTAS
-- Responden a las preguntas del enunciado
-- ============================================

USE DW_Ventas;

-- ============================================
-- 1. ANÁLISIS GENERAL DE VENTAS
-- ============================================

-- 1.1 Total de ventas global registrado
SELECT 
    SUM(total) AS ventas_totales,
    SUM(cantidad) AS unidades_vendidas,
    COUNT(DISTINCT venta_id) AS total_transacciones
FROM FACT_VENTA;

-- 1.2 Promedio de ventas por transacción
SELECT 
    AVG(total) AS promedio_venta,
    MIN(total) AS venta_minima,
    MAX(total) AS venta_maxima,
    STDDEV(total) AS desviacion_estandar
FROM FACT_VENTA;

-- 1.3 Ventas totales en un periodo específico
-- Por día
SELECT 
    dt.fecha,
    dt.nombre_dia,
    COUNT(fv.venta_id) AS num_transacciones,
    SUM(fv.total) AS ventas_totales
FROM FACT_VENTA fv
INNER JOIN DIM_TIEMPO dt ON fv.tiempo_id = dt.tiempo_id
WHERE dt.fecha BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY dt.fecha, dt.nombre_dia
ORDER BY dt.fecha;

-- Por mes
SELECT 
    dt.anio,
    dt.mes,
    dt.nombre_mes,
    COUNT(fv.venta_id) AS num_transacciones,
    SUM(fv.total) AS ventas_totales,
    AVG(fv.total) AS ticket_promedio
FROM FACT_VENTA fv
INNER JOIN DIM_TIEMPO dt ON fv.tiempo_id = dt.tiempo_id
WHERE dt.anio = 2024
GROUP BY dt.anio, dt.mes, dt.nombre_mes
ORDER BY dt.mes;

-- Por año
SELECT 
    dt.anio,
    COUNT(fv.venta_id) AS num_transacciones,
    SUM(fv.total) AS ventas_totales,
    AVG(fv.total) AS ticket_promedio
FROM FACT_VENTA fv
INNER JOIN DIM_TIEMPO dt ON fv.tiempo_id = dt.tiempo_id
GROUP BY dt.anio
ORDER BY dt.anio;

-- 1.4 Volumen de ventas por país, región y ciudad
SELECT 
    du.pais,
    du.region,
    du.ciudad,
    COUNT(fv.venta_id) AS num_transacciones,
    SUM(fv.cantidad) AS unidades_vendidas,
    SUM(fv.total) AS ventas_totales
FROM FACT_V