-- ============================================
-- SCRIPT DE CREACIÓN: DATA WAREHOUSE DE VENTAS
-- Sistema de Análisis de Ventas
-- ============================================

-- Crear base de datos
CREATE DATABASE IF NOT EXISTS DW_Ventas;
USE DW_Ventas;

-- ============================================
-- TABLAS DE DIMENSIONES
-- ============================================

-- Dimensión: Categoría de Productos
CREATE TABLE DIM_CATEGORIA (
    categoria_id INT AUTO_INCREMENT PRIMARY KEY,
    nombre_categoria VARCHAR(100) NOT NULL,
    descripcion_categoria TEXT,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_nombre_categoria (nombre_categoria)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Dimensión de categorías de productos';

-- Dimensión: Ubicación Geográfica
CREATE TABLE DIM_UBICACION (
    ubicacion_id INT AUTO_INCREMENT PRIMARY KEY,
    pais VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    ciudad VARCHAR(100),
    codigo_postal VARCHAR(20),
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_ubicacion (pais, region, ciudad, codigo_postal),
    INDEX idx_pais (pais),
    INDEX idx_ciudad (ciudad)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Dimensión de ubicaciones geográficas';

-- Dimensión: Productos
CREATE TABLE DIM_PRODUCTO (
    producto_id INT AUTO_INCREMENT PRIMARY KEY,
    codigo_producto VARCHAR(50) NOT NULL UNIQUE,
    nombre_producto VARCHAR(200) NOT NULL,
    categoria_id INT NOT NULL,
    precio_unitario DECIMAL(12,2) NOT NULL,
    descripcion TEXT,
    fecha_registro DATE,
    fuente_datos VARCHAR(50) COMMENT 'CSV, API, DB_EXTERNA',
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    activo BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (categoria_id) REFERENCES DIM_CATEGORIA(categoria_id),
    INDEX idx_codigo_producto (codigo_producto),
    INDEX idx_nombre_producto (nombre_producto),
    INDEX idx_categoria (categoria_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Dimensión de productos';

-- Dimensión: Clientes
CREATE TABLE DIM_CLIENTE (
    cliente_id INT AUTO_INCREMENT PRIMARY KEY,
    codigo_cliente VARCHAR(50) NOT NULL UNIQUE,
    nombre_cliente VARCHAR(200) NOT NULL,
    email VARCHAR(150),
    telefono VARCHAR(20),
    ubicacion_id INT,
    tipo_cliente VARCHAR(50) COMMENT 'MINORISTA, MAYORISTA, CORPORATIVO',
    fecha_registro DATE,
    fuente_datos VARCHAR(50) COMMENT 'CSV, API, DB_EXTERNA',
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    activo BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (ubicacion_id) REFERENCES DIM_UBICACION(ubicacion_id),
    INDEX idx_codigo_cliente (codigo_cliente),
    INDEX idx_nombre_cliente (nombre_cliente),
    INDEX idx_tipo_cliente (tipo_cliente),
    INDEX idx_ubicacion (ubicacion_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Dimensión de clientes';

-- Dimensión: Tiempo
CREATE TABLE DIM_TIEMPO (
    tiempo_id INT AUTO_INCREMENT PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    anio INT NOT NULL,
    trimestre INT NOT NULL,
    mes INT NOT NULL,
    nombre_mes VARCHAR(20) NOT NULL,
    semana INT NOT NULL,
    dia INT NOT NULL,
    nombre_dia VARCHAR(20) NOT NULL,
    dia_semana INT NOT NULL COMMENT '1=Lunes, 7=Domingo',
    es_fin_semana BOOLEAN DEFAULT FALSE,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fecha (fecha),
    INDEX idx_anio_mes (anio, mes),
    INDEX idx_trimestre (anio, trimestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Dimensión temporal';

-- ============================================
-- TABLA DE HECHOS
-- ============================================

-- Tabla de Hechos: Ventas
CREATE TABLE FACT_VENTA (
    venta_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    numero_factura VARCHAR(50) NOT NULL,
    producto_id INT NOT NULL,
    cliente_id INT NOT NULL,
    tiempo_id INT NOT NULL,
    ubicacion_id INT NOT NULL,
    cantidad INT NOT NULL,
    precio_unitario DECIMAL(12,2) NOT NULL,
    descuento DECIMAL(12,2) DEFAULT 0.00,
    subtotal DECIMAL(12,2) NOT NULL,
    impuesto DECIMAL(12,2) DEFAULT 0.00,
    total DECIMAL(12,2) NOT NULL,
    fuente_datos VARCHAR(50) COMMENT 'CSV, API, DB_EXTERNA',
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (producto_id) REFERENCES DIM_PRODUCTO(producto_id),
    FOREIGN KEY (cliente_id) REFERENCES DIM_CLIENTE(cliente_id),
    FOREIGN KEY (tiempo_id) REFERENCES DIM_TIEMPO(tiempo_id),
    FOREIGN KEY (ubicacion_id) REFERENCES DIM_UBICACION(ubicacion_id),
    INDEX idx_numero_factura (numero_factura),
    INDEX idx_producto (producto_id),
    INDEX idx_cliente (cliente_id),
    INDEX idx_tiempo (tiempo_id),
    INDEX idx_ubicacion (ubicacion_id),
    INDEX idx_fecha_carga (fecha_carga)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Tabla de hechos de ventas';

-- ============================================
-- PROCEDIMIENTOS ALMACENADOS ÚTILES
-- ============================================

-- Procedimiento para poblar dimensión tiempo
DELIMITER //
CREATE PROCEDURE poblar_dim_tiempo(
    IN fecha_inicio DATE,
    IN fecha_fin DATE
)
BEGIN
    DECLARE fecha_actual DATE;
    SET fecha_actual = fecha_inicio;
    
    WHILE fecha_actual <= fecha_fin DO
        INSERT IGNORE INTO DIM_TIEMPO (
            fecha, anio, trimestre, mes, nombre_mes, 
            semana, dia, nombre_dia, dia_semana, es_fin_semana
        ) VALUES (
            fecha_actual,
            YEAR(fecha_actual),
            QUARTER(fecha_actual),
            MONTH(fecha_actual),
            DATE_FORMAT(fecha_actual, '%M'),
            WEEK(fecha_actual, 3),
            DAY(fecha_actual),
            DATE_FORMAT(fecha_actual, '%W'),
            DAYOFWEEK(fecha_actual),
            IF(DAYOFWEEK(fecha_actual) IN (1, 7), TRUE, FALSE)
        );
        
        SET fecha_actual = DATE_ADD(fecha_actual, INTERVAL 1 DAY);
    END WHILE;
END //
DELIMITER ;

-- ============================================
-- VISTAS ÚTILES PARA ANÁLISIS
-- ============================================

-- Vista: Ventas consolidadas con todas las dimensiones
CREATE VIEW VW_VENTAS_COMPLETAS AS
SELECT 
    fv.venta_id,
    fv.numero_factura,
    dt.fecha,
    dt.anio,
    dt.trimestre,
    dt.mes,
    dt.nombre_mes,
    dp.codigo_producto,
    dp.nombre_producto,
    dc.nombre_categoria,
    dcl.codigo_cliente,
    dcl.nombre_cliente,
    dcl.tipo_cliente,
    du.pais,
    du.region,
    du.ciudad,
    fv.cantidad,
    fv.precio_unitario,
    fv.descuento,
    fv.subtotal,
    fv.impuesto,
    fv.total,
    fv.fuente_datos
FROM FACT_VENTA fv
INNER JOIN DIM_TIEMPO dt ON fv.tiempo_id = dt.tiempo_id
INNER JOIN DIM_PRODUCTO dp ON fv.producto_id = dp.producto_id
INNER JOIN DIM_CATEGORIA dc ON dp.categoria_id = dc.categoria_id
INNER JOIN DIM_CLIENTE dcl ON fv.cliente_id = dcl.cliente_id
INNER JOIN DIM_UBICACION du ON fv.ubicacion_id = du.ubicacion_id;

-- Vista: KPIs principales
CREATE VIEW VW_KPI_VENTAS AS
SELECT 
    COUNT(DISTINCT venta_id) AS total_transacciones,
    COUNT(DISTINCT cliente_id) AS total_clientes,
    COUNT(DISTINCT producto_id) AS total_productos,
    SUM(cantidad) AS unidades_vendidas,
    SUM(total) AS ingresos_totales,
    AVG(total) AS ticket_promedio,
    MAX(total) AS venta_maxima,
    MIN(total) AS venta_minima
FROM FACT_VENTA;

-- ============================================
-- DATOS INICIALES DE EJEMPLO
-- ============================================

-- Insertar categorías de ejemplo
INSERT INTO DIM_CATEGORIA (nombre_categoria, descripcion_categoria) VALUES
('Electrónica', 'Productos electrónicos y tecnología'),
('Ropa', 'Prendas de vestir y accesorios'),
('Alimentos', 'Productos alimenticios y bebidas'),
('Hogar', 'Artículos para el hogar'),
('Deportes', 'Equipamiento deportivo y fitness');

-- Insertar ubicaciones de ejemplo
INSERT INTO DIM_UBICACION (pais, region, ciudad, codigo_postal) VALUES
('República Dominicana', 'Santo Domingo', 'Santo Domingo Este', '11511'),
('República Dominicana', 'Santiago', 'Santiago de los Caballeros', '51000'),
('República Dominicana', 'La Vega', 'La Vega', '41000'),
('Estados Unidos', 'New York', 'New York City', '10001'),
('España', 'Madrid', 'Madrid', '28001');

-- Poblar dimensión tiempo (2020-2026)
CALL poblar_dim_tiempo('2020-01-01', '2026-12-31');

-- ============================================
-- ÍNDICES ADICIONALES PARA OPTIMIZACIÓN
-- ============================================

-- Índices compuestos para consultas frecuentes
CREATE INDEX idx_fact_tiempo_producto ON FACT_VENTA(tiempo_id, producto_id);
CREATE INDEX idx_fact_tiempo_cliente ON FACT_VENTA(tiempo_id, cliente_id);
CREATE INDEX idx_fact_cliente_producto ON FACT_VENTA(cliente_id, producto_id);

-- ============================================
-- COMENTARIOS Y METADATOS
-- ============================================

-- Tabla de metadatos ETL (opcional pero recomendada)
CREATE TABLE ETL_LOG (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    proceso VARCHAR(100) NOT NULL,
    tabla_destino VARCHAR(100),
    registros_procesados INT,
    registros_exitosos INT,
    registros_fallidos INT,
    fuente_datos VARCHAR(50),
    fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_fin TIMESTAMP NULL,
    estado VARCHAR(20) COMMENT 'EXITOSO, FALLIDO, EN_PROCESO',
    mensaje_error TEXT,
    INDEX idx_proceso (proceso),
    INDEX idx_fecha_inicio (fecha_inicio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Log de procesos ETL';

