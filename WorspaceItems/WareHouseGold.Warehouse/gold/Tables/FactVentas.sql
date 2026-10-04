CREATE TABLE [gold].[FactVentas] (

	[transaccion_id] varchar(max) NULL, 
	[fecha_key] int NULL, 
	[tienda_key] int NULL, 
	[producto_key] int NULL, 
	[empleado_key] int NULL, 
	[cliente_key] int NOT NULL, 
	[cantidad] int NULL, 
	[precio_unitario] decimal(18,2) NULL, 
	[descuento_pct] int NULL, 
	[venta_total] decimal(18,2) NULL, 
	[costo_total] decimal(29,2) NULL
);