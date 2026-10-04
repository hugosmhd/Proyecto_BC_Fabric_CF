CREATE TABLE [gold].[DimProducto] (

	[producto_key] int NOT NULL, 
	[sku] varchar(max) NULL, 
	[nombre_producto] varchar(max) NULL, 
	[categoria] varchar(max) NULL, 
	[subcategoria] varchar(max) NULL, 
	[marca] varchar(max) NULL, 
	[costo_unitario] decimal(18,2) NULL
);