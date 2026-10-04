CREATE TABLE [gold].[DimTienda] (

	[tienda_key] int NOT NULL, 
	[tienda_id] int NULL, 
	[nombre_tienda] varchar(max) NULL, 
	[ciudad] varchar(max) NULL, 
	[pais] varchar(max) NULL, 
	[region] varchar(max) NULL, 
	[formato_tienda] varchar(max) NULL, 
	[fecha_apertura] date NULL
);