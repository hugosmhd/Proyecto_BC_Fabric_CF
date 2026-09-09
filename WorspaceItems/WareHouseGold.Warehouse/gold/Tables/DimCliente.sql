CREATE TABLE [gold].[DimCliente] (

	[cliente_key] int NOT NULL, 
	[cliente_id] int NULL, 
	[nombre_completo] varchar(max) NULL, 
	[fecha_registro] date NULL, 
	[tier_lealtad] varchar(max) NULL, 
	[ciudad] varchar(max) NULL
);