CREATE TABLE [gold].[DimFecha] (

	[fecha_key] int NULL, 
	[fecha] date NOT NULL, 
	[anio] int NOT NULL, 
	[mes] int NOT NULL, 
	[trimestre] int NOT NULL, 
	[nombre_mes] varchar(max) NOT NULL, 
	[dia_semana] varchar(max) NOT NULL
);