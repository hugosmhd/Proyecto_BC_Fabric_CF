CREATE TABLE [gold].[DimEmpleado] (

	[empleado_key] int NOT NULL, 
	[empleado_id] int NULL, 
	[nombre_completo] varchar(max) NULL, 
	[tienda_id] int NULL, 
	[puesto] varchar(max) NULL, 
	[fecha_ingreso] date NULL, 
	[salario] decimal(18,2) NULL, 
	[documento_identidad] varchar(max) NULL
);