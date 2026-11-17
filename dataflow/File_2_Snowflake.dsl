source(output(
		CUSTOMER_ID as string,
		CUSTOMER_NAME as string,
		EMAIL as string,
		PHONE as long,
		STATE as string,
		CREATED_DATE as date,
		SOURCE_SYSTEM as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> srcCustomers
srcCustomers filter(CUSTOMER_ID == 'C0011') ~> filterstate
filterstate sink(allowSchemaDrift: true,
	validateSchema: false,
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	format: 'table',
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	saveOrder: 1,
	stageInsert: true) ~> Snowflake