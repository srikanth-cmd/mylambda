const AWS = require('aws-sdk');
const athena = new AWS.Athena();

module.exports.createAthenaResources = async (event) => {
  const dbName = process.env.ATHENA_DATABASE;
  const tableName = process.env.ATHENA_TABLE;
  const workgroup = process.env.ATHENA_WORKGROUP;
  const s3Bucket = process.env.S3_BUCKET;

  const workgroupExists = await checkIfWorkgroupExists(workgroup);
  if(!workgroupExists){
    console.log(`workgroup ${workgroup} doesnt exist`);
    await createAthenaWorkgroup(workgroup)
  }else{
    console.log("workgroup already exists")
  }

  // Create Athena Database
  console.log("before db", dbName);
  const createDatabaseQuery = `CREATE DATABASE IF NOT EXISTS ${dbName}`;
  await executeAthenaQuery(createDatabaseQuery);
  console.log("afterdb");

  //await restrictWorkgroupToDatabase(workgroup, dbName);

  // Create Athena Table
  const deletequery = `DROP TABLE IF EXISTS ${dbName}.${tableName}`;
  const res = await executeAthenaQuery(deletequery);
  const createTableQuery =  `
  CREATE EXTERNAL TABLE ${dbName}.\`${tableName}\` (
    newimage STRUCT<
      vehiclesite:STRING,
      collectinfo:STRING,
      armliftdate:STRING,
      additionalmedia:ARRAY<STRUCT<destmediaurl:STRING>>,
      routename:STRING,
      stopaddress:STRING,
      customeruniqueid:STRING,
      scheduleid:STRING,
      longitude:STRING,
      eventid:STRING,
      materialcategory:STRING,
      transactionid:STRING,
      lob:STRING,
      containerserialnumber:STRING,
      vehiclename:STRING,
      servicecode:STRING,
      sourcesystem:STRING,
      latitude:STRING,
      hoccode:STRING,
      vehiclelineofbusiness:STRING,
      routeid:STRING,
      destmediaurl:STRING,
      customernumber:STRING,
      customersite:STRING,
      serviceuniqueid:STRING,
      siteid:STRING,
      customermaterialcategory:STRING,
      status:STRING
    >,
    keys STRUCT<
      siteid:STRUCT<s:STRING>,
      transactionid:STRUCT<s:STRING>
    > COMMENT 'from deserializer',
    oldimage STRUCT<
      vehiclesite:STRUCT<s:STRING>,
      collectinfo:STRUCT<s:STRING>,
      armliftdate:STRUCT<s:STRING>,
      additionalmedia:STRUCT<
        l:ARRAY<STRUCT<m:STRUCT<destmediaurl:STRUCT<s:STRING>>>>
      >,
      routename:STRUCT<s:STRING>,
      stopaddress:STRUCT<s:STRING>,
      customeruniqueid:STRUCT<s:STRING>,
      scheduleid:STRUCT<s:STRING>,
      longitude:STRUCT<n:STRING>,
      eventid:STRUCT<n:STRING>,
      materialcategory:STRUCT<s:STRING>,
      transactionid:STRUCT<s:STRING>,
      lob:STRUCT<s:STRING>,
      containerserialnumber:STRUCT<s:STRING>,
      vehiclename:STRUCT<s:STRING>,
      servicecode:STRUCT<s:STRING>,
      sourcesystem:STRUCT<s:STRING>,
      latitude:STRUCT<n:STRING>,
      hoccode:STRUCT<s:STRING>,
      vehiclelineofbusiness:STRUCT<s:STRING>,
      routeid:STRUCT<s:STRING>,
      destmediaurl:STRUCT<s:STRING>,
      customernumber:STRUCT<s:STRING>,
      customersite:STRUCT<s:STRING>,
      serviceuniqueid:STRUCT<s:STRING>,
      siteid:STRUCT<s:STRING>,
      customermaterialcategory:STRUCT<s:STRING>,
      status:STRUCT<n:STRING>
    >
  )
  PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING,
    hour STRING
  )
  ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
  LOCATION
    's3://mytxndata227/'
`;
  await executeAthenaQuery(createTableQuery);
  const loadPartition = `MSCK REPAIR TABLE ${dbName}.${tableName}`;
  await executeAthenaQuery(loadPartition);

  //await restrictWorkgroupToDatabase(workgroup, process.env.ATHENA_DATABASE);


  return {
    statusCode: 200,
    body: JSON.stringify({ message: 'Athena resources created successfully' }),
  };
};

// Execute Athena Query
const executeAthenaQuery = async (query) => {
  const params = {
    QueryString: query,
    ResultConfiguration: {
      OutputLocation: `s3://${process.env.S3_BUCKET}/query-results/`,
    },
    WorkGroup: process.env.ATHENA_WORKGROUP,
  };

  try {
    console.log("query:", query)
    const result = await athena.startQueryExecution(params).promise();
    console.log('Query started successfully:', result);
    await waitForQueryToComplete(result.QueryExecutionId);
    return result;
  } catch (error) {
    console.error('Error executing query:', error);
    throw new Error('Error executing Athena query');
  }
};

// Check if Athena Workgroup exists
const checkIfWorkgroupExists = async (workgroupName) => {
  try {
      const res = await athena.getWorkGroup({ WorkGroup: workgroupName }).promise();
      return true;
  } catch (error) {
      if (error.code === 'InvalidRequestException') {
          return false;
      }
      console.error('Error checking for Athena workgroup:', error);
      throw new Error('Error checking for Athena workgroup');
  }
};

// Create Athena Workgroup if not created
const createAthenaWorkgroup = async (workgroupName) => {
  const athenaQueryBucket = process.env.ATHENA_QUERY_BUCKET;
  const executionRoleArn = `arn:aws:iam::222634370665:role/smarttruck-athena-role`; // Make sure to define the execution role ARN

  const params = {
    Name: workgroupName,  // Name of the Athena workgroup
    Description: 'smarttruck workgroup',  // Description of the workgroup
    Configuration: {
      ResultConfiguration: {
        OutputLocation: `s3://${process.env.S3_BUCKET}/query-results/`,  // S3 bucket for query results
        EncryptionConfiguration: {
          EncryptionOption: 'SSE_S3',  // Encryption for query results
        },
      },
      ExecutionRole: executionRoleArn,  // Specify execution role for the workgroup
      EnforceWorkGroupConfiguration: true,  // Enforce the configuration for the workgroup
      // Set the default database for the workgroup
      QueryExecutionContext: {
        Database: process.env.ATHENA_DATABASE,  // Restrict the workgroup to a specific database
      },
    },
  };

  try {
      await athena.createWorkGroup(params).promise();
      console.log(`Smarttruck Athena Workgroup '${workgroupName}' created.`);
      //await restrictWorkgroupToDatabase(workgroupName, process.env.ATHENA_DATABASE);
  } catch (error) {
      console.error('Error creating Athena workgroup:', error);
      throw new Error('Error creating Athena workgroup');
  }
};

const waitForQueryToComplete = async (queryExecutionId) => {
    try {
        let queryStatus = 'RUNNING';
       
        // Poll every 5 seconds until the query status is either SUCCEEDED, FAILED, or CANCELLED
        while (queryStatus === 'RUNNING' || queryStatus === 'QUEUED') {
            // Get the status of the query execution
            const statusResult = await athena.getQueryExecution({ QueryExecutionId: queryExecutionId }).promise();
            queryStatus = statusResult.QueryExecution.Status.State;

            console.log('Current query status:', queryStatus);

            // If the query succeeded, exit the loop
            if (queryStatus === 'SUCCEEDED') {
                console.log('Query succeeded');
                return;
            }

            // If the query failed or was cancelled, throw an error
            if (queryStatus === 'FAILED' || queryStatus === 'CANCELLED') {
                throw new Error(`Query failed or was cancelled: ${statusResult.QueryExecution.Status.State}`);
            }

            // Wait before checking status again
            await new Promise(resolve => setTimeout(resolve, 2000));  // Wait for 5 seconds
        }
    } catch (error) {
        console.error('Error waiting for query completion:', error);
        throw new Error('Error waiting for Athena query to complete');
    }
};

const restrictWorkgroupToDatabase = async (workgroupName, databaseName) => {
  const params = {
    WorkGroup: workgroupName,
    ConfigurationUpdates: {
      WorkGroupQueries: {
        QueryExecutionContext: {
          Database: databaseName,  // Restrict the workgroup to a specific database
        },
      },
    },
  };

  try {
    await athena.updateWorkGroup(params).promise();
    console.log(`Workgroup ${workgroupName} is now restricted to database ${databaseName}.`);
  } catch (error) {
    console.error('Error restricting workgroup to database:', error);
    throw new Error('Error restricting workgroup to database');
  }
};

