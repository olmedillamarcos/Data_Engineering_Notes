/* External Tables gives more flexibility on the type of data you can access
and scurity mechanisms you can specify, also defining the table allows you to start working 
if it right away. If we want only a quick reading of the data we can use OPENROWSET.
It uses Azure Active Directory authentication, through the container access control 
(adding role assignment)
*/
SELECT *
FROM OPENROWSET(BULK 'https://storageolmed.blob.core.windows.net/parquet/*.parquet',
                FORMAT = 'PARQUET') AS [logdata_temp]