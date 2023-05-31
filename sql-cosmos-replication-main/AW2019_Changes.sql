USE AdventureWorks2019
go

/*
EXEC Cosmos.spStart @Collection = 'PersonCache'
EXEC Cosmos.spEnd @Collection = 'PersonCache', @message = 'OK', @success = 1, @documentCount = 19997
EXEC [Cosmos].spGetPersonsToRefresh @DateFrom = '1900-01-01 00:00:00.000', @DateTo = '2022-08-11 09:52:52.577'
EXEC Cosmos.spGetPersonJSON @ItemId = 10
EXEC [Cosmos].spGetPersonsToRefreshv2 @DateFrom = '1900-01-01 00:00:00.000', @DateTo = '2022-08-11 09:52:52.577'

select * from person.Person 
select * from person.PersonPhone 
select * from person.EmailAddress
*/

select * from Cosmos.ImportLog

/* 
	PLAYING AROUND WITH CHANGES
*/

UPDATE Person.Person
SET FirstName = 'Marco', ModifiedDate = GETDATE()
WHERE BusinessEntityID = 10
GO

BEGIN TRANSACTION

	UPDATE Person.Person
	SET FirstName = 'Michael', ModifiedDate = GETDATE()
	WHERE BusinessEntityID = 10

	UPDATE Person.Person
	SET ModifiedDate = GETDATE()
	WHERE BusinessEntityID between 15 and 24

	UPDATE person.PersonPhone 
	SET ModifiedDate = GETDATE()
	WHERE BusinessEntityID between 80 and 89

	UPDATE person.EmailAddress
	SET ModifiedDate = GETDATE()
	WHERE BusinessEntityID between 210 and 219

--ROLLBACK TRANSACTION
COMMIT TRANSACTION
GO

INSERT INTO PERSON.EmailAddress (BusinessEntityID, EmailAddress)
VALUES (3, 'roberto0@hotmail.com')
go


USE Master
go

CREATE DATABASE AW2019Snap
ON PRIMARY (NAME = 'AdventureWorks2017', FILENAME = N'C:\Temp\AdventureWorks2019Snap_Data.mdf')
AS SNAPSHOT OF AdventureWorks2019
go

DROP DATABASE AW2019Snap
GO

RESTORE DATABASE AdventureWorks2019
FROM DATABASE_SNAPSHOT = 'AW2019Snap'
go
