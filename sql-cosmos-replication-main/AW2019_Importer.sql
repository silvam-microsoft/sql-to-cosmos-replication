use AdventureWorks2019
-- can be downloaded here https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver16&tabs=ssms
go

--create new schema for objects
if SCHEMA_ID('Cosmos') is null
	exec('CREATE SCHEMA Cosmos')
go

--create import log table
if object_id('Cosmos.[ImportLog]') is not null
	drop table Cosmos.[ImportLog]
go

CREATE TABLE Cosmos.[ImportLog](
	[ImportLogId] [int] IDENTITY(1,1) NOT NULL,
	[Collection] varchar(100) NOT NULL,
	[DateFrom] datetime NOT NULL,
	[DateTo] datetime NOT NULL,
	[StartDate] [datetime] NOT NULL,
	[EndDate] [datetime] NULL,
	[Success] [bit] NOT NULL,
	[DocumentCount] [int] NULL,
	[Message] [varchar](max) NULL,
 CONSTRAINT [PK_Cosmos_ImportLog] PRIMARY KEY CLUSTERED ([ImportLogId] ASC)
) 
go

--create import log table
if object_id('Cosmos.[Collections]') is not null
	drop table Cosmos.Collections
go

create table cosmos.Collections (
	[Collection] varchar(100) NOT NULL,
	StoredProcedure varchar(100) NOT NULL,
)
go
insert into cosmos.Collections values ('Person','cosmos.spGetPersonsToRefreshV2')
go

if object_id('[Cosmos].[spStart]') is not null
	DROP PROCEDURE [Cosmos].[spStart]
GO
CREATE PROCEDURE [Cosmos].[spStart] (@Collection varchar(100), @DateFrom datetime=null output, @DateTo datetime=null output, @StoredProcedure varchar(100) output)
as 
	--get last sucessfull run	
	select @DateFrom = max(DateTo) from Cosmos.[ImportLog] where Collection = @Collection and [Success] = 1

	select @StoredProcedure = StoredProcedure from cosmos.Collections where [Collection] = @Collection

	if @DateFrom is null
		set @DateFrom = '1/1/1900'--oldest possible date

	if @DateTo is null
		set @DateTo = getdate()

	insert into Cosmos.ImportLog(Collection, DateFrom, DateTo, StartDate, Success)
	values (@Collection, @DateFrom, @DateTo, getdate(), 0)

	select @DateFrom DateFrom, @DateTo DateTo, @StoredProcedure StoredProcedure
GO


if object_id('[Cosmos].[spEnd]') is not null
	DROP PROCEDURE [Cosmos].[spEnd]
go
create proc [Cosmos].[spEnd] (@Collection varchar(100), @message varchar(max), @success bit=1, @DocumentCount int=null)
as 

	declare @ImportLogId int

	--get last unfinished run
	select top 1 @ImportLogId = ImportLogId
		from Cosmos.ImportLog
		where Collection = @Collection 
		and EndDate is null 
		order by ImportLogId desc

	--close import process
	update Cosmos.ImportLog set 
		  Message = @message
		, EndDate = getdate()
		, Success=@success
		, DocumentCount = @DocumentCount
	where ImportLogId = @ImportLogId

GO


if object_id('[Cosmos].[spGetPersonsToRefresh]') is not null
	DROP PROCEDURE [Cosmos].spGetPersonsToRefresh
GO

CREATE PROCEDURE [Cosmos].spGetPersonsToRefresh (@DateFrom datetime, @DateTo datetime)
AS
	if object_id('tempdb..#Person') is not null
		drop table #Person

	create table #Person (ID BIGINT)

	insert into #Person
	select BusinessEntityId 
	from person.Person 
	where [ModifiedDate] between @DateFrom and @DateTo

	insert into #Person
	select BusinessEntityId 
	from person.PersonPhone 
	where [ModifiedDate] between @DateFrom and @DateTo

	insert into #Person
	select BusinessEntityId 
	from [Person].[EmailAddress]
	where [ModifiedDate] between @DateFrom and @DateTo

	--return
	select distinct ID from #Person order by 1
go

CREATE OR ALTER PROCEDURE Cosmos.spGetPersonJSON (@ItemId BIGINT=1)
AS

SELECT
	cast(@ItemId as varchar) as PartitionKey,
	cast((select cast(Person.BusinessEntityID as varchar) as id
			, Person.FirstName
			, Person.LastName
			, (select PhoneNumber as Phone, pnt.Name as Type 
				from Person.PersonPhone Phone 
				join [Person].[PhoneNumberType] pnt on Phone.PhoneNumberTypeID = pnt.PhoneNumberTypeID
				where Person.BusinessEntityID = Phone.BusinessEntityID 
				for json path) PhoneNumbers
			, (select EmailAddress as Email 
				from Person.EmailAddress Email 
				where Person.BusinessEntityID = Email.BusinessEntityID 
				for json path) Emails
			from Person.Person Person
			where Person.BusinessEntityID = @ItemId
			FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
		) as VARCHAR(max)) AS [JSON]
GO

-- EXEC Cosmos.spGetPersonJSON @ItemId = 10


CREATE OR ALTER FUNCTION Cosmos.fnGetPersonJSON (@ItemId BIGINT)
RETURNS TABLE
AS
RETURN
	SELECT
		cast(@ItemId as varchar) as PartitionKey,
		cast((select cast(Person.BusinessEntityID as varchar) as id
				, Person.FirstName
				, Person.LastName
				, (select PhoneNumber as Phone, pnt.Name as Type 
					from Person.PersonPhone Phone 
					join [Person].[PhoneNumberType] pnt on Phone.PhoneNumberTypeID = pnt.PhoneNumberTypeID
					where Person.BusinessEntityID = Phone.BusinessEntityID 
					for json path) PhoneNumbers
				, (select EmailAddress as Email 
					from Person.EmailAddress Email 
					where Person.BusinessEntityID = Email.BusinessEntityID 
					for json path) Emails
				from Person.Person Person
				where Person.BusinessEntityID = @ItemId
				FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
			) as VARCHAR(max)) AS [JSON]
GO


CREATE OR ALTER PROCEDURE [Cosmos].spGetPersonsToRefreshV2 (@DateFrom datetime, @DateTo datetime)
AS
	if object_id('tempdb..#Person') is not null
		drop table #Person

	create table #Person (ID BIGINT)

	insert into #Person
	select BusinessEntityId 
	from person.Person 
	where [ModifiedDate] between @DateFrom and @DateTo

	insert into #Person
	select BusinessEntityId 
	from person.PersonPhone 
	where [ModifiedDate] between @DateFrom and @DateTo

	insert into #Person
	select BusinessEntityId 
	from [Person].[EmailAddress]
	where [ModifiedDate] between @DateFrom and @DateTo

	--return
	select distinct ID, D.* 
	from #Person AS P cross apply Cosmos.fnGetPersonJSON(P.ID) AS D  order by 1
go

exec [Cosmos].spGetPersonsToRefreshV2 '1/1/2000','12/31/2030'
