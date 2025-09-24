-- create_db.sql
IF DB_ID('OpinionesDB') IS NULL
    CREATE DATABASE OpinionesDB;
GO

USE OpinionesDB;
GO

CREATE TABLE Sources (
    SourceId INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(200) NOT NULL,
    Type NVARCHAR(50) NOT NULL,
    Url NVARCHAR(500) NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Products (
    ProductId INT IDENTITY(1,1) PRIMARY KEY,
    SKU NVARCHAR(100) NOT NULL UNIQUE,
    Name NVARCHAR(250) NOT NULL,
    Category NVARCHAR(150) NULL,
    Price DECIMAL(18,2) NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Customers (
    CustomerId INT IDENTITY(1,1) PRIMARY KEY,
    ExternalId NVARCHAR(100) NULL,
    FullName NVARCHAR(250) NULL,
    Email NVARCHAR(200) NULL,
    Country NVARCHAR(100) NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Surveys (
    SurveyId INT IDENTITY(1,1) PRIMARY KEY,
    SurveyDate DATETIME2 NOT NULL,
    ProductId INT NULL,
    CustomerId INT NULL,
    Rating TINYINT NULL,
    Comment NVARCHAR(MAX) NULL,
    SourceId INT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_Surveys_Products FOREIGN KEY (ProductId) REFERENCES Products(ProductId),
    CONSTRAINT FK_Surveys_Customers FOREIGN KEY (CustomerId) REFERENCES Customers(CustomerId),
    CONSTRAINT FK_Surveys_Sources FOREIGN KEY (SourceId) REFERENCES Sources(SourceId)
);
GO

CREATE TABLE WebReviews (
    ReviewId INT IDENTITY(1,1) PRIMARY KEY,
    ReviewDate DATETIME2 NULL,
    ProductId INT NULL,
    ReviewerName NVARCHAR(200) NULL,
    Rating TINYINT NULL,
    Title NVARCHAR(250) NULL,
    Body NVARCHAR(MAX) NULL,
    SourceId INT NULL,
    Sentiment FLOAT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_WebReviews_Products FOREIGN KEY (ProductId) REFERENCES Products(ProductId),
    CONSTRAINT FK_WebReviews_Sources FOREIGN KEY (SourceId) REFERENCES Sources(SourceId)
);
GO

CREATE TABLE SocialComments (
    CommentId INT IDENTITY(1,1) PRIMARY KEY,
    CommentDate DATETIME2 NULL,
    ProductId INT NULL,
    UserHandle NVARCHAR(200) NULL,
    Body NVARCHAR(MAX) NULL,
    SourceId INT NULL,
    Sentiment FLOAT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_SocialComments_Products FOREIGN KEY (ProductId) REFERENCES Products(ProductId),
    CONSTRAINT FK_SocialComments_Sources FOREIGN KEY (SourceId) REFERENCES Sources(SourceId)
);
GO

CREATE INDEX IX_Products_SKU ON Products(SKU);
CREATE INDEX IX_Customers_Email ON Customers(Email);
GO
