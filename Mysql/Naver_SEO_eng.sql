CREATE DATABASE NaverSEO_example2;


use NaverSEO_example2;


CREATE TABLE Brands (
    BrandID INT PRIMARY KEY AUTO_INCREMENT,
    BrandName VARCHAR(255) NOT NULL
);

CREATE TABLE Products (
    ProductID INT PRIMARY KEY AUTO_INCREMENT,
    BrandID INT NOT NULL,
    ProductName VARCHAR(255) NOT NULL,
    FOREIGN KEY (BrandID) REFERENCES Brands(BrandID)
);

CREATE TABLE SearchKeywords (
    KeywordID INT PRIMARY KEY AUTO_INCREMENT,
    ProductID INT NOT NULL,
    Keyword VARCHAR(255) NOT NULL,
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

ALTER TABLE SearchKeywords ADD KeywordGroup VARCHAR(128) AFTER Keyword;

CREATE TABLE Categories (
    CategoryID INT PRIMARY KEY AUTO_INCREMENT,
    KeywordID INT NOT NULL,
    CategoryName VARCHAR(255) NOT NULL,
    Category_Date DATE,
    FOREIGN KEY (KeywordID) REFERENCES SearchKeywords(KeywordID)
);