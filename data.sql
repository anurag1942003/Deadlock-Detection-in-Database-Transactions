drop table Accounts;

CREATE DATABASE Deadlock;
USE Deadlock;

CREATE TABLE Accounts (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    balance DECIMAL(10, 2),
    level INT 
);

INSERT INTO Accounts (id, name, balance,level) VALUES (1, 'Person A', 1000,1);
INSERT INTO Accounts (id, name, balance,level) VALUES (2, 'Person B', 1000,3);
INSERT INTO Accounts (id, name, balance,level) VALUES (3, 'Person C', 1000,2);

INSERT INTO Accounts (id, name, balance,level)  VALUES  (4, 'Person D', 1000,1),
(5, 'Person E', 1000,3), (6, 'Person F', 1000,1), (7, 'Person G', 1000,2), (8, 'Person H', 1000,3), (9, 'Person I', 1000,1), (10, 'Person J', 1000,3), (11, 'Person K', 1000,2), (12, 'Person L', 1000,3), (13, 'Person M', 1000,1), (14, 'Person N', 1000,2), (15,
'Person O', 1000,2),(16, 'Person P', 1000,1),(17, 'Person Q', 1000,3),(18, 'Person R', 1000,2),(19, 'Person S', 1000,1),(20, 'Person T', 1000,3);

CREATE TABLE Transactions (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    src_id INT,
    dest_id INT,
    amount DECIMAL(10, 2),
    transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (src_id) REFERENCES Accounts(id),
    FOREIGN KEY (dest_id) REFERENCES Accounts(id)
    );
