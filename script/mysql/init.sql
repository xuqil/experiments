-- test database --
create database if not exists `integration_test`;
CREATE TABLE if not exists  `integration_test`.`user` (
   id BIGINT auto_increment NOT NULL,
   name varchar(100) NULL,
   age INT NULL,
   PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8
COLLATE=utf8_unicode_ci;

-- canal --
CREATE USER canal IDENTIFIED BY 'canal';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
-- GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
FLUSH PRIVILEGES;



