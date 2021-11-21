CREATE TABLE warehouse (
    w_warehouse_sk int  NOT NULL,
    w_warehouse_id char(16),
    w_warehouse_name varchar(20),
    w_warehouse_sq_ft int,
    w_street_number varchar(60),
    w_street_name varchar,
    w_street_type char(15),
    w_suite_number char(10),
    w_city varchar(60),
    w_county varchar(30),
    w_state char(2),
    w_zip char(10),
    w_country varchar(20),
    w_gmt_offset decimal(5,2),
    PRIMARY KEY (w_warehouse_sk) );