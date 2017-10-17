-- copyright 2017 Martin Lurie 
-- have you changed the default replica count to 1

drop table if exists telematicsobd;
CREATE TABLE telematicsobd
(
  vin BIGINT,
  longitude float,
  latitude float,
  speed float,
  everythingelse string,
  PRIMARY KEY(vin)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU TBLPROPERTIES( 'kudu.master_addresses' = 'gromit:7051');
select * from telematicsobd;
