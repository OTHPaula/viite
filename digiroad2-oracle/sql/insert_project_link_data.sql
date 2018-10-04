alter session set nls_language = 'american' NLS_NUMERIC_CHARACTERS = ', ';

Insert into PROJECT (ID,STATE,NAME,ELY,CREATED_BY,CREATED_DATE,MODIFIED_BY,MODIFIED_DATE,ADD_INFO,START_DATE,STATUS_INFO,CHECK_COUNTER) values ('7081807','1','ProjectOne','1','testUser',to_date('17.08.08','RR.MM.DD'),'-',to_date('17.08.08','RR.MM.DD'),null,to_date('17.08.31','RR.MM.DD'),null,'0');

Insert into PROJECT_RESERVED_ROAD_PART (ID,ROAD_NUMBER,ROAD_PART_NUMBER,PROJECT_ID,CREATED_BY) values ('7081807','77997','1','7081807','testUser');

Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','0','685','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','2','1',null,null,'1','0','[6975409.0,525303],[6975409,525988]','2','0','685,135','6117675','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','685','722','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','0','1',null,null,'1','0','[6975409.0,525988],[6975409,526025]','2','0','36,786','6638374','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','722','799','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','0','1',null,null,'1','0','[6975409.0,526025],[6975409,526102]','2','0','77,673','6638371','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','799','1136','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','0','1',null,null,'1','0','[6975409.0,526102],[6975409,526439]','2','0','336,503','6638357','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','1136','1186','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','0','1',null,null,'1','0','[6975409.0,526439],[6975409,526489]','2','0','49,887','6117732','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','1186','1337','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','1','1',null,null,'1','0','[6975409.0,526489],[6975409,526640]','2','0','151,548','6117725','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','1','5','77997','1','1337','1573','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','2','1',null,null,'1','0','[6975409.0,526640],[6975409,526876]','2','0','212,915','6638363','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','1','5','77997','1','1573','1824','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','0','1',null,null,'1','0','[6975409.0,526876],[6975409,527127]','2','0','226,346','6117633','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','1','5','77997','1','1824','1958','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','1','1',null,null,'1','0','[6975409.0,527127],[6975409,527261]','2','0','121,161','6117621','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','2','5','77997','1','1337','1670','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','2','1',null,null,'1','0','[6975409.0,526640],[6975409,526973]','2','0','366,025','6638300','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','2','5','77997','1','1670','1721','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','0','1',null,null,'1','0','[6975409.0,526973],[6975409,527024]','2','0','55,83','6117634','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','2','5','77997','1','1721','1958','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','1','1',null,null,'1','0','[6975409.0,527024],[6975409,527261]','2','0','260,592','6117622','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','1958','1984','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','2','1',null,null,'1','0','[6975409.0,527261],[6975409,527287]','2','0','26,431','6638330','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','1984','2281','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','0','1',null,null,'1','0','[6975409.0,527287],[6975409,527584]','2','0','296,922','6638318','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','2281','2522','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','0','1',null,null,'1','0','[6975409.0,527584],[6975409,527825]','2','0','240,279','6117602','0','1');
Insert into PROJECT_LINK (ID,PROJECT_ID,TRACK,DISCONTINUITY_TYPE,ROAD_NUMBER,ROAD_PART_NUMBER,START_ADDR_M,END_ADDR_M,CREATED_BY,MODIFIED_BY,CREATED_DATE,MODIFIED_DATE,STATUS,CALIBRATION_POINTS,ROAD_TYPE,ROADWAY_ID,CONNECTED_LINK_ID,ELY,REVERSED,GEOMETRY,SIDE,START_MEASURE,END_MEASURE,LINK_ID,ADJUSTED_TIMESTAMP,LINK_SOURCE) values (viite_general_seq.nextval,'7081807','0','5','77997','1','2522','3213','testUser',null,to_date('08.08.2017','DD.MM.RRRR'),null,'2','1','1',null,null,'1','0','[6975409.0,527825],[6975409,528516]','2','0','691,186','6638304','0','1');

UPDATE PROJECT_LINK SET
  ROAD_TYPE = 1,
  ELY = 1,
  GEOMETRY_STRING = '[6975409.0,' || (525303 + start_addr_m) || '],[6975409,' || (525303 + end_addr_m) || ']';
