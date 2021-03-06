CREATE OR REPLACE TRIGGER ROADNAME_SEQ_TRIGGER
BEFORE INSERT ON ROAD_NAME
FOR EACH ROW
WHEN (NEW.ID IS NULL)
BEGIN
:NEW.ID := ROAD_NAME_SEQ.NEXTVAL;
END;
/

CREATE OR REPLACE TRIGGER VALIDTODATE_VALIDATION
BEFORE INSERT OR UPDATE ON ROADWAY
FOR EACH ROW
WHEN (NEW.START_DATE = NEW.END_DATE)
BEGIN
:NEW.VALID_TO := :NEW.VALID_FROM;
END;
/
