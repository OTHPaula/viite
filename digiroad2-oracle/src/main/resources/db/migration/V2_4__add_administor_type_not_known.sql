insert into enumerated_value (id, value, name_fi, name_sv, created_by, property_id)
values (primary_key_seq.nextval, 99, 'Ei tiedossa', ' ', 'db_migration_v2.4', (select id from property where name_fi = 'Tietojen ylläpitäjä'));