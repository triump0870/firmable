CREATE SCHEMA IF NOT EXISTS abn_lookup;
CREATE ROLE abn_lookup;
ALTER SCHEMA abn_lookup OWNER TO abn_lookup;
CREATE ROLE abn_lookup_access;
GRANT USAGE ON SCHEMA abn_lookup TO abn_lookup_access;

CREATE SCHEMA IF NOT EXISTS fjcs_lookup;
CREATE ROLE fjcs_lookup;
ALTER SCHEMA fjcs_lookup OWNER TO fjcs_lookup;
CREATE ROLE fjcs_lookup_access;
GRANT USAGE ON SCHEMA fjcs_lookup TO fjcs_lookup_access;

CREATE SCHEMA IF NOT EXISTS afsl_lookup;
CREATE ROLE afsl_lookup;
ALTER SCHEMA afsl_lookup OWNER TO afsl_lookup;
CREATE ROLE afsl_lookup_access;
GRANT USAGE ON SCHEMA afsl_lookup TO afsl_lookup_access;

