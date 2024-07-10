# Data munging of the PEW NPOR.

# Load libraries.
#   - Standard libraries.
import subprocess
import shlex
import duckdb as db

from utilities.dataConvert import sav_to_parquet

# Convert the files to parquet.
dir = "/Users/dcr/Desktop/pew-npors"
sav_to_parquet(dir=dir)

# Place in a OLAP DB.
#   1. Establish connection.
con = db.connect("./data/pew_npors.duckdb")
#   2. Create the fact table.
con.execute(
    """
    CREATE TABLE responses (
        resp_id BIGINT PRIMARY KEY NOT NULL
        , econ_1_mod TEXT
        , econ_1b_mod TEXT
        , com_type_2 TEXT
        , unity TEXT
        , typology_b TEXT
        , crime_safe TEXT
        , infra_spend TEXT
        , police_fund TEXT
        , more_gun_impact TEXT
        , gamble_restr TEXT
        , gun_strict TEXT
        , vet_1 TEXT
        , vol_12_cps TEXT
        , em_in_use TEXT
        , int_mob TEXT
        , int_freq TEXT
        , int_freq_collapsed TEXT
        , home_4_nw2 TEXT
        , bb_home TEXT
        , sm_use_a TEXT
        , sm_use_b TEXT
        , sm_use_c TEXT
        , sm_use_d TEXT
        , sm_use_e TEXT
        , sm_use_f TEXT
        , sm_use_g TEXT
        , sm_use_h TEXT
        , sm_use_i TEXT
        , sm_use_j TEXT
        , sm_use_k TEXT
        , radio TEXT
        , books_1 TEXT
        , books_1_refused TEXT
        , device_1a TEXT
        , smart_2 TEXT
        , nhisll TEXT
        , relig TEXT
        , relig_cat_1 TEXT
        , born TEXT
        , attend TEXT
        , attend_online_2 TEXT
        , rel_imp TEXT
        , pray TEXT
        , registration TEXT
        , party TEXT
        , party_ln TEXT
        , party_sum TEXT
        , hisp TEXT
        , race_mod_1 TEXT
        , race_mod_2 TEXT
        , race_mod_3 TEXT
        , race_mod_4 TEXT
        , race_mod_5 TEXT
        , race_mod_6 TEXT
        , race_mod_99 TEXT
        , race_mod_mixed TEXT
        , race_cmb TEXT
        , race_ethnicity TEXT
        , age TEXT
        , age_cat TEXT
        , age_refused TEXT
        , birth_place TEXT
        , gender TEXT
        , marital TEXT
        , education TEXT
        , educ_cat TEXT
        , adults TEXT
        , vote_2020 TEXT
        , vote_gen_post TEXT
        , inc_sdt1 TEXT
    );
    """
)
#   3. Insert the 2023 data into the fact table.
con.execute(
    """
    INSERT INTO responses (
        resp_id, econ_1_mod, econ_1b_mod, com_type_2
        , typology_b, crime_safe, police_fund, more_gun_impact
        , gun_strict, vet_1, vol_12_cps, em_in_use, int_mob
        , int_freq, int_freq_collapsed, home_4_nw2, bb_home
        , sm_use_a, sm_use_b, sm_use_c, sm_use_d, sm_use_e
        , sm_use_f, sm_use_g, sm_use_h, sm_use_i, sm_use_j
        , sm_use_k, radio, books_1, books_1_refused, device_1a
        , smart_2, nhisll, relig, relig_cat_1, born
        , attend, attend_online_2, rel_imp, pray, registration, party
        , party_ln, party_sum, hisp, race_mod_1, race_mod_2, race_mod_3
        , race_mod_4, race_mod_5, race_mod_6, race_mod_99, race_mod_mixed
        , race_cmb, race_ethnicity, age, age_cat, age_refused, birth_place
        , gender, marital, education, educ_cat, adults
        , vote_2020, vote_gen_post, inc_sdt1
    )
    SELECT
        RESPID, ECON1MOD, ECON1BMOD, COMTYPE2, TYPOLOGYB
        , CRIMESAFE, POLICE_FUND, MOREGUNIMPACT, GUNSTRICT, VET1, VOL12_CPS
        , EMINUSE, INTMOB, INTFREQ, INTFREQ_COLLAPSED, HOME4NW2, BBHOME
        , SMUSE_a, SMUSE_b, SMUSE_c, SMUSE_d, SMUSE_e, SMUSE_f, SMUSE_g
        , SMUSE_h, SMUSE_i, SMUSE_j, SMUSE_k, RADIO, BOOKS1, BOOKS1_REFUSED
        , DEVICE1A, SMART2, NHISLL, RELIG, RELIGCAT1, BORN, ATTEND, ATTENDONLINE2
        , RELIMP, PRAY, REGISTRATION, PARTY, PARTYLN, PARTYSUM, HISP, RACEMOD_1
        , RACEMOD_2, RACEMOD_3, RACEMOD_4, RACEMOD_5, RACEMOD_6, RACEMOD_99
        , RACEMOD_MIXEDOE, RACECMB, RACETHN, AGE, AGECAT, AGE_REFUSED, BIRTHPLACE
        , GENDER, MARITAL, EDUCATION, EDUCCAT, ADULTS, VOTED2020, VOTEGEN_POST
        , INC_SDT1
    FROM './data/npors_2023.parquet'
    """
)
#   4. Insert the 2024 data into the fact table.
con.execute(
    """
    INSERT INTO responses (
        resp_id, econ_1_mod, econ_1b_mod, com_type_2
        , unity, crime_safe, infra_spend, more_gun_impact
        , gamble_restr, vet_1, vol_12_cps, em_in_use, int_mob
        , int_freq, int_freq_collapsed, radio, smart_2, device_1a
        , nhisll, relig_cat_1, registration, party
        , party_ln, party_sum, hisp, race_mod_1, race_mod_2, race_mod_3
        , race_mod_4, race_mod_5, race_mod_6, race_mod_99, race_mod_mixed
        , race_cmb, race_ethnicity, age, age_cat, birth_place
        , gender, marital, education, educ_cat, adults
        , vote_2020, vote_gen_post, inc_sdt1
    )
    SELECT
        RESPID, ECON1MOD, ECON1BMOD, COMTYPE2
        , UNITY, CRIMESAFE, INFRASPEND, MOREGUNIMPACT, GAMBLERESTR, VET1, VOL12_CPS
        , EMINUSE, INTMOB, INTFREQ, INTFREQ_COLLAPSED, RADIO, SMART2
        , DEVICE1A, NHISLL, RELIGCAT1, REGISTRATION, PARTY, PARTYLN
        , PARTYSUM, HISP, RACEMOD_1, RACEMOD_2, RACEMOD_3, RACEMOD_4
        , RACEMOD_5, RACEMOD_6, RACEMOD_99, RACEMOD_MIXEDOE, RACECMB
        , RACETHN, AGE, AGECAT, BIRTHPLACE, GENDER, MARITAL, EDUCATION
        , EDUCCAT, ADULTS, VOTED2020, VOTEGEN_POST, INC_SDT1
    FROM './data/npors_2024.parquet'
    """
)
#   5. Create the administration dimension table.
con.execute(
    """
    CREATE TABLE dim_survey (
        resp_id BIGINT NOT NULL
        , FOREIGN KEY (resp_id) REFERENCES responses(resp_id)
        , year INTEGER CHECK (CAST(year AS TEXT) SIMILAR TO '^[0-9]{4}$')
        , mode TEXT
        , language TEXT
        , language_initial TEXT
        , language_pref TEXT
        , wave TEXT
        , interview_start TEXT
        , interview_end TEXT
        , device_type TEXT
        , stratum TEXT
        , census_region TEXT
        , metro TEXT
        , division TEXT
    );
    INSERT INTO dim_survey (
        resp_id, year, mode
        , language_pref, interview_start, interview_end
        , device_type, stratum, census_region, metro, division
    )
    SELECT
        RESPID, 2023 AS year, MODE_2WAY, LANG_PREF, INTERVIEW_START, INTERVIEW_END
        , DEVICE_TYPE, STRATUM, CREGION, METRO, DIVISION
    FROM './data/npors_2023.parquet';
    INSERT INTO dim_survey (
        resp_id, year, mode
        , language, language_pref, interview_start, interview_end
        , wave, stratum, census_region, metro, division
    )
    SELECT
        RESPID, 2024 AS year, MODE, LANGUAGE, LANG_PREF, SURVEYSTARTDATE, SURVEYENDDATE
        , BWAVE, BIDENT1_GEOSTRATA, CREGION, METRO, DIVISION
    FROM './data/npors_2024.parquet';
    """
)
#   6. Create weights dimension table.
con.execute(
    """
    CREATE TABLE dim_weight (
        resp_id BIGINT NOT NULL
        , FOREIGN KEY (resp_id) REFERENCES responses(resp_id)
        , base_weight DOUBLE
        , weight DOUBLE
    );
    INSERT INTO dim_weight (
        resp_id, base_weight, weight
    )
    SELECT RESPID, BASEWT, WEIGHT
    FROM './data/npors_2023.parquet';
    INSERT INTO dim_weight (
        resp_id, base_weight, weight
    )
    SELECT RESPID, BASEWT, WEIGHT
    FROM './data/npors_2024.parquet';
    """
)

# Delete the temp and original data.
subprocess.call(shlex.split(f"./data_cleanup.sh {dir}"))

# Close connection.
con.close()
