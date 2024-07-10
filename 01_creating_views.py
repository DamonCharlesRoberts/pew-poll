# PEW NPOR Views

# Description
#   - Script to define the views of PEW NPOR data.

# Import libraries.
import duckdb as db

# Connect to the db.
con = db.connect("./data/pew_npors.duckdb")

# Create a view converting responses to integers.
con.execute(
    """
    CREATE OR REPLACE VIEW responses_int_view AS (
        SELECT
            resp_id
            , (
                CASE
                    WHEN registration='Yes' THEN 1
                    WHEN registration='Not sure' OR registration='No' THEN 0
                    ELSE NULL
                END
            ) AS registered
            , (
                CASE
                    WHEN registration='Yes' OR registration='Not sure' THEN 1
                    WHEN registration='No' THEN 0
                    ELSE NULL
                END
            ) AS registered_incl_not_sure
            , (
                CASE
                    WHEN age_cat='18-29' THEN 1
                    WHEN age_cat='30-49' THEN 2
                    WHEN age_cat='50-64' THEN 3
                    WHEN age_cat='65+' THEN 4
                    WHEN age_cat='Refused' THEN 0
                    ELSE NULL
                END
            ) AS age_cat
            , (
                CASE
                    WHEN party='Republican' OR party_ln LIKE '%Republican%' THEN 1
                    ELSE 0
                END
            ) AS republican
            , (
                CASE
                    WHEN party='Democrat' OR party_ln LIKE '%Democrat%' THEN 1
                    ELSE 0
                END
            ) AS democrat
        FROM responses
    );
    """
)
# Create a view that denormalizes the view with responses as integers.
con.execute(
    """
    CREATE OR REPLACE VIEW denormalized_view AS (
        SELECT
            responses_int_view.*
            , dim_survey.year
            , (
                CASE
                    WHEN dim_survey.mode='Online' THEN 1
                    ELSE 0
                END
            ) AS online_mode
            , (
                CASE
                    WHEN dim_survey.mode='Paper' THEN 1
                    ELSE 0
                END
            ) AS paper_mode
            , (
                CASE 
                    WHEN dim_survey.mode='Phone' THEN 1
                    ELSE 0
                END
            ) AS phone_mode
            , dim_survey.language
            , dim_survey.language_initial
            , dim_survey.language_pref
            , dim_survey.wave
            , dim_survey.interview_start
            , dim_survey.interview_end
            , dim_survey.device_type
            , dim_survey.stratum
            , dim_survey.census_region
            , dim_survey.metro
            , dim_survey.division
            , dim_weight.base_weight
            , dim_weight.weight
        FROM responses_int_view
            INNER JOIN dim_survey
                ON responses_int_view.resp_id=dim_survey.resp_id
            INNER JOIN dim_weight
                ON responses_int_view.resp_id=dim_weight.resp_id
    )
    """
)
# Close the connection.
con.close()