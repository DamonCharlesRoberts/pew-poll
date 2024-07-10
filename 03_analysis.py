# PEW NPOR analyses.

# Description
#   - Python script that includes various analyeses I have done with the
#    PEW NPOR data.

# Import libraries.
import duckdb as db
import pandas as pd

# Establish connection.
con = db.connect("./data/pew_npors.duckdb")

# Examine the partisanship by age breakdown.
#   a. Unweighted average of those that are registered.
con.sql(
    """
    SELECT
       AVG(republican) AS avg_republican
       , AVG(democrat) AS avg_democrat
    FROM denormalized_view
    WHERE year=2024 
        AND age_cat=1
        AND registered=1;
    """
)
#   b. Unweighted average of those that are NOT registered.
con.sql(
    """
    SELECT
       AVG(republican) AS avg_republican
       , AVG(democrat) AS avg_democrat
    FROM denormalized_view
    WHERE year=2024 
        AND age_cat=1 
        AND registered=0;
    """
)
#   c. Weighted average of those that are registered.
con.sql(
    """
    SELECT
       SUM(weight * republican) / SUM(weight) AS avg_republican
       , SUM(weight * democrat) / SUM(weight) AS avg_democrat
       , SUM(weight) AS N
    FROM denormalized_view
    WHERE year=2024 
        AND age_cat=1
        AND registered=1;
    """
)
#   d. Weighted average of those that are NOT registered.
con.sql(
    """
    SELECT
       SUM(weight * republican) / SUM(weight) AS avg_republican
       , SUM(weight * democrat) / SUM(weight) AS avg_democrat
       , SUM(weight) AS N
    FROM denormalized_view
    WHERE year=2024 
        AND age_cat=1
        AND registered=0
    """
)
#   e. Weighted average of those that may be registered but not sure.
con.sql(
    """
    SELECT
       SUM(weight * republican) / SUM(weight) AS avg_republican
       , SUM(weight * democrat) / SUM(weight) AS avg_democrat
       , SUM(weight) AS N
    FROM denormalized_view
    WHERE year=2024 
        AND age_cat=1
        AND registered_incl_not_sure=1
    """
)
#   f. Weighted average of those that may be registered but not sure in 2023.
con.sql(
    """
    SELECT
       SUM(weight * republican) / SUM(weight) AS avg_republican
       , SUM(weight * democrat) / SUM(weight) AS avg_democrat
       , SUM(weight) AS N
    FROM denormalized_view
    WHERE year=2023 
        AND age_cat=1
        AND registered_incl_not_sure=1
    """
)
#   g. How many say that they are not registered.
con.sql(
    """
    SELECT
        SUM(registered) AS prop_registered
        , COUNT(*) AS N
    FROM denormalized_view
    WHERE year=2024
        AND age_cat=1
    """
)
con.sql(
    """
    SELECT
        SUM(registered) AS prop_registered
        , COUNT(*) AS N
    FROM denormalized_view
    WHERE year=2023
        AND age_cat=1
    """
)
#   h. How many say that they are not sure whether they are registered.
con.sql(
    """
    SELECT
        SUM(registered_incl_not_sure - registered) AS prop_registered
        , COUNT(*) AS N
    FROM denormalized_view
    WHERE year=2024
        AND age_cat=1
    """
)
con.sql(
    """
    SELECT
        SUM(registered_incl_not_sure - registered) AS prop_registered
        , COUNT(*) AS N
    FROM denormalized_view
    WHERE year=2023
        AND age_cat=1
    """
)
#   j. How different are the weights relative to years past.
pd.set_option("display.max_columns", 12)
con.sql(
    """
    SUMMARIZE (
        SELECT
            base_weight
            , weight
        FROM denormalized_view
        WHERE year=2024
            AND age_cat=1
    );
    """
).df()
con.sql(
    """
    SUMMARIZE (
        SELECT
            base_weight
            , weight
        FROM denormalized_view
        WHERE year=2023
            AND age_cat=1
    );
    """
).df()
#   k. What survey modes are the young respondents using?
con.sql(
    """
    SELECT
        AVG(online_mode) AS avg_online
        , AVG(paper_mode) AS avg_paper
        , AVG(phone_mode) AS avg_phone
    FROM denormalized_view
    WHERE year=2024
        AND age_cat=1;
    """
)
con.sql(
    """
    SELECT
        AVG(online_mode) AS avg_online
        , AVG(paper_mode) AS avg_paper
        , AVG(phone_mode) AS avg_phone
    FROM denormalized_view
    WHERE year=2023
        AND age_cat=1;
    """
)
# Close connection.
con.close()