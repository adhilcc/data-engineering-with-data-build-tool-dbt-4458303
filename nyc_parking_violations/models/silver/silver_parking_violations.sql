SELECT
    "Summons Number",
    "Registration State",
    "Plate Type",
    "Issue Date",
    "Violation Code",
    "Vehicle Body Type",
    "Vehicle Make",
    "Issuing Agency",
    "Vehicle Expiration Date",
    "Violation Location",
    "Violation Precinct",
    "Issuer Precinct",
    "Issuer Code",
    "Issuer Command",
    "Issuer Squad",
    "Violation Time",
    "Violation County",
    "Violation Legal Code",
    "Vehicle Color",
    "Vehicle Year",
    CASE WHEN
        "Violation County" == 'MN'
        THEN TRUE
        ELSE FALSE
        END AS is_manhattan_96th_st_below
FROM
    {{ref('bronze_parking_violations')}}
