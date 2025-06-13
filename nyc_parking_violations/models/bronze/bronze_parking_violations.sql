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
    "Vehicle Year"
FROM {{ ref('parking_violations_issued_fiscal_year_2023_sample') }}

