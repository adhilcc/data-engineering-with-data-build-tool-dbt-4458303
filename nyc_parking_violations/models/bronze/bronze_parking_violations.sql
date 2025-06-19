SELECT
    Summons_Number as summons_number,
    Registration_State ,
    Plate_Type,
    Issue_Date,
    Violation_Code,
    Vehicle_Body_Type,
    Vehicle_Make,
    Issuing_Agency,
    Vehicle_Expiration_Date,
    Violation_Location,
    Violation_Precinct,
    Issuer_Precinct,
    Issuer_Code,
    Issuer_Command,
    Issuer_Squad,
    Violation_Time,
    Violation_County,
    Violation_Legal_Code,
    Vehicle_Color,
    Vehicle_Year
FROM {{ ref('parking_violations_issued_fiscal_year_2023_sample') }}

