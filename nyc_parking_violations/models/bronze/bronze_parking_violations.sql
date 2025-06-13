SELECT
    "Summons Number" as summons_number,
    "Registration State" as Registration_State ,
    "Plate Type" as Plate_Type,
    "Issue Date" as Issue_Date,
    "Violation Code" as Violation_Code,
    "Vehicle Body Type" as Vehicle_Body_Type,
    "Vehicle Make" as Vehicle_Make,
    "Issuing Agency" as Issuing_Agency,
    "Vehicle Expiration Date" as Vehicle_Expiration_Date,
    "Violation Location" as Violation_Location,
    "Violation Precinct" as Violation_Precinct,
    "Issuer Precinct" as Issuer_Precinct,
    "Issuer Code" as Issuer_Code,
    "Issuer Command" as Issuer_Command,
    "Issuer Squad" as Issuer_Squad,
    "Violation Time" as Violation_Time,
    "Violation County" as Violation_County,
    "Violation Legal Code" as Violation_Legal_Code,
    "Vehicle Color" as Vehicle_Color,
    "Vehicle Year" as Vehicle_Year
FROM {{ ref('parking_violations_issued_fiscal_year_2023_sample') }}

