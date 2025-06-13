SELECT
    "CODE" AS violation_code,
    "DEFINITION",
    "Manhattan 96th St. & below" as "Manhattan_96th_St_&_below",
    "All Other Areas" as All_Other_Areas
FROM
    {{ ref('dof_parking_violation_codes') }}
