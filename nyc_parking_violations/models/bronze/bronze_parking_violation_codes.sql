SELECT
    "CODE" AS violation_code,
    "DEFINITION",
    "Manhattan 96th St. & below",
    "All Other Areas"
FROM
    {{ ref('dof_parking_violation_codes') }}
