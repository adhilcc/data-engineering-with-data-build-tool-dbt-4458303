SELECT
    "CODE" AS violation_code,
    "DEFINITION" AS definition,
    "Manhattan  96th St. & below" AS manhattan_96th_st_below,
    "All Other Areas" AS all_other_areas
FROM
    {{ ref('dof_parking_violation_codes') }}
