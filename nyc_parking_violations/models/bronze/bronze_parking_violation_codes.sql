SELECT
    code AS violation_code,
    definition,
    manhattan_96th_st_and_below,
    all_other_areas
FROM
    {{ ref('dof_parking_violation_codes') }}
