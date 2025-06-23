SELECT
    code AS violation_code,
    definition,
    Manhattan_96th_St_and_below,
    All_Other_Areas
FROM
    {{ ref('dof_parking_violation_codes') }}
