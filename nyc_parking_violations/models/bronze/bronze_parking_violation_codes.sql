SELECT
    CODE AS violation_code,
    DEFINITION as definition,
    Manhattan_96th_St_and_below,
    All_Other_Areas
FROM
    {{ ref('dof_parking_violation_codes') }}
