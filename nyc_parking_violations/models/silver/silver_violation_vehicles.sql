SELECT
    "summons number",
    "registration state",
    "plate type"
    "vehicle body type",
    "vehicle make",
    "vehicle expiration date",
    "vehicle color",
    "vehicle year"
FROM
    {{ref('silver_parking_violations')}}
