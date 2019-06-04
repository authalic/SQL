SELECT
    INCIDENT.number as LocalIncidentID,
    FORMAT(INCIDENT.startDate, 'MM/dd/yyyy') as FireDiscoveryDate,
    INCIDENT.name as IncidentName,
    FORMAT(INCIDENT.startDate, 'HHmm') as FireDiscoveryTime,
    FORMAT(INCIDENT.endDate, 'MM/dd/yyyy') as FireContainmentDate,
    FORMAT(INCIDENT.endDate, 'HHmm') as FireContainmentTime,
    LEFT(INCIDENT.number, 5) as FireReportingAgencyUnitIdentifier,
    'UT' as State,  -- non-Utah fires are filtered out in the WHERE clause
    '49' as StateFIPS,
    COUNTYNAME.name as CountyName,
    "CountyFIPS" = 
        CASE
            WHEN COUNTYNAME.id = 2  THEN '49001' -- Beaver
            WHEN COUNTYNAME.id = 3  THEN '49003' -- Box Elder
            WHEN COUNTYNAME.id = 4  THEN '49005' -- Cache
            WHEN COUNTYNAME.id = 5  THEN '49007' -- Carbon
            WHEN COUNTYNAME.id = 6  THEN '49009' -- Daggett
            WHEN COUNTYNAME.id = 7  THEN '49011' -- Davis
            WHEN COUNTYNAME.id = 8  THEN '49013' -- Duchesne
            WHEN COUNTYNAME.id = 9  THEN '49015' -- Emery
            WHEN COUNTYNAME.id = 10 THEN '49017' -- Garfield
            WHEN COUNTYNAME.id = 11 THEN '49019' -- Grand
            WHEN COUNTYNAME.id = 12 THEN '49021' -- Iron
            WHEN COUNTYNAME.id = 13 THEN '49023' -- Juab
            WHEN COUNTYNAME.id = 14 THEN '49025' -- Kane
            WHEN COUNTYNAME.id = 15 THEN '49027' -- Millard
            WHEN COUNTYNAME.id = 16 THEN '49029' -- Morgan
            WHEN COUNTYNAME.id = 17 THEN '49031' -- Piute
            WHEN COUNTYNAME.id = 18 THEN '49033' -- Rich
            WHEN COUNTYNAME.id = 1  THEN '49035' -- Salt Lake
            WHEN COUNTYNAME.id = 19 THEN '49037' -- San Juan
            WHEN COUNTYNAME.id = 20 THEN '49039' -- Sanpete
            WHEN COUNTYNAME.id = 21 THEN '49041' -- Sevier
            WHEN COUNTYNAME.id = 22 THEN '49043' -- Summit
            WHEN COUNTYNAME.id = 23 THEN '49045' -- Tooele
            WHEN COUNTYNAME.id = 24 THEN '49047' -- Uintah
            WHEN COUNTYNAME.id = 25 THEN '49049' -- Utah
            WHEN COUNTYNAME.id = 26 THEN '49051' -- Wasatch
            WHEN COUNTYNAME.id = 27 THEN '49053' -- Washington
            WHEN COUNTYNAME.id = 28 THEN '49055' -- Wayne
            WHEN COUNTYNAME.id = 29 THEN '49057' -- Weber
        END,
    'n/a' as District,  -- Utah doesn't have state fire districts
    INCIDENT.[pointOfOrigin].Lat as Latitude,
    INCIDENT.[pointOfOrigin].Long as Longitude,
    "StatisticalCauseCode" = 
        CASE
            WHEN LKUP.id IN (415, 434, 444, 411)
                THEN '1' -- Lightning
            WHEN LKUP.id IN (435, 445, 446, 449, 450, 447, 448, 451, 452)
                THEN '2' -- Equipment Use
            WHEN LKUP.id IN (436, 453)
                THEN '3' -- Smoking
            WHEN LKUP.id IN (437, 421, 454, 455)
                THEN '4' -- Campfire
            WHEN LKUP.id IN (438, 456, 457, 458, 459, 460, 461, 462, 463, 420, 499)
                THEN '5' -- Debris Burning
            WHEN LKUP.id IN (439, 464, 465, 466)
                THEN '6' -- Railroad
            WHEN LKUP.id IN (413, 419, 467, 440)
                THEN '7' -- Arson
            WHEN LKUP.id IN (441, 468, 469)
                THEN '8' -- Children
            WHEN LKUP.id IN (412, 414, 416, 417, 418, 442, 470, 471, 472, 473, 475, 476, 477, 479, 480, 481, 483, 500)
                THEN '9' -- Miscellaneous
            WHEN LKUP.id IN (422, 474)
                THEN '10' -- Fireworks
            WHEN LKUP.id IN (478)
                THEN '11' -- Power line
            WHEN LKUP.id IN (482)
                THEN '12' -- Structure
        END,
    "OwnershipCode" = 
        CASE
            WHEN LKUPagency.id IN (6, 36, 37, 38, 39, 88, 89, 94, 370, 371, 372, 373)
                THEN 'F' -- Federal
            WHEN LKUPagency.id IN (35, 95, 35, 367, 368, 369)
                THEN 'S' -- State
            WHEN LKUPagency.id IN (7, 96)
                THEN 'C' -- County
            WHEN LKUPagency.id IN (93, 97)
                THEN 'M' -- City/Municipality
            WHEN LKUPagency.id IN (82, 90)
                THEN 'P' -- Private
            WHEN LKUPagency.id IN (98, 40, 92, 374)
                THEN 'O' -- Other
        END,
    DETAILS.homesThreatened as  ResidencesThreatened,
    DETAILS.homesDestroyed as ResidencesDestroyed,
    DETAILS.structuresThreatened as OtherStructuresThreatened,
    DETAILS.structuresDestroyed as OtherStructuresDestroyed,
    ADMIN.numFireFighterInjuries as NumberInjuries,
    ADMIN.numFireFighterFatalities as NumberFatalities,
    ROUND(INCIDENT.TotalAcres, 3) as FinalFireAcreQuantity,
    'N' as DeleteFlag

FROM fire.dbo.Incident INCIDENT

    LEFT OUTER JOIN fire.dbo.IncidentDetails DETAILS 
    ON INCIDENT.id = DETAILS.incidentId

    LEFT OUTER JOIN fire.dbo.IncidentLocation LOCATION 
    ON INCIDENT.id = LOCATION.id

    LEFT OUTER JOIN fire.dbo.County COUNTYNAME
    ON LOCATION.countyId = COUNTYNAME.id

    LEFT OUTER JOIN fire.dbo.Lookup LKUP
    ON DETAILS.generalCauseId = LKUP.id

    LEFT OUTER JOIN fire.dbo.Lookup LKUPgeneralcause
    ON DETAILS.generalCauseId = LKUPgeneralcause.id

    LEFT OUTER JOIN fire.dbo.Lookup LKUPagency
    ON INCIDENT.jurisdictionalAgencyId = LKUPagency.id

    LEFT OUTER JOIN fire.dbo.IncidentAdministration ADMIN 
    ON INCIDENT.id = ADMIN.incidentId

WHERE INCIDENT.startDate >= CONVERT(datetime, '2018-01-01') AND INCIDENT.startDate <= CONVERT(datetime, '2018-12-31')
    AND INCIDENT.number LIKE 'UT%'  -- filter out only the Utah fires
    AND ((LKUPgeneralcause.id IS NULL) OR LKUPgeneralcause.id NOT IN (429,443,484)) -- filter out any fire with a general cause of "false alarm". Do not remove NULL values
    AND NOT INCIDENT.name LIKE 'FA %'  -- filter out any fire names starting with FA or FA0 (false alarms)
    AND NOT INCIDENT.name LIKE 'FA0%'
    AND NOT LOWER(INCIDENT.name) LIKE '%false%'
    AND INCIDENT.TotalAcres > 0

ORDER BY INCIDENT.startDate
