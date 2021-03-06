﻿
SELECT
      INCIDENT.[number],
      INCIDENT.[name],
	  UPPER(LKUPagency.[keyName]) as agency,
	  INCIDENT.[id] as id,
      INCIDENT.[startDate],
	  ISNULL(INCIDENT.[TotalAcres], 0) as totalAcres,
	  INCIDENT.[countyFire],
	  INCIDENT.[fireCode],
	  LKUPgeneralcause.[name] as generalCause,
	  LKUPspecificcause.[name] as specificCause,
	  LKUPperson.[name] as person,
	  LKUPactivity.[name] as activity,
	  LKUPfiretype.[name] as fireType,
	  DETAILS.homesDamaged,
	  DETAILS.homesDestroyed,
	  DETAILS.homesThreatened,
	  DETAILS.structuresDamaged,
	  DETAILS.structuresDestroyed,
	  DETAILS.structuresThreatened,
	  DETAILS.vehiclesDamaged,
	  DETAILS.vehiclesDestroyed,
	  DETAILS.vehiclesThreatened,
	  INCIDENT.[pointOfOrigin].Long as LON,
	  INCIDENT.[pointOfOrigin].Lat as LAT,
	  INCIDENT.[createdDate],
	  INCIDENT.[modifiedDate]
  FROM fire.dbo.Incident INCIDENT

  LEFT OUTER JOIN fire.dbo.IncidentDetails DETAILS 
	ON INCIDENT.id = DETAILS.incidentId

  LEFT OUTER JOIN fire.dbo.Lookup LKUP
	ON DETAILS.generalCauseId = LKUP.id

  LEFT OUTER JOIN fire.dbo.Lookup LKUPgeneralcause
	ON DETAILS.generalCauseId = LKUPgeneralcause.id

  LEFT OUTER JOIN fire.dbo.Lookup LKUPspecificcause
	ON DETAILS.specificCauseId = LKUPspecificcause.id

  LEFT OUTER JOIN fire.dbo.Lookup LKUPperson
	ON DETAILS.personId = LKUPperson.id	

  LEFT OUTER JOIN fire.dbo.Lookup LKUPactivity
	ON DETAILS.activityId = LKUPactivity.id	

  LEFT OUTER JOIN fire.dbo.Lookup LKUPfiretype
	ON DETAILS.fireTypeId = LKUPfiretype.id

  LEFT OUTER JOIN fire.dbo.Lookup LKUPagency
	ON INCIDENT.jurisdictionalAgencyId = LKUPagency.id

  WHERE INCIDENT.[startDate] > CONVERT(datetime, '2018-01-01')
    AND INCIDENT.[number] LIKE 'UT%'  /* filter out only the Utah fires */
	AND ((LKUPgeneralcause.id IS NULL) OR LKUPgeneralcause.id NOT IN (429,443,484))  /* filter out any fire with a general cause of "false alarm" */
	AND NOT INCIDENT.[name] LIKE 'FA %'  /* filter out any fire names starting with FA or FA0 (false alarms) */
	AND NOT INCIDENT.[name] LIKE 'FA0%'
	AND NOT LOWER(INCIDENT.[name]) LIKE '%false%' 

  ORDER BY INCIDENT.startDate
