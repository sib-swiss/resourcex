# only works on the rserver

library(resourcer)
library(resourcex)
res <- resourcer::newResource(name='db', url='postgresql://localhost:5433/omop_test', identity='opal', secret='opalpass', format=NULL)
b <- SQLFlexResolver$new()
a <- b$newClient(res)
a$getConnection()

a$readQuery('SELECT     cs.place_of_service_concept_id,     COUNT(*) AS places_of_service_count FROM synthea_omop.care_site AS cs GROUP BY cs.place_of_service_concept_id ORDER BY cs.place_of_service_concept_id')

loadQuery(a, 'SELECT
  cast((ca.condition_era_end_date - ca.condition_era_start_date) as integer) + 1 AS num_condition_duration_days,
  count(*)::integer                                                                 AS condition_duration_freq_count
FROM synthea_omop.condition_era ca
INNER JOIN omop_vocabulary.concept c
ON ca.condition_concept_id = c.concept_id
WHERE c.concept_id = $1
GROUP BY  c.concept_id,
          c.concept_name,
          cast((ca.condition_era_end_date - condition_era_start_date) as integer)
ORDER BY 1;', 200219)
library(dsQueryLibraryServer)
loadAllQueries()
execQuery('care_site', 'CS01..Care.site.place.of.service.counts', NULL, resource = 'a')
