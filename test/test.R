# only works on the rserver

library(resourcer)
library(resourcex)
res <- resourcer::newResource(name='db', url='postgresql://localhost:5433/omop_test', identity='opal', secret='opalpass', format=NULL)
b <- SQLFlexResolver$new()
a <- b$newClient(res)
a$getConnection()

a$readQuery('SELECT     cs.place_of_service_concept_id,     COUNT(*) AS places_of_service_count FROM synthea_omop.care_site AS cs GROUP BY cs.place_of_service_concept_id ORDER BY cs.place_of_service_concept_id')

loadQuery(a, 'SELECT COUNT(person_id) AS num_persons
FROM synthea_omop.person
WHERE gender_concept_id = $1', 8532)
library(dsQueryLibraryServer)
loadAllQueries()
execQuery('care_site', 'CS01..Care.site.place.of.service.counts', NULL, resource = 'a')
