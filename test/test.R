# only works on the rserver

library(resourcer)
library(resourcex)
res <- resourcer::newResource(name='db', url='postgresql://localhost:5433/omop_test', identity='opal', secret='opalpass', format=NULL)
b <- SQLFlexResolver$new()
a <- b$newClient(res)
a$getConnection()

a$readQuery('SELECT COUNT(person_id) AS num_persons
FROM synthea_omop.person
WHERE gender_concept_id = $1', 8532)

loadQuery(a, 'SELECT COUNT(person_id) AS num_persons
FROM synthea_omop.person
WHERE gender_concept_id = $1', 8532)
