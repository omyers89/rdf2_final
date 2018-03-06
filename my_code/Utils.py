from json import JSONEncoder

class GraphObjectEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__

subjectsPerson = {'person': "http://dbpedia.org/ontology/Person"}
                          #'politician': "http://dbpedia.org/ontology/Politician",
                          #'soccer_player': "http://dbpedia.org/ontology/SoccerPlayer"}
                          # 'baseball_players': "http://dbpedia.org/ontology/BaseballPlayer",
                          # 'comedian': "http://dbpedia.org/ontology/Comedian",
                          # "Company": "http://dbpedia.org/ontology/Company",
                          #   "BasketballPlayer": "http://dbpedia.org/ontology/BasketballPlayer",
                          # "EducationalInstitution": "http://dbpedia.org/ontology/EducationalInstitution"}


subjectsPlaces = {#'Place': "http://dbpedia.org/ontology/Place",
                  'NaturalPlace': "http://dbpedia.org/ontology/NaturalPlace",
                  'HistoricPlace': "http://dbpedia.org/ontology/HistoricPlace",
                  'CelestialBody': "http://dbpedia.org/ontology/CelestialBody",
                  'architectural_structure': "http://dbpedia.org/ontology/ArchitecturalStructure"}

subjectsLive = {#'Animal': "http://dbpedia.org/ontology/Animal",
                'Plant': "http://dbpedia.org/ontology/Plant",
                'Insect': "http://dbpedia.org/ontology/Insect",
                'Fish': "http://dbpedia.org/ontology/Fish",
                "BasketballPlayer": "http://dbpedia.org/ontology/BasketballPlayer",
                #'person': "http://dbpedia.org/ontology/Person"}
                #'Mammal': "http://dbpedia.org/ontology/Mammal",
                #'Play': "http://dbpedia.org/ontology/Play"
                }

dictionaries = [subjectsPerson, subjectsPlaces, subjectsLive]



dictionariest = [subjectsPerson]

# {  #"Company": "http://dbpedia.org/ontology/Company",
#                     #'comedian': "http://dbpedia.org/ontology/Comedian",
#                     'Mammal': "http://dbpedia.org/ontology/Mammal",
#                     #'Fish': "http://dbpedia.org/ontology/Fish",
#                     #"EducationalInstitution": "http://dbpedia.org/ontology/EducationalInstitution",
#                     'politician': "http://dbpedia.org/ontology/Politician",
#                     'architectural_structure': "http://dbpedia.org/ontology/ArchitecturalStructure",
#                     #'person': "http://dbpedia.org/ontology/Person",
#                     #'baseball_players': "http://dbpedia.org/ontology/BaseballPlayer",
#                     "BasketballPlayer": "http://dbpedia.org/ontology/BasketballPlayer"},
#                     {'person': "http://dbpedia.org/ontology/Person"}]

dictionariesq = [{'comedian': "http://dbpedia.org/ontology/Comedian",
                  "EducationalInstitution": "http://dbpedia.org/ontology/EducationalInstitution",
                    'Play': "http://dbpedia.org/ontology/Play"}]


