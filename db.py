from neo4j import GraphDatabase
import pandas as pd

class Neo4J:
    def __init__(self):
        self.driver = None
    
    def connect(self):
        host = 'bolt://127.0.0.1:7687'
        user = 'neo4j'
        password = '12345678'
        driver = GraphDatabase.driver(host,auth=(user, password))
        try:
            driver.verify_connectivity()
            self.driver = driver
            return True
        except Exception as ex:
            print(f"Failed to connect to Neo4J: {ex}")
            return False


    def run_query(self, query, params={}):
        print("Running a Neo4J query...")

        if self.driver is None:
            print("...Failed. Not connected to DB.")
            return None
        
        with self.driver.session() as session:
            result = session.run(query, params)
            print("...OK.")
            return pd.DataFrame([r.values() for r in result], columns=result.keys())
        
    def get_import_query(self):
        return """
        UNWIND $data AS row
        MERGE (h:Entity {id: CASE WHEN NOT row.head_span.id = 'id-less' THEN row.head_span.id ELSE row.head_span.text END})
        ON CREATE SET h.text = row.head_span.text
        MERGE (t:Entity {id: CASE WHEN NOT row.tail_span.id = 'id-less' THEN row.tail_span.id ELSE row.tail_span.text END})
        ON CREATE SET t.text = row.tail_span.text
        WITH row, h, t
        CALL apoc.merge.relationship(h, toUpper(replace(row.relation,' ', '_')),
        {file_id: row.file_id},
        {},
        t,
        {}
        )
        YIELD rel
        RETURN distinct 'done' AS result;
        """
    
    def get_selection_query(self):
        return """
        CALL apoc.periodic.iterate("
        MATCH (e:Entity)
        WHERE e.id STARTS WITH 'Q'
        RETURN e
        ","
        // Prepare a SparQL query
        WITH 'SELECT * WHERE{ ?item rdfs:label ?name . filter (?item = wd:' + e.id + ') filter (lang(?name) = \\"en\\") ' +
            'OPTIONAL {?item wdt:P31 [rdfs:label ?label] .filter(lang(?label)=\\"en\\")}}' AS sparql, e
        // make a request to Wikidata
        CALL apoc.load.jsonParams(
            'https://query.wikidata.org/sparql?query=' + 
            + apoc.text.urlencode(sparql),
            { Accept: 'application/sparql-results+json'}, null)
        YIELD value
        UNWIND value['results']['bindings'] as row
        SET e.wikipedia_name = row.name.value
        WITH e, row.label.value AS label
        MERGE (c:Class {id:label})
        MERGE (e)-[:INSTANCE_OF]->(c)
        RETURN distinct 'done'", {batchSize:1, retry:1})
        """