import spacy
import crosslingual_coreference
from utils import *
from rebel_component import *
import pandas as pd
import wikipedia
from neo4j import GraphDatabase
import glob
import json

def main():
    print("Starting main program...")

    DEVICE = -1 # Number of the GPU, -1 if want to use CPU

    print("Loading coreference model...")

    # Add coreference resolution model
    coref = spacy.load('en_core_web_lg', disable=['ner', 'tagger', 'parser', 'attribute_ruler', 'lemmatizer'])
    coref.add_pipe(
        "xx_coref", config={"chunk_size": 2500, "chunk_overlap": 2, "device": DEVICE})
    
    print("Loading relation extraction model...")

    # Define rel extraction model
    rel_ext = spacy.load('en_core_web_sm', disable=['ner', 'lemmatizer', 'attribute_rules', 'tagger'])
    rel_ext.add_pipe("rebel", config={
        'device':DEVICE, # Number of the GPU, -1 if want to use CPU
        'model_name':'Babelscape/rebel-large'} # Model used, will default to 'Babelscape/rebel-large' if not given
        )
    

    # input_text = "Christian Drosten works in Germany. He likes to work for Google."

    # coref_text = coref(input_text)._.resolved_text

    # doc = rel_ext(coref_text)

    # for value, rel_dict in doc._.rel.items():
    #     print(f"{value}: {rel_dict}")

    print("Connecting to Neo4J...")

    # Define Neo4j connection
    host = 'bolt://127.0.0.1:7687'
    user = 'neo4j'
    password = '12345678'
    driver = GraphDatabase.driver(host,auth=(user, password))

    print("Generating JSON from group text files...")

    DIR="groups"
    for file in glob.glob(DIR+"/*.txt"):
        record = {}
        record['title'] = file.split("/")[-1]
        content = open(file).read()
        record['text'] = content
        open(file+".json","w").write(json.dumps(record))

    print("Creating Neo4J import query...")

    import_query = """
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

    def run_query(query, params={}):
        print("Running a Neo4J query...")
        with driver.session() as session:
            result = session.run(query, params)
            return pd.DataFrame([r.values() for r in result], columns=result.keys())


    def store_content(file):
        print(f"Processing \"{file}\"...")
        #try:
        file_id = file.split("/")[-1].split(".")[0]
        f = open(file)
        doc = json.load(f)
        for input_text in doc["text"].split("\n\n"):
            print(f"Working on file input line \"{input_text[:100]}\"...")
            print("Getting coreference data...")
            coref_text = coref(input_text)._.resolved_text
            try:
                print("Extracting relations...")
                doc = rel_ext(coref_text)
                params = [rel_dict for value, rel_dict in doc._.rel.items()]
                for p in params:
                    p['file_id']=file_id
                print("Importing extracted relations to Neo4J...")
                run_query(import_query, {'data': params})
            except:
                print("Failed!!")
        #except Exception as e:
        #  print(f"Couldn't parse text for {page} due to {e}")

    print("Globbing group JSON files...")

    files = glob.glob(DIR + '/*.json')

    for file in files:
        print(f"Parsing {file}...")
        store_content(file)

    print("Done all. Loading data from Neo4J...")
    
    run_query("""
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
    """)
     
if __name__ == "__main__":
    print("Bootstrapping...")
    main()