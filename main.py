from utils import *
from models import *
from db import Neo4J
from file_processor import *
import ray

@ray.remote
def process_file(file, rel_ext_shared):
    print(f"Processing \"{file}\"...")
    
    file_id = file.split("/")[-1].split(".")[0]
    doc = json.load(open(file))

    for input_text in doc["text"].split("\n\n"):
        print(f"Working on file input line \"{input_text[:100]}\"...")
        
        # print("Getting coreference data...")
        # coref_text = ray.get(coref_shared)(input_text)._.resolved_text

        # print(f"Got coref text \"{coref_text}\"")

        try:
            print("Extracting relations...")
            doc = ray.get(rel_ext_shared)(input_text)
        
            params = [rel_dict for value, rel_dict in doc._.rel.items()]
            for p in params:
                p['file_id']=file_id

            return {'data': params}
        except:
            print("Failed!!")


def main(process: bool):
    print("Starting main program...")

    print("Starting Ray...")
    ray.init()

    print("Connecting to Neo4J...")
    db = Neo4J()
    if db.connect() is None:
        print("...Failed to connect to Neo4J")
    else:
        print("...Connected.")

    if process:
        print("Loading models...")
        models = ray.get([load_relation_extraction.remote()])

        print("Putting coref in shared mem...")
        rel_ext_shared = ray.put(models[0])

        print("Processing files in parallel")
        ray.get([process_file.remote(file, rel_ext_shared) for file in parse_and_read_files()])

        # job_ids = []
        # print("Dispatching Ray jobs...")
        # for file in parse_and_read_files():
        #     print(f"Creating job for {file}...")
        #     job_ids.append(process_file.options(memory=2 * 1024 * 1024 * 1024).remote(file, models[0], models[1]))

        # print(job_ids)

        #print("Importing extracted relations to Neo4J...")
        # db.run_query(db.get_import_query(), {'data': params})

        # print("Waiting for Ray jobs...")
        # ray.get(job_ids)

    # print("Loading data from Neo4J...")
    # df = db.run_query(db.get_selection_query())
    # print(df.describe())
     
if __name__ == "__main__":
    print("Bootstrapping...")
    main(True)