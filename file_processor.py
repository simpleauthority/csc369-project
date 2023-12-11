import glob
import json

def parse_and_read_files():
    DIR="groups"
    
    for file in glob.glob(DIR + "/*.txt"):
        record = {}
        record['title'] = file.split("/")[-1]
        content = open(file).read()
        record['text'] = content
        open(file+".json","w").write(json.dumps(record))


    print("Globbing group JSON files...")
    return glob.glob(DIR + '/*.json')