import spacy
import ray
from rebel_component import RebelComponent # not even necessary?

DEVICE = -1 #use CPU

@ray.remote
def load_coreference():
    print("Loading coreference model...")
    coref = spacy.load('en_core_web_lg', disable=['ner', 'tagger', 'parser', 'attribute_ruler', 'lemmatizer'])
    print("...Adding pipe")
    coref.add_pipe("xx_coref", config={"chunk_size": 2500, "chunk_overlap": 2, "device": DEVICE})
    print("...Done")
    return coref

@ray.remote
def load_relation_extraction():
    RebelComponent
    print("Loading relation extraction model...")
    rel_ext = spacy.load('en_core_web_sm', disable=['ner', 'lemmatizer', 'attribute_rules', 'tagger'])
    rel_ext.add_pipe("rebel", config={'device':DEVICE, 'model_name':'Babelscape/rebel-large'})
    return rel_ext