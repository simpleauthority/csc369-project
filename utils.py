import requests


def extract_triplets(text):
    triplets = []
    relation, subject, relation, object_ = '', '', '', ''
    text = text.strip()
    current = 'x'
    for token in text.replace("<s>", "").replace("<pad>", "").replace("</s>", "").split():
        if token == "<triplet>":
            current = 't'
            if relation != '':
                triplets.append({'head': subject.strip(), 'type': relation.strip(),'tail': object_.strip()})
                relation = ''
            subject = ''
        elif token == "<subj>":
            current = 's'
            if relation != '':
                triplets.append({'head': subject.strip(), 'type': relation.strip(),'tail': object_.strip()})
            object_ = ''
        elif token == "<obj>":
            current = 'o'
            relation = ''
        else:
            if current == 't':
                subject += ' ' + token
            elif current == 's':
                object_ += ' ' + token
            elif current == 'o':
                relation += ' ' + token
    if subject != '' and relation != '' and object_ != '':
        triplets.append({'head': subject.strip(), 'type': relation.strip(),'tail': object_.strip()})
    return triplets

# Add rebel component https://github.com/Babelscape/rebel/blob/main/spacy_component.py


def call_wiki_api(item):
  try:
    url = f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={item}&language=en&format=json"
    data = requests.get(url).json()
    # Return the first id (Could upgrade this in the future)
    return data['search'][0]['id']
  except:
    return 'id-less'

        
# sent = '<s><triplet> Jennifer Anne Doudna <subj> February 19, 1964 <obj> date of birth <subj> Nobel Prize in Chemistry <obj> award received <triplet> Emmanuelle Charpentier <subj> Nobel Prize in Chemistry <obj> award received</s>'
# print(extract_triplets(sent))