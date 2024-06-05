import requests
import csv
import urllib.parse
from config import Config

download_url = "https://mapcore-demo.org/devel/flatmap/v4/annotator/download/"
#Either a list of id or None
annotationId = None
process_number = 20
headers = {"Authorization": f"Bearer {Config.ANNOTATION_SECRET}"}
r = requests.get(download_url, headers=headers)
rawData = r.json()
taxonMapping = {}
batch_name = "test_1"
exportFile = "test.csv"

def getTaxon(entry):
  resource = get_keys_value(entry, "resource")
  if not resource in taxonMapping:
    try:
      r = requests.get(urllib.parse.unquote(resource))
      data = r.json()
      if "taxon" in data:
        taxonMapping[resource] = data["taxon"]
        return taxonMapping[resource]
    except:
      return None
  if resource in taxonMapping:
    return taxonMapping[resource]
  return None

def findAnnotationIdForFeatureId(featureId):
  ids = []
  targetID = featureId.replace('Feature ', '')
  for entry in rawData:
    itemID = get_keys_value(entry, 'item', 'id')
    if itemID and str(itemID) == targetID:
      if entry['annotationId']:
        ids.append(download_url + str(entry['annotationId']))
  return ids

def keysExists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if not isinstance(element, dict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True

def get_keys_value(element, *keys):
    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return None
    return _element

def getCurationURLs(entry):
  return get_keys_value(entry, "body", "evidence")

def getCurationIDs(entry):
  curationIds = getCurationURLs(entry)
  if curationIds:
    ids = ';'.join(curationIds)
    ids = ids.replace('https://doi.org/', 'DOI:')
    ids = ids.replace('https://pubmed.ncbi.nlm.nih.gov/', '')
    return ids
  return None

def parseIDs(entry, matchString):
  curationIds = getCurationURLs(entry)
  if curationIds:
    for _id in curationIds:
      if _id.find(matchString) > -1:
        return _id.replace(matchString, '')
  return None


def getDOIs(entry):
  return parseIDs(entry, 'https://doi.org/')

def getPMIDs(entry):
  pmid = parseIDs(entry, 'https://pubmed.ncbi.nlm.nih.gov/')
  if pmid and pmid.isdigit():
    return pmid
  return None  


  
def getOrcidId(entry):
  return get_keys_value(entry, "creator", "orcid")

def getComment(entry):
  return get_keys_value(entry, "body", "comment")

def getItemModels(entry):
  return get_keys_value(entry, "item", "models")

def getAnnotationId(entry):
  return get_keys_value(entry, "annotationId")

def getModelsFromBody(entry, field):
  return get_keys_value(entry, 'body', field, 'models')

def getModelsForNewFeature(entry, field):
  models = getModelsFromBody(entry, field)
  if models == None:
    featureID = get_keys_value(entry, 'body', field, 'label')
    annotationIds = findAnnotationIdForFeatureId(featureID)
    models = ";".join(annotationIds)
  return models

def processNewConnections(entry, processed):
  #Check if it is a newly created line
  if get_keys_value(entry, 'body', 'type') == "connectivity" and\
    keysExists(entry, 'body', 'source', 'label') and\
    keysExists(entry, 'body', 'target', 'label'):
    sourceModels = getModelsForNewFeature(entry, 'source')
    if sourceModels:
      processed['structure_1'] = sourceModels
      targetModels = getModelsForNewFeature(entry, 'target')
      sentence = "This new connection starts from: " + sourceModels
      addToSentence(processed, sentence)
      if targetModels:
        processed['structure_2'] = targetModels
        sentence = "This new connection end at: " + targetModels
        addToSentence(processed, sentence)

def addToSentence(processed, sentence):
  new_sentence = sentence
  if not new_sentence.endswith("."):
    new_sentence += "."
  current = get_keys_value(processed, "sentence")
  if current:
    processed["sentence"] = current + " " + new_sentence
  else:
    processed["sentence"] = new_sentence

def processNewStructure(entry, processed):
  processNewConnections(entry, processed)

def processEntry(entry, entry_id):
  processed = {}

  processed['id'] = entry_id

  pmids = getPMIDs(entry)
  if pmids:
    processed['pmid'] = pmids

  dois = getDOIs(entry)
  if dois:
    processed['doi'] = dois

  comment = getComment(entry)
  if comment:
    addToSentence(processed, comment)

  processed['batch_name'] = batch_name

  processed['sentence_id'] = entry_id

  processed['out_of_scope'] = "no"

  orcid = getOrcidId(entry)
  if orcid:
    processed['orcid'] = orcid
    addToSentence(processed, "This is annotated using orcid id:" + orcid)

  annotationId = getAnnotationId(entry)
  if annotationId:
    url = download_url + str(annotationId)
    processed['annotation_id'] = url
    addToSentence(processed, "This annotation can be viewed in " + url)

  urls = getCurationURLs(entry)
  if urls:
    processed['url'] = ";".join(urls)

  models = getItemModels(entry)
  if models:
    processed['structure_1'] = models
    addToSentence(processed, "This is annotated on " + models)
  else:
    processNewStructure(entry, processed)
    addToSentence(processed, "This is an user drawn feature.")

  taxon = getTaxon(entry)
  if taxon:
    processed['taxon'] = taxon
    addToSentence(processed, "This annotation was created on " + taxon)

  return processed

def isValidData(processed):
  if keysExists(processed, "sentence"):
    return True
  return False

def processEntries(rawData):
  processedEntries = []
  entry_id = 1
  for entry in rawData:
      processed = processEntry(entry, entry_id)
      if isValidData(processed):
        processedEntries.append(processed)
        entry_id = entry_id + 1
  return processedEntries

def getRow(entry, csvColumns):
  row = []
  for column in csvColumns:
    value = get_keys_value(entry, column)
    if value:
      row.append(str(value))
    else:
      row.append("")
  return row

def writeToCSV(processedEntries):
  csvColumns = ['id', 'pmid', 'pmcid', 'doi', 'sentence', 'batch_name', 'sentence_id', 'out_of_scope', 'structure_1', 'structure_2', 'url', 'annotation_id', 'orcid']
  with open(exportFile, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(csvColumns)
    for entry in processedEntries:
      row = getRow(entry, csvColumns)
      writer.writerow(row)

def run():
  processedEntries = processEntries(rawData)
  writeToCSV(processedEntries)

run()
