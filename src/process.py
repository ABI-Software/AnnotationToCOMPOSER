import requests
import csv
import urllib.parse
from config import Config

download_url = Config.FLATMAP_URL + 'annotator/download/'
flatmap_server_url = Config.FLATMAP_URL
#Either a list of id or None
annotationId = None
process_number = 20
headers = {"Authorization": f"Bearer {Config.ANNOTATION_SECRET}"}
r = requests.get(download_url, headers=headers)
rawData = r.json()
taxonMapping = {}
flatmapServerData = None
sckanMapping = {}
batch_name = 'dev'
exportFile = '6-1-25-dev.csv'

def get_keys_value(element, *keys):
    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return None
    return _element

def get_keys_value_from_list(list, uuid, *keys):
    for item in list:
      if 'uuid' in item and item['uuid'] == uuid:
        return get_keys_value(item, *keys)
    return None


def getDescribes(uuid):
  global flatmapServerData
  if flatmapServerData:
    return get_keys_value_from_list(flatmapServerData, uuid, 'describes')

def getTaxon(uuid):
  global flatmapServerData
  if flatmapServerData:
    return get_keys_value_from_list(flatmapServerData, uuid, 'taxon')

def getName(uuid):
  global flatmapServerData
  if flatmapServerData:
    return get_keys_value_from_list(flatmapServerData, uuid, 'name')

def getSckan(uuid):
  global flatmapServerData
  if flatmapServerData:
    return get_keys_value_from_list(flatmapServerData, uuid, 'sckan', 'npo', 'path')

def getResourceInformation(entry):
  resource = get_keys_value(entry, 'resource')
  map_type = None
  describes = None
  name = None
  taxon = None
  sckan= None
  if resource:
    uuid = None
    try:
      uuid = resource[resource.rindex('/')+1:]
    except:
      #Handle old case where only the map id is provided
      uuid = resource
      resource = flatmap_server_url + '/' + uuid

    if flatmap_server_url in resource:
      map_type = 'Flatmap'
      global flatmapServerData
      if not flatmapServerData:
        try:
          r = requests.get(urllib.parse.unquote(flatmap_server_url))
          flatmapServerData = r.json()
        except:
          return None
      describes = getDescribes(uuid)
      taxon = getTaxon(uuid)
      name = getName(uuid)
      sckan = getSckan(uuid)
    else:
      map_type = 'Scaffold'

  return {
    'map_type': map_type,
    'describes': describes,
    'taxon': taxon,
    'name': name,
    'sckan': sckan,
  }


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

def getCurationURLs(entry):
  processedIds = []
  curationIds = get_keys_value(entry, "body", "evidence")
  if curationIds:
    for newId in curationIds:
      if isinstance(newId, str):
        processedIds.append(newId)
      elif isinstance(newId, dict):
        key = next(iter(newId))
        if key:
          processedIds.append(newId[key])
  return processedIds

def getCurationIDs(entry):
  curationIds = getCurationURLs(entry)
  ids = ';'.join(curationIds)
  ids = ids.replace('https://doi.org/', 'DOI:')
  ids = ids.replace('https://pubmed.ncbi.nlm.nih.gov/', 'PMIDS')
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

def getStatus(entry):
  return get_keys_value(entry, 'status')

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
      sentence = "The origin of this new connection is: " + sourceModels
      addToSentence(processed, sentence)
      #### Add Via

      if targetModels:
        processed['structure_2'] = targetModels
        sentence = "This destination of this new connection is: " + targetModels
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

  status = getStatus(entry)
  if status:
    processed['status'] = status

  annotationId = getAnnotationId(entry)
  if annotationId:
    url = download_url + str(annotationId)
    processed['id'] = url
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

  map_resource = getResourceInformation(entry)
  if map_resource:
    if map_resource["map_type"]:
      processed['map_type'] = map_resource['map_type']
      addToSentence(processed, "This annotation was created on a " + processed['map_type'] + ".")
    if map_resource['taxon']:
      processed['taxon'] = map_resource['taxon']
      addToSentence(processed, "This annotation was created on " +  processed['taxon'] + ".")
    if map_resource["describes"]:
      processed['describes'] = map_resource['describes']
    if map_resource["name"]:
      processed['map_name'] = map_resource['name']
    if map_resource["sckan"]:
      processed['sckan'] = map_resource['sckan']
      addToSentence(processed, "This map contains sckan knowledge from " +  processed['sckan'] + ".")
  #taxon = getTaxon(entry)
  #if taxon:
  #  processed['taxon'] = taxon
  #  addToSentence(processed, "This annotation was created on " + taxon)

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
  csvColumns = ['id', 'status',  'pmid', 'pmcid', 'doi', 'sentence', 'batch_name', 'sentence_id', 'out_of_scope', 'structure_1', 'structure_2', 'url', 'orcid', 'map_type', 'taxon', 'sckan']
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
