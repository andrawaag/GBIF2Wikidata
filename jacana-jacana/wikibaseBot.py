from wikidataintegrator import wdi_core, wdi_login
from wikidataintegrator.wdi_helpers import try_write
import pandas as pd
import os
import pprint

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
  WDUSER = os.environ['WDUSER']
  WDPASS = os.environ['WDPASS']
else:
  raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")



wikibase = "https://jacana-jacana.semscape.org/"
api = wikibase+"w/api.php"
sparql = wikibase+"query/sparql"
entityUri = wikibase.replace("https:", "http:")+"entity/"
print("variables done")


query = """
PREFIX wdt: <http://jacana-jacana.semscape.org/prop/direct/>
SELECT * WHERE {
   ?jacana_observation wdt:P2 ?gbifID
}
"""

existing_observsations = dict()
results =  wdi_core.WDItemEngine.execute_sparql_query(query=query, endpoint=sparql)

for result in results["results"]["bindings"]:
    existing_observsations[result["gbifID"]["value"]] = result["jacana_observation"]["value"].replace("http://jacana-jacana.semscape.org/entity/", "")


login = wdi_login.WDLogin(WDUSER, WDPASS, mediawiki_api_url=api)

occurrences = "data/0047406-200221144449610-1/occurrence.txt"
multimedia = "data/0047406-200221144449610-1/multimedia.txt"
mainDataframe = pd.read_csv(occurrences, sep='\t')
multiDataframe = pd.read_csv(multimedia, sep='\t')
print("loading done")


WDselection = mainDataframe[["gbifID", "license", "taxonKey", "kingdomKey", "phylumKey", "classKey", "orderKey", "familyKey", "genusKey",
                        "subgenusKey", "speciesKey", "decimalLatitude", "decimalLongitude", "coordinatePrecision"]]
WDselection.to_csv("temptable.txt")

multimediaSelection = multiDataframe[["gbifID", "type", "format", "identifier", "creator", "publisher"]]
multimediaSelection.to_csv("multi.txt")

for index, row in WDselection.iterrows():
    item_data = []
    # gbifID
    print(row["gbifID"])
    if row["gbifID"] in existing_observsations.keys():
        continue
    item_data.append(wdi_core.WDExternalID(str(row["gbifID"]), prop_nr="P2"))

    #license
    licenseQID = wdi_core.WDItemEngine.get_wd_search_results(search_string=row["license"], mediawiki_api_url=api)
    if len(licenseQID) == 0:
        license_item = wdi_core.WDItemEngine(new_item=True, mediawiki_api_url=api, sparql_endpoint_url=sparql)
        license_item.set_label(row["license"], lang="en")
        license_item.set_description("license", lang="en")
        try_write(license_item, record_id=row["license"], record_prop="", edit_summary="Add a license", login=login)
        licenseQID.append(license_item.wd_item_id)
    item_data.append(wdi_core.WDItemID(licenseQID[0], prop_nr="P12"))
    if isinstance(row["taxonKey"], int):
        item_data.append(wdi_core.WDExternalID(str(row["taxonKey"]), prop_nr="P32"))
    if isinstance(row["kingdomKey"], int):
        item_data.append(wdi_core.WDExternalID(str(row["kingdomKey"]), prop_nr="P33"))
    if isinstance(row["phylumKey"], int):
        item_data.append(wdi_core.WDExternalID(str(row["phylumKey"]), prop_nr="P34"))
    if isinstance(row["classKey"], int):
        item_data.append(wdi_core.WDExternalID(str(row["classKey"]), prop_nr="P35"))
    if isinstance(row["orderKey"], int):
        item_data.append(wdi_core.WDExternalID(str(row["orderKey"]), prop_nr="P36"))
    if isinstance(row["familyKey"], int):
        item_data.append(wdi_core.WDExternalID(str(row["familyKey"]), prop_nr="P39"))
    if isinstance(row["genusKey"], int):
        item_data.append(wdi_core.WDExternalID(str(row["genusKey"]), prop_nr="P40"))
    if isinstance(row["subgenusKey"], int):
        item_data.append(wdi_core.WDExternalID(str(row["subgenusKey"]), prop_nr="P41"))
    if isinstance(row["speciesKey"], int):
        item_data.append(wdi_core.WDExternalID(str(row["speciesKey"]), prop_nr="P42"))
    if row["decimalLatitude"] != "":
        item_data.append(wdi_core.WDGlobeCoordinate(row["decimalLatitude"], row["decimalLongitude"], precision=0.016666666666667,
                                   prop_nr="P45"))

    for index2, row2 in multimediaSelection[multimediaSelection["gbifID"]==row["gbifID"]].iterrows():
        creatorQID = wdi_core.WDItemEngine.get_wd_search_results(search_string=row2["creator"], mediawiki_api_url=api)
        if len(creatorQID) == 0:
            creator_item = wdi_core.WDItemEngine(new_item=True, mediawiki_api_url=api, sparql_endpoint_url=sparql)
            creator_item.set_label(row2["creator"], lang="en")
            try_write(creator_item, record_id=row2["creator"], record_prop="", edit_summary="Add a creator", login=login)
            creatorQID.append(creator_item.wd_item_id)
        creator = wdi_core.WDItemID(creatorQID[0], prop_nr="P21", is_qualifier=True)

        publisherQID = wdi_core.WDItemEngine.get_wd_search_results(search_string=row2["publisher"], mediawiki_api_url=api)
        if len(publisherQID) == 0:
            publisher_item = wdi_core.WDItemEngine(new_item=True, mediawiki_api_url=api, sparql_endpoint_url=sparql)
            publisher_item.set_label(row2["publisher"], lang="en")
            try_write(publisher_item, record_id=row2["publisher"], record_prop="", edit_summary="Add a publisher", login=login)
            publisherQID.append(publisher_item.wd_item_id)
        publisher = wdi_core.WDItemID(publisherQID[0], prop_nr="P46", is_qualifier=True)
        qualifiers = [creator, publisher]
        item_data.append(wdi_core.WDUrl(row2["identifier"], prop_nr="P47", qualifiers=qualifiers))

    item = wdi_core.WDItemEngine(data=item_data, mediawiki_api_url=api, sparql_endpoint_url=sparql)
    try_write(item, record_id=row["gbifID"], record_prop="P2", edit_summary="Add an observation", login=login)
    print("observation: "+item.wd_item_id)

