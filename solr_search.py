import pysolr
import json

browser_0 = 'http://ui1b.ornl.gov:8983/solr/browser_0'
ds_info = 'http://ui1b.ornl.gov:8983/solr/datastream_info/'
db_table = 'http://ui1b.ornl.gov:8983/solr/db_table/'

TEST = True

def search_solr(text, row_limit=3, select=0, format='json'):
    if select == 0:
        solr = pysolr.Solr(browser_0, search_handler='select', timeout=10)
    elif select == 1:
        solr = pysolr.Solr(ds_info, search_handler='select', timeout=10)
    elif select == 2:
        solr = pysolr.Solr(db_table, search_handler='select', timeout=10)

    # r = solr.search("*.*", rows=100, wt=json, fl="datastream,primary_measurement,meas_subcategory_name,meas_category_name", indent="on")
    results = solr.search(text, rows=row_limit, fl="datastream,score", wt=format)

    if TEST:
        print("Saw {0} result(s)\nsearch query: {1}.".format(len(results), text))

        print("The datastreams are:")
        for i, result in enumerate(results):
            if i == 0:
                print("{0} keys = {1}".format(result, result.keys()))
            # print("datastream -> {}".format(result['datastream']))
            # print("name ----> {}".format(result["instrument_name_text"][0]))
            # print("site ----> {}".format(result["site_name"]))
            # print("desc ----> {}\n".format(result["primary_meas_type_desc"]))

        for result in results:
            print(json.dumps(result, indent=2))

    return results