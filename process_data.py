import overpy
import pprint
import re
import pymongo
import sys
from collections import defaultdict
import time

# Global Variables
highway_types = ['primary', 'residential', 'secondary']
street_types_map = {'Avenue': 'Ave', 'Boulevard': 'Blv', 'Court': 'Ct',
                         'Drive': 'Dr', 'Place': 'Pl', 'Road': 'Rd',
                         'Square': 'Sq', 'Street': 'St', 'Broadway': 'Bdwy',
                         'Plaza': 'Plz', 'Transverse': 'Tvs', 'Tunnel': 'Tnl',
                         'Bridge': 'Brdg', 'Exit': 'Exit'}
cfcc_types_re = re.compile(r'([A-C])(\d{1,2})')

# Check if the input string is a number
def is_number(input_string):
    try:
        int(input_String)
        return True
    except:
        return False

# Check if given element is a street
def is_street(element):
    highway = element.tags.get("highway")
    if highway in highway_types:
        cfcc = element.tags.get("tiger:cfcc")
        if cfcc:
            cfcc_base, cfcc_type = process_cfcc(cfcc)
            if cfcc_base =='A' and 21 <= cfcc_type <= 49:
                return True
            else:
                return False
        else:
            return True
    else:
        return False


# Given street name, return a tuple containing name_base and name_type
def process_name(name):
    if name:
        name_parts = name.rsplit(" ", 1)
        if len(name_parts) > 1:
            if is_number(name_parts[1]): # Street type in front: e.g. Road 10
                return (name_parts[1], name_parts[0])
            else: # Street type in the back: e.g. 5th Avenue
                return (name_parts[0], name_parts[1])
        else: # Sometimes the street name is only 1 word. E.g. Broadway
            return (name_parts[0], name_parts[0])
    else:
        return (None, None)

# Given TIGER CFCC code, return the a tuple containing the first letter and
# ensuing numbers
def process_cfcc(cfcc):
    if cfcc:
        m = cfcc_types_re.findall(cfcc)
        return (m[0][0], int(m[0][1]))
    else:
        return (None, None)

# Audit ways returned from OpenStreetMap to check for missing street attributes.
# If the missing attributes can be corrected, this function returns a dictionary
# containing way ID and corresponding dictionary of attributes to be overwritten
# Otherwise the function print out remaining issues to be resolved.
def audit_ways(ways):

    print("Auditing open street map data.")
    audit_issues = {'Street with no name': [],
                    'Unable to infer street type': [],
                    'Unknown street type': []}

    overwrites = {}

    for way in ways:
        #overwrite streets with no name
        if is_street(way):
            street_name = way.tags.get("name")
            street_name_alt = way.tags.get("name_1")
            street_type = way.tags.get("tiger:name_type")
            street_base = way.tags.get("tiger:name_base")
            if not street_name and not street_name_alt:
                issue = audit_issues['Street with no name']
                issue.append("%s: name=%s, name_1=%s" %
                            (way.id, street_name, street_name_alt))
            elif not street_type:
                if street_name:
                    try:
                        street_base, street_type = process_name(street_name)
                    except Exception as e:
                        if street_name_alt:
                            street_base, street_type = process_name(street_name)
                        else:
                            issue = audit_issues['Unable to infer street type']
                            issue.append("%s: %s" % (way.id, street_name))
                elif street_name_alt:
                    try:
                        street_base, street_type = process_name(street_name_alt)
                    except Exception as e:
                        issue = audit_issues['Unable to infer street type']
                        issue.append("%s: %s" % (way.id, street_name))

                if street_type:
                    if street_type in street_types_map:
                        street_type = street_types_map[street_type]
                        overwrites[way.id] = {'tiger:name_base': street_base,
                                              'tiger:name_type': street_type}
                    else:
                        issue = audit_issues['Unknown street type']
                        issue.append("%s: %s" % (way.id, street_name))


    print("Check issues encounted during audit process:")
    pprint.pprint(audit_issues)

    return overwrites



# Shape way data into a python dictionary for loading into MongoDB. Below is an
# example of dictionary returned by shape way:
# {
#   'id': 5699874,
#   'type': 'way',
#   'change_info': {'changeset': 3971835,
#                  'timestamp': datetime.datetime(2010, 2, 25, 14, 4, 26),
#                  'uid': 165869,
#                  'user': 'chdr',
#                  'version': 4},
#   'highway': 'residential',
#   'name': '54th Avenue',
#   'nodes': [42786594, 277046385],
#   'tiger': {'tiger:cfcc': 'A41',
#            'tiger:county': 'Queens, NY',
#            'tiger:name_base': '54th',
#            'tiger:name_type': 'Ave',
#            'tiger:reviewed': 'no',
#            'tiger:separated': 'no',
#            'tiger:source': 'tiger_import_dch_v0.6_20070829',
#            'tiger:tlid': '59727415',
#            'tiger:upload_uuid': 'bulk_upload.pl-76c2f01f-97e8-4577-832a-27364377b656'},
#  }

def shape_way(way, overwrites):

    output = {}
    output['id'] = way.id
    output['document_type'] = 'way'

    try:
        nodes = way.get_nodes(resolve_missing=False)
        for node in nodes:
            if 'nodes' not in output:
                output['nodes'] = []
            output['nodes'].append(node.id)
    except overpy.exception.DataIncomplete:
        pass


    if way.tags:
        for tag_k in way.tags:
            tag_v = way.tags.get(tag_k)
            if (way.id in overwrites) and (tag_k in overwrites[way.id]):
                tag_v = overwrites[way.id][tag_k]
            if tag_k.split(':')[0] == 'tiger':
                if 'tiger' not in output:
                    output['tiger']={}
                output['tiger'][tag_k] = tag_v
            else:
                output[tag_k] = tag_v

    if way.attributes:
        for attrib_k in way.attributes:
            attrib_v = way.attributes.get(attrib_k)
            if 'change_info' not in output:
                output['change_info'] = {}
            output['change_info'][attrib_k] = attrib_v

    return output

# Shape node data into a python dictionary for loading into MongoDB. Below is an
# example of dictionary returned by shape node:
# {
#     'id': 1765445839,
#     'type': 'node',
#     'change_info': {'changeset': 14183174,
#                     'timestamp': datetime.datetime(2012, 12, 7, 3, 28, 23),
#                     'uid': 716239,
#                     'user': 'WestsideGuy',
#                     'version': 2},
#     'pos': [40.7456318, -73.9568079],
#     'amenity': 'restaurant',
#     'address': {'city': 'Long Island City',
#                 'country': 'US',
#                 'housenumber': '4705',
#                 'postcode': '11109',
#                 'state': 'NY',
#                 'street': 'Center Boulevard'},
#     'cuisine': 'mexican',
#     'email': 'info@skinnyscantina.com',
#     'name': "Skinny's Cantina",
#     'website': 'http://www.skinnyscantina.com/'
#  }
def shape_node(node):
    output = {}
    output['id'] = node.id
    output['document_type'] = 'node'
    output['pos'] = [float(node.lat), float(node.lon)]

    if node.tags:
        for tag_k in node.tags:
            tag_v = node.tags.get(tag_k)
            if tag_k.split(":")[0] == 'addr':
                if 'address' not in output:
                    output['address'] = {}
                tag_k = tag_k.split(":")[1]
                output['address'][tag_k] = tag_v
            elif tag_k.split(".")[0] == 'cityracks':
                if 'cityracks' not in output:
                    output['cityracks']={}
                tag_k = tag_k.split(".")[1]
                output['cityracks'][tag_k] = tag_v
            else:
                output[tag_k] = tag_v

    if node.attributes:
        for attrib_k in node.attributes:
            attrib_v = node.attributes.get(attrib_k)
            if 'change_info' not in output:
                output['change_info'] = {}
            output['change_info'][attrib_k] = attrib_v

    return output

# Get shape function by overpy object err_type
def shape_item(overpy_obj, overwrites):
    if type(overpy_obj) is overpy.Node: return shape_node(overpy_obj)
    if type(overpy_obj) is overpy.Way: return shape_way(overpy_obj, overwrites)

    raise(Exception(
         "Unrecognized overpy object %s. No function found to process data."
         % type(overpy_obj)))

def clear_db(db_collection):
    db_collection.delete_many({})

# Convert overpy object into python dictionary then load into mongodb
def convert_then_insert_data(data, db_collection, overwrites=None):
    err_data = {}
    out_data = []
    for i, item in enumerate(data):
        try:
            out_data.append(shape_item(item, overwrites))
        except Exception as err:
            err_data[item.id] = str(err)
        if (i % 200) == 0: print(".", end='', flush=True)
    print("")
    results = db_collection.insert_many(out_data)
    print(
        "Uploaded %s documents into nodes collection in MongoDB." %
        (len(results.inserted_ids)))
    if len(err_data) > 0:
        print("Skipped %s nodes due to following errors:" % len(err_data))
        for err_type in set(err_data.values()):
            print(err_type)


if __name__ == "__main__":

    # Fetch OpenStreetMap data for a custom grid via Overpass API
    api = overpy.Overpass()
    print("Downloading map data from OpenStreetMap. Please Wait.")
    if True:
        # Larger Map Area (~60MB). Takes long time to download
        result = api.query("(node(40.71,-73.97,40.78,-73.92);<;);out meta;")
    else:
        # Smaller Map Area (~3MB). Use for testing code execution.
        result = api.query("(node(40.74,-73.96,40.75,-73.95);<;);out meta;")

    print("Retrieved %s Nodes from OpenStreetMap." % len(result.nodes))
    print("Retrieved %s Ways from OpenStreetMap. " % len(result.ways))

    if False: # Print out the first node and way returned by Overpass API

        print("Sample node data:")
        pprint.pprint(result.nodes[0])
        pprint.pprint(result.nodes[0].attributes)
        pprint.pprint(result.nodes[0].tags)

        print("Sample way data:")
        pprint.pprint(result.ways[0])
        pprint.pprint(result.ways[0].attributes)
        pprint.pprint(result.ways[0].tags)

    if True: # Audit data and generate overwrites for data cleaning
        overwrites = audit_ways(result.ways)
        print("Audit process returned %s overwrites to be applied to map data."
                % len(overwrites))

    if True: #Proceed with uploading to Mongo DB?

        # Connect to MongoDB Atlas
        client = pymongo.MongoClient("mongodb+srv://public_access:GoData2017!@analyze-openstreetmap-vhq3i.mongodb.net/map_lic")
        db = client.map
        db_collection = db.lic
        clear_db(db_collection)

        start_time = time.time()

        # Load nodes data into nodes collection in mongo DB
        print("Begin uploading nodes data to MongoDB", end='')
        convert_then_insert_data(result.nodes, db_collection, overwrites)


        # Load ways data into ways collection in mongo DB
        print("Begin uploading ways data to MongoDB", end='')
        convert_then_insert_data(result.ways, db_collection, overwrites)

        print("Finished uploading map data to MongoDB in %s minutes."
            % ((time.time() - start_time)/60))
