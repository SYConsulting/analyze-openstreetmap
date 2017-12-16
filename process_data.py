import overpy
import pprint
import re
import pymongo
from collections import defaultdict

# Global Variables
highway_types = ['primary', 'residential', 'secondary']
street_types = ['Ave', 'Blvd', 'Ct', 'Dr', 'Pl', 'Rd', 'Sq', 'St']
street_types_map = {'Avenue': 'Ave', 'Boulevard': 'Blv', 'Court': 'Ct',
                         'Drive': 'Dr', 'Place': 'Pl', 'Road': 'Rd',
                         'Square': 'Sq', 'Street': 'St'}
cfcc_types_re = re.compile(r'([A-C])(\d{1,2})')

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
        return (name.rsplit(" ",1)[0], name.rsplit(" ",1)[1])
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

# Check if tiger:name_base and tiger:name_type attributes are populated. If
# not, infer it based on name attribute. This function returns a dictionary
# containing way ID and corresponding street_base, street_type attributes
# to be used for overwrites
def audit_ways(ways):
    overwrites = {}
    for way in ways:
        if is_street(way):
            street_name = way.tags.get("name")
            street_type = way.tags.get("tiger:name_type")
            street_base = way.tags.get("tiger:name_base")
            if street_type not in street_types:
                street_base, street_type = process_name(street_name)
                if street_type:
                    street_type = street_types_map[street_type]
                    overwrites[way.id] = (street_base, street_type)
    return overwrites

# Shape way data into a python dictionary for loading into MongoDB
def shape_way(way, overwrites):
    output = {}
    output['id'] = way.id
    output['type'] = 'way'

    nodes = way.get_nodes(resolve_missing=True)
    for node in nodes:
        if 'nodes' not in output:
            output['nodes'] = []
        output['nodes'].append(node.id)


    if way.tags:
        for tag_k in way.tags:
            if tag_k in ['tiger:name_base', 'tiger:name_type', 'name',
                         'tiger:county', 'tiger:cfcc']:
                if 'address' not in output:
                    output['address']={}
                tag_v = way.tags.get(tag_k)
                if (way.id in overwrites) and (tag_k == 'tiger:name_base'):
                    tag_v = overwrites[way.id][0]
                if (way.id in overwrites) and (tag_k == 'tiger:name_value'):
                    tag_v = overwrites[way.id][1]
                output['address'][tag_k] = tag_v

    return output

# Shape node data into a dictionary to be outputted to JSON
def shape_node(node):
    output = {}
    output['id'] = node.id
    output['type'] = 'node'
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
    return output

# Retrieve Mongo DB collections object based on overpy object type
def insert_item(overpy_obj, data, db):
    if type(overpy_obj) is overpy.Node: return db.nodes.insert_one(data)
    if type(overpy_obj) is overpy.Way: return db.ways.insert_one(data)
    raise(Exception(
        "Unrecognized overpy object %s cannot be inserted into MongoDB."
        % type(overpy_obj)))

# Get shape function by overpy object err_type
def shape_item(overpy_obj, overwrites):
    if type(overpy_obj) is overpy.Node: return shape_node(overpy_obj)
    if type(overpy_obj) is overpy.Way: return shape_way(overpy_obj, overwrites)

    raise(Exception(
         "Unrecognized overpy object %s. No function found to process data."
         % type(overpy_obj)))

def clear_db(db):
    db.nodes.delete_many({})
    db.ways.delete_many({})

# Convert overpy object into python dictionary then load into mongodb
def convert_then_insert_data(data, db, overwrites=None):
    err_data = {}
    for i, item in enumerate(data):
        try:
            item_dict = shape_item(item, overwrites)
            item_collection = insert_item(item, item_dict, db)
        except Exception as err:
            err_data[item.id] = str(err)
        if (i % 100) == 0: print(".", end='', flush=True)
    print("")
    print(
        "Uploaded %s documents into nodes collection in MongoDB." %
        (len(data) - len(err_data)))
    if len(err_data) > 0:
        print("Skipped %s nodes due to following errors:" % len(err_data))
        for err_type in set(err_data.values()):
            print(err_type)


if __name__ == "__main__":

    # Fetch OpenStreetMap data for a custom grid via Overpass API
    api = overpy.Overpass()
    result = api.query("(node(40.74,-73.96,40.750,-73.94);<;);out meta;")
    print("Retrieved %s Nodes from OpenStreetMap." % len(result.nodes))
    print("Retrieved %s Ways from OpenStreetMap. " % len(result.ways))

    # Audit data and generate overwrites for data cleaning
    overwrites = audit_ways(result.ways)
    print("Audit process returned %s overwrites to be applied to map data."
            % len(overwrites))

    # Connect to MongoDB Atlas
    client = pymongo.MongoClient("mongodb+srv://public_access:GoData2017!@analyze-openstreetmap-vhq3i.mongodb.net/map_lic")
    db = client.map_lic
    clear_db(db)

    # Load nodes data into nodes collection in mongo DB
    print("Begin uploading nodes data to MongoDB", end='')
    convert_then_insert_data(result.nodes, db)


    # Load ways data into ways collection in mongo DB
    print("Begin uploading ways data to MongoDB", end='')
    convert_then_insert_data(result.ways, db, overwrites)
