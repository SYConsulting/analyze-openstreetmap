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

# for a list of nodes/ways/relations, return a dictionary containing
# unique tag values found for a given key, and corresponding number
# of elements tagged with that key:value pair
def count_tags(key, elements):
    values = defaultdict(int)
    for element in elements:
        value = element.tags.get(key)
        if value:
            values[element.tags.get(key)]+=1
    return values

# for a list of elements, return a dictionary containing the distinct
# list of attributes and corresponding number of keys tagged with
# that attribute
def audit_way_tags(ways):
    way_tags = defaultdict(int)
    for element in elements:
        for tag in element.tags:
            way_tags[tag] += 1
    return way_tags

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

if __name__ == "__main__":

    # Fetch OpenStreetMap data for a custom grid via Overpass API
    api = overpy.Overpass()
    result = api.query("(node(40.74,-73.96,40.750,-73.94);<;);out meta;")
    print("# of Nodes:     %s" % len(result.nodes))
    print("# of Ways:      %s" % len(result.ways))

    # Audit data and generate overwrites for data cleaning
    overwrites = audit_ways(result.ways)

    # Connect to MongoDB Atlas
    client = pymongo.MongoClient("mongodb+srv://public_access:GoData2017!@analyze-openstreetmap-vhq3i.mongodb.net/map_lic")
    db = client.map_lic

    # Load data into mongo DB
    db_nodes = db.nodes
    db_nodes.delete_many({})
    for node in result.nodes:
        node_data = shape_node(node)
        db_nodes.insert_one(node_data)

    db_ways = db.ways
    db_ways.delete_many({})
    for way in result.ways:
        way_data = shape_way(way)
        db_ways.insert_one(way_data)
