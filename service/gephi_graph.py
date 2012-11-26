from lxml import etree
from space_temporal import SpaceTemporalModel
from datetime import datetime
import time

class GephiGraph:

    def __init__(self, nodes_model, edges_model):
        self.nodes_model = nodes_model
        self.edges_model = edges_model

    def __build_nodes(self):
        nodes = etree.Element("nodes")
        children = []
        for antenna, data in self.nodes_model.items():
            latitude = -1
            longitude = -1
            values = []
            for time_gap in data:
                latitude = time_gap['latitude']
                longitude = time_gap['longitude']
                values.append(GephiAttValues(len(time_gap['users']), time_gap['date_start'], time_gap['date_end']))
            node = GephiNode(antenna, latitude, longitude, values)
            nodes.append(node.build_xml())
        return nodes

    def __build_edges(self):
        edges = etree.Element("edges")
        children = []
        for antennas, data in self.edges_model.items():
            values = []
            for time_gap in data:
                values.append(GephiAttValues(len(time_gap['users']), time_gap['date_start'], time_gap['date_end']))
            edge = GephiEdge(antennas[0], antennas[1], values)
            edges.append(edge.build_xml())
        return edges

    def build_xml(self):
        xmlns="http://www.gephi.org/gexf/1.1draft"
        xsi="http://www.w3.org/2001/XMLSchema-instance"
        schemaLocation="http://www.gephi.org/gexf/1.1draft http://gephi.org/gexf/1.1draft.xsd"
        viz="http://www.gexf.net/1.1draft/viz"
        version="1.1"
        gexfXML = etree.Element("{"+xmlns+"}gexf",version=version,nsmap={None:xmlns,'viz':viz,'xsi':xsi})
        gexfXML.set("{xsi}schemaLocation",schemaLocation)
        graph = etree.Element("graph", defaultedgetype="directed", timeformat="double", mode="dynamic")
        node_attrs = etree.Element("attributes", CLASS="node", mode="dynamic")
        node_attrs.append(etree.Element("attribute", id="latitude", title="latitude", type="double"))
        node_attrs.append(etree.Element("attribute", id="longitude", title="longitude", type="double"))
        node_attrs.append(etree.Element("attribute", id="value", title="value", type="integer"))
        graph.append(node_attrs)
        edge_attrs = etree.Element("attributes", CLASS="edge", mode="dynamic")
        edge_attrs.append(etree.Element("attribute", id="value", title="value", type="integer"))
        graph.append(edge_attrs)
        graph.append(self.__build_nodes())
        graph.append(self.__build_edges())
        gexfXML.append(graph)
        return gexfXML

class GephiNode:

    def __init__(self, id, latitude, longitude, values):
        self.id = "%s" % id
        self.latitude = "%s" % latitude
        self.longitude = "%s" % longitude
        self.values = values

    def build_xml(self):
        node = etree.Element("node", id = self.id, label=self.id)
        att_values = etree.Element("attvalues")
        latitude = etree.Element("attvalue", FOR = "latitude", value = self.latitude)
        longitude = etree.Element("attvalue", FOR = "longitude", value = self.longitude)
        att_values.append(latitude)
        att_values.append(longitude)
        for value in self.values:
            att_values.append(value.build_xml())
        node.append(att_values)
        return node

class GephiAttValues:

    def __init__(self, value, start_time, end_time):
        self.value = "%s" % value 
        self.start_time = "%s" % time.mktime(start_time.timetuple())
        self.end_time = "%s" % time.mktime(end_time.timetuple())

    def build_xml(self):
        return etree.Element("attvalue", FOR="value", value = self.value, start = self.start_time,
            end = self.end_time)

class GephiEdge:

    def __init__(self, source, target, values):
        self.source = "%s" % source
        self.target = "%s" % target
        self.values = values

    def build_xml(self):
        edge = etree.Element("edge", source = self.source, target = self.target)
        att_values = etree.Element("attvalues")
        for value in self.values:
            att_values.append(value.build_xml())
        edge.append(att_values)
        return edge

stm = SpaceTemporalModel()
nodes_model = stm.create_gephi_node_model(datetime(2011, 12, 7, 8, 0), datetime(2011, 12, 7, 20, 0))
edges_model = stm.create_gephi_edge_model(datetime(2011, 12, 7, 8, 0), datetime(2011, 12, 7, 20, 0))
graph = GephiGraph(nodes_model, edges_model)

content = etree.tostring(graph.build_xml(), pretty_print=True)
with open('nodes_edges.gexf', 'w') as f:
    f.write(str(content, 'utf-8'))




