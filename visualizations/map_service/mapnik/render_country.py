#!/usr/bin/python
# -*- coding: utf-8 -*-

import mapnik

def main():
    # creates the map 
    m = mapnik.Map(1200,600)

    # Blue background
    m.background = mapnik.Color('steelblue')

    s = mapnik.Style()
    r = mapnik.Rule()

    # Styles - If style is not defined, then it's not drawed
    # Polygon style (countries)
    polygon_symbolizer = mapnik.PolygonSymbolizer(mapnik.Color('#f2eff9'))
    r.symbols.append(polygon_symbolizer)

    # Line style
    line_symbolizer = mapnik.LineSymbolizer(mapnik.Color('rgb(50%,50%,50%)'),0.1)
    r.symbols.append(line_symbolizer)

    # Point style (default)
    point_symbolizer = mapnik.PointSymbolizer()
    r.symbols.append(point_symbolizer)

    s.rules.append(r)
    m.append_style('My Style',s)

    # World Map Layer
    m.layers.append(create_layer('world','resources/ne_110m_admin_0_countries.shp','My Style'))
    # Ivory Coast Layer
    m.layers.append(create_layer('points','resources/antennas.shp','My Style'))

    # Zoom to layer 2 boundaries
    m.zoom_to_box(layer2.envelope())

    mapnik.render_to_file(m,'ivory.png', 'png')

def create_layer(name, sourcefile, style):
    ds = mapnik.Shapefile(file=sourcefile)
    layer = mapnik.Layer(name)
    layer.datasource = ds2
    layer.styles.append(style)
    return layer


if name == '__main__':
    main()