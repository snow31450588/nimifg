#!/bin/env python
# encoding=utf8

import os
import sys

from osgeo import gdal
from osgeo import ogr

from google_tiles import create_tiles


def create_datasource(driverName, file_name):
    drv = gdal.GetDriverByName( driverName )
    if drv is None:
        print("%s driver not available.\n" % driverName)
        return None
    ds = drv.Create( file_name, 0, 0, 0, gdal.GDT_Unknown, options = ['SPATIALITE=YES'] )
    if ds is None:
        print("Creation of output file failed.\n")
        return None
    return ds


def tile_sqlite(sqn_src, sqn_dst, level):
    print(sqn_src, sqn_dst, level)

    driver_name = "SQLite"
    data_dst = create_datasource(driver_name, sqn_dst)
    if not data_dst:
        raise Exception("Create output failed!")

    data_source = gdal.OpenEx(sqn_src, gdal.OF_VECTOR )
    if not data_source:
        raise Exception("Read data source failed!")


    ALL_TILES = []

    for i in range(data_source.GetLayerCount()):
        layer_read = data_source.GetLayer(i)
        table_name = layer_read.GetName()
        geom_type = layer_read.GetGeomType()
        feat_defn = layer_read.GetLayerDefn()
        extent = layer_read.GetExtent()
        print(table_name, geom_type, extent)
        if geom_type == 0:
            continue

        if table_name!='mcm_build':
            continue

        layer_write = data_dst.CreateLayer(table_name, None, geom_type, options = ['SPATIAL_INDEX=NO'] )
        if layer_write is None:
            print("Layer creation failed.\n")

        for i in range(feat_defn.GetFieldCount()):
            field_defn = feat_defn.GetFieldDefn(i)
            if layer_write.CreateField ( field_defn ) != 0:
                print("Creating field failed.\n")
                sys.exit( 1 )
        for field_name in ('row', 'col', 'level'):
            field = gdal.ogr.FieldDefn(field_name, gdal.ogr.OFTInteger)
            layer_write.CreateField(field)
        feat_write_defn = layer_write.GetLayerDefn()

        tiles = create_tiles.create_google_tiles_lonlat(extent, level)
        ALL_TILES.extend(tiles)

        fid = 0
        layer_read.ResetReading()
        layer_write.StartTransaction()
        for feat in layer_read:
            geo = feat.GetGeometryRef()
            pt = geo.Centroid()
            col, row = create_tiles.lonlat_to_cell(pt.GetX(), pt.GetY(), level)

            feature = gdal.ogr.Feature(feat_write_defn)
            feature.SetFrom(feat)
            feature.SetFID(fid)
            feature.SetField("col", col)
            feature.SetField("row", row)
            feature.SetField("level", level)
            #feature.SetGeomFieldDirectly("GEOMETRY", geom.Clone())
            fid += 1
            if layer_write.CreateFeature(feature) != 0:
                print("Failed to create feature in output.\n")
                sys.exit( 1 )
            feature.Destroy()
            feat.Destroy()
        layer_write.CommitTransaction()

    table_name = 'google_tiles'
    geom_type = gdal.ogr.wkbPolygon
    layer_write = data_dst.CreateLayer(table_name, None, geom_type, options = ['SPATIAL_INDEX=NO'] )
    if layer_write is None:
        print("Layer creation failed.\n")
    field = gdal.ogr.FieldDefn("ID", gdal.ogr.OFTString)
    if layer_write.CreateField ( field ) != 0:
        print("Creating field failed.\n")
        sys.exit( 1 )

    feat_defn = layer_write.GetLayerDefn()
    layer_write.StartTransaction()
    for i, tile in enumerate(set(ALL_TILES)):
        col, row, tile_extent = tile
        feature = gdal.ogr.Feature(feat_defn)
        feature.SetField("ID", "%d_%d"%(col,row))
        feature.SetGeomFieldDirectly("GEOMETRY", gdal.ogr.CreateGeometryFromWkt(extent_to_wkt(tile_extent)))
        if layer_write.CreateFeature(feature) != 0:
            print("Failed to create feature in shapefile.\n")
            sys.exit( 1 )
        feature.Destroy()
    layer_write.CommitTransaction()

    data_dst = None


def extent_to_wkt(extent):
    lon_min, lon_max, lat_max, lat_min = extent
    return 'Polygon ((%f %f, %f %f, %f %f, %f %f, %f %f))'%(lon_min,lat_min, lon_min,lat_max, lon_max,lat_max, lon_max,lat_min, lon_min,lat_min)

def usage():
	print('''Usage: %s <source sqlite file> <destination sqlite file> <level>'''%__file__)


if __name__ == '__main__':
    if len(sys.argv)!=4:
        usage()
        sys.exit()

    sqn_src = sys.argv[1]
    sqn_dst = sys.argv[2]
    level = int(sys.argv[3])

    import time
    s = time.time()

    tile_sqlite(sqn_src, sqn_dst, level)

    e = time.time()

    print('start: ', s)
    print('elapse: ', e-s)
    print('end: ', e)
