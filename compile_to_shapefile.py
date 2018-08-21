#!/bin/env python
# encoding=utf8

import os
import sys
from osgeo import gdal
from osgeo import ogr


def create_shapefile(file_name, feature_type):
    print(file_name)
    driver_name = "ESRI Shapefile"
    drv = ogr.GetDriverByName( driver_name )
    if drv is None:
        print("%s driver not available.\n" % driver_name)
        sys.exit( 1 )
    ds = drv.CreateDataSource( file_name)
    if ds is None:
        print("Creation of output file failed.\n")
        sys.exit( 1 )
    layer_name = os.path.splitext(os.path.basename(file_name))[0]
    lyr = ds.CreateLayer(layer_name, geom_type=feature_type, options = ['ENCODING=UTF-8'])
    if lyr is None:
        print("Layer creation failed.\n")
        sys.exit( 1 )
    return lyr, ds


def sqlite_to_shapefile(sqn, fd_out):
    if not os.path.exists(fd_out):
        os.makedirs(fd_out)

    data_source = gdal.OpenEx(sqn, gdal.OF_VECTOR )

    table, sql = "compile_land_t", "select t.*, n.name, n.language from land_t t left join other_fname n on n.featid=t.id"

    layer_read = data_source.ExecuteSQL(sql)
    geom_type = layer_read.GetGeomType()

    feat_defn = layer_read.GetLayerDefn()
    layer_name = table
    file_name = os.path.join(fd_out, layer_name + '.shp')
    layer_write, ds_write = create_shapefile(file_name, geom_type)
    if layer_write is None:
        print("Layer creation failed.\n")
    for i in range(feat_defn.GetFieldCount()):
        field_defn = feat_defn.GetFieldDefn(i)
        if layer_write.CreateField ( field_defn ) != 0:
            print("Creating field failed.\n")
            sys.exit( 1 )

    layer_read.ResetReading()
    #layer_write.StartTransaction()
    for feat in layer_read:
        if layer_write.CreateFeature(feat) != 0:
            print("Failed to create feature in shapefile.\n")
            sys.exit( 1 )
        feat.Destroy()
    #layer_write.CommitTransaction()
    ds_write = None
    data_source = None


def usage():
	print('''Usage: %s <sqlite file> <shapefile output folder>'''%__file__)


if __name__ == '__main__':
    if len(sys.argv)!=3:
        usage()
        sys.exit()
    
    sqn = sys.argv[1]
    fd_out = sys.argv[2]
    sqlite_to_shapefile(sqn, fd_out)
