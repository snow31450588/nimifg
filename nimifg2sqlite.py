#!/bin/env python
# encoding=utf8

import os
import sys
import glob

import gdal
import ogr


def read_mif(data_source, table_name, file_name):
    print(table_name, file_name)
    ds = gdal.OpenEx(file_name, gdal.OF_VECTOR )
    if ds is None:
        print("Open failed.\n")
        sys.exit( 1 )
    layer_name = os.path.splitext(os.path.basename(file_name))[0]
    layer_read = ds.GetLayerByName(layer_name)
    feat_defn = layer_read.GetLayerDefn()

    layer_write = data_source.CreateLayer(table_name, None, layer_read.GetGeomType(), options = ['SPATIAL_INDEX=NO'] )
    if layer_write is None:
        print("Layer creation failed.\n")

    for i in range(feat_defn.GetFieldCount()):
        field_defn = feat_defn.GetFieldDefn(i)
        if layer_write.CreateField ( field_defn ) != 0:
            print("Creating field failed.\n")
            sys.exit( 1 )

    layer_read.ResetReading()
    layer_write.StartTransaction()
    for feat in layer_read:
        if layer_write.CreateFeature(feat) != 0:
            print("Failed to create feature in shapefile.\n")
            sys.exit( 1 )
        feat.Destroy()
    layer_write.CommitTransaction()
    ds = None


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


def folder_to_sqlite(fd, sqn):
    driver_name = "SQLite"

    province_name = os.path.basename(fd)

    data_source = create_datasource(driver_name, sqn)

    if not data_source:
        raise Exception("Create output failed!")

    for sub in os.listdir(fd):
        for mif in glob.glob(os.path.join(fd, sub, '*.mif')):
            basename = os.path.basename(mif)
            basename = os.path.splitext(basename)[0]
            if len(basename)>len(province_name):
                basename = basename[:-len(province_name)]
            table_name = "%s_%s"%(sub, basename)
            read_mif(data_source, table_name, mif)


def usage():
	print('''Usage: %s <nimifg folder>'''%__file__)


if __name__ == '__main__':
    if len(sys.argv)!=3:
        usage()
        sys.exit()
    
    fd_src = sys.argv[1]
    sqn = sys.argv[2]
    folder_to_sqlite(fd_src, sqn)
