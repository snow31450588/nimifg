#!/bin/env python
# encoding=utf8

import os
import sys
import gdal
import ogr


def read_mif(file_name, datasource, province_name):
    print(file_name)
    ds = gdal.OpenEx(file_name, gdal.OF_VECTOR )
    if ds is None:
        print("Open failed.\n")
        sys.exit( 1 )
    layer_name = os.path.splitext(os.path.basename(file_name))[0]
    table_name = layer_name[:-len(province_name)]
    layer_read = ds.GetLayerByName(layer_name)
    feat_defn = layer_read.GetLayerDefn()

    layer_write = datasource.CreateLayer(table_name, None, layer_read.GetGeomType(), options = ['SPATIAL_INDEX=NO'] )
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
    driverName = "SQLite"

    province_name = os.path.basename(fd)
    print(fd, sqn, province_name)

    datasource = create_datasource(driverName, sqn)

    if not datasource:
        raise Exception("Create output failed!")

    for table, sub in ((('R', 'road'), ('BL', 'back'), ('BN', 'back'), ('BP', 'back'), ('BUP', 'back'), ('D', 'back'))):
        table_r = os.path.join(fd, sub, '%s%s.mif'%(table, province_name))
        read_mif(table_r, datasource, province_name)



def usage():
	print('''Usage: %s <nimifg folder>'''%__file__)


if __name__ == '__main__':
    if len(sys.argv)!=3:
        usage()
        sys.exit()
    
    fd_src = sys.argv[1]
    sqn = sys.argv[2]
    folder_to_sqlite(fd_src, sqn)
