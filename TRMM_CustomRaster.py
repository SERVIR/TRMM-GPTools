
#Script indended to servce in a 'ResultSet' map service
#   It extracts 3Hour Trmm Rasters based on a user defined date
#   and accumulates those between the beginning and end dates
#   Then spits out the result to a map service through ArcGIS Server
#
#Note: Change the paths when moving things between servers with different folder structures
#       Besure to set the ScratchWorkspacePath in ArcGIS desktop before deploying the service
#       Also, the output paramter in the Toolbar is ever so slightly different from the output parameters
#        In the TOC Layer.
#       It also helps to set the Arc Environment NOT to automatically build pyramids. 
#
#   There are two versions of this script. This one returns a raster dataset.
#
#SpatialDev
# 
#Import dependencies
import arcpy
from arcpy import env
from arcpy.sa import *
from datetime import datetime 
import os
import sys


arcpy.CheckOutExtension("Spatial")
env.overwriteOutput="true"

#change local pathnames to match local environment if necessary#
env.workspace="%scratchworkspace%"
#source of the actual raster catalog holding the individual 3hourly trmm layers
SourceDB = "FILE PATH"\\ReferenceNode\\FileGeodatabases\\TRMM.gdb\\TRMM"   
#GUAL boundaries used for clipping if necessary     
inClipFeatures = "FILE PATH"\ReferenceNode\\GPServices\\TRMM Tools\\boundaries\\GaulBoundaries.gdb\\Admin0"
scratch_workspace_basepath= "%scratchworkspace%"
#scratch path plus the gdb extension
scratch_fgdb_fullpath = "%scratchworkspace%\\scratch.gdb\\"
#This folder holds the extracted rasters if necessary for each transaction
archive_space="FILE PATH"\ReferenceNode\\GPServices\\TRMM Tools\\archive_rasters\\"

arcpy.gp.overwriteOutput = 1
arcpy.env.cellSize = 0.25

#general functions#
def createWhereClause(start_datetime,end_datetime):
        
    datetime_sql_cast="date"
    datetime_field_format='%m-%d-%Y %I:%M:%S %p'

    datetime_field = 'datetime'

    where_clause = "%s >= %s \'%s\'" % (datetime_field, datetime_sql_cast, start_datetime.strftime(datetime_field_format))
    where_clause += " AND %s <= %s \'%s\'" % (datetime_field, datetime_sql_cast, end_datetime.strftime(datetime_field_format))
            
    return where_clause
def AddMsgAndPrint(msg, severity=0):
    # Adds a Message (in case this is run as a tool)
    # and also prints the message to the screen (standard output)
    print msg

    # Split the message on \n first, so that if it's multiple lines, 
    #  a GPMessage will be added for each line
    try:
        for string in msg.split('\n'):
            # Add appropriate geoprocessing message 
            #
            if severity == 0:
                arcpy.AddMessage(string)
            elif severity == 1:
                arcpy.AddWarning(string)
            elif severity == 2:
                arcpy.AddError(string)
    except:
        pass
def getCountryClipRaster(country_name, scratch_workspace_basepath):
    AddMsgAndPrint('Getting Clip Features')
    try:

        #in_features = "FILE PATH"\TRMM Tools\\SampleGauls\\SimpleGauls.shp"
        #in_features = "FILE PATH"\\SERVIR\\ReferenceNode\\GPServices\\TRMM Tools\\SampleGauls\\SimpleGauls.shp"
                      
        #out_feature_class = os.path.join(scratch_workspace_basepath, "temp_fc")
        out_feature_class = scratch_fgdb_fullpath+"\\temp_fc"
        arcpy.Select_analysis(inClipFeatures, out_feature_class, "\"ADM0_NAME\" = \'"+country_name+"\'")
        
        clip_raster = out_feature_class+"_ras"
        arcpy.FeatureToRaster_conversion(out_feature_class+".shp", 'value', clip_raster, 0.25)
        
        return clip_raster
    except Exception as e:
            AddMsgAndPrint(e.message)
            arcpy.AddError(e.message)
def GetRasterRows(SourceDB,where_clause):
    rows = arcpy.SearchCursor(SourceDB, where_clause, '', '', '')

    try:
        return [str(row.getValue('Name')) for row in rows]
    except Exception as e:
        AddMsgAndPrint(e)
    finally:
        del rows

def extractRastersFromRasterCatalog(SourceDB,rasters_to_extract_list, archive_space):

    extracted_raster_list = []
    joinPath = os.path.join
    raster_name_field = 'Name'
    #output_raster_catalog = self.input_raster_catalog_options['raster_catalog_fullpath']
        
    for raster_name in rasters_to_extract_list:
            
        extracted_raster_fullpath = joinPath(archive_space, raster_name)
            
        if arcpy.Exists(extracted_raster_fullpath) and (raster_name not in extracted_raster_list):
            extracted_raster_list.append(extracted_raster_fullpath)
                       
        else:
            where_clause = "%s = \'%s\'" % (raster_name_field, str(raster_name))
            #AddMsgAndPrint('arcpy.RasterCatalogToRasterDataset_management('+SourceDB+','+archive_space+','+where_clause+')')
            AddMsgAndPrint('Extracted : '+ str(raster_name))
            arcpy.RasterCatalogToRasterDataset_management(SourceDB,archive_space+'\\'+raster_name, where_clause)
            
            extracted_raster_list.append(extracted_raster_fullpath)
    return extracted_raster_list  
    
def CreateFancyRaster(scratch_fgdb_fullpath,raster_list):
    AddMsgAndPrint('Creating Cummulative Raster')
    sumstring = ""

    for raster_name in raster_list:
        sumstring+= raster_name+'+'
          
    sumstring = sumstring[:-1]
    AddMsgAndPrint('Added Rasters for Calculation')

    final_raster = sum([Con(IsNull(raster), 0, raster) for raster in raster_list])
    final_raster = final_raster * 3

    if clip_country !="":
        final_raster = final_raster * Raster(input_parameters_dict['clip_raster']) 
        
    final_raster = SetNull(final_raster == 0, final_raster) # set 0's back to NULL after all mathematical operations are peformed
    return final_raster
#===get inputs======#
input_parameters_dict = {}
    
#When Debugging comment the line below this and uncomment the line below that.     
start_datetime = datetime.strptime(arcpy.GetParameterAsText(0), "%Y%m%d%H") # requried
#start_datetime = datetime.strptime("2011122203", "%Y%m%d%H")

#When Debugging comment the line below this and uncomment the line below that.     
end_datetime = datetime.strptime(arcpy.GetParameterAsText(1), "%Y%m%d%H")  #required
#end_datetime = datetime.strptime("2011122203", "%Y%m%d%H")

        
clip_country = arcpy.GetParameterAsText(2) # optional
#clip_country = "#"
#TODO put in some validation to make sure that the country name matches up with the actual GAUL0 Admin unit names
#clip_country="Somalia"

if clip_country != "":
   
    AddMsgAndPrint('Clipping to :'+clip_country)
    input_parameters_dict['clip_raster'] = getCountryClipRaster(clip_country, scratch_workspace_basepath)
#==== End getting inputs


where_clause = (createWhereClause(start_datetime,end_datetime))

AddMsgAndPrint('Selecting Dates Between: '+where_clause)

rasters_to_extract_list = GetRasterRows(SourceDB,where_clause)
extracted_rasters_list = extractRastersFromRasterCatalog(SourceDB,rasters_to_extract_list, archive_space)
AddMsgAndPrint('Extracted Individual Rasters')
#rainfall = scratch_fgdb_fullpath + "\\rainfall"
OutRas = CreateFancyRaster(scratch_fgdb_fullpath,extracted_rasters_list)

#if arcpy.Exists(scratch_fgdb_fullpath+"\\rainfall_ras"):
#     arcpy.Delete_management(scratch_fgdb_fullpath+"\\rainfall_ras")
   
AddMsgAndPrint('Saving output Raster Dataset')
#Copy the raster to save it to disk

arcpy.CopyRaster_management(OutRas,scratch_fgdb_fullpath+"\\rainfall_ras","DEFAULTS","","","","","16_BIT_UNSIGNED")
#arcpy.AddColormap_management(OutRas,"#","C:/Projects/SERVIR/ReferenceNode/GPServices/TRMM Tools/TRMM_ColorMap.clr")


#arcpy.RasterToPolygon_conversion(OutRas, rainfall,"NO_SIMPLIFY")
AddMsgAndPrint('Created Raster Layer Conversion, all done')
#On Result Set GPServices not need to set the last output parameters. 
