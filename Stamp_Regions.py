#####  Philatelic eBay Listings Database Creator: World Regions
#####  Concurrently writes .csv files and SQL database files

DEBUG = True # Toggles verbosity of nested object parsing

if DEBUG == True: import ebaysdk
import datetime
import mysql.connector
import json 

start_time_stamp = datetime.datetime.now() # returns an object

from ebaysdk.finding import Connection
from ebaysdk.exception import ConnectionError



def checkModuleContents():

    if DEBUG == True: print("------ Contents of ebaysdk--------")
    if DEBUG == True: print(dir(ebaysdk))
    print("------ Contents of ebaysdk.finding.Connection --------")
    print(dir(ebaysdk.finding.Connection)) # contains the API equivalent calls like response.dict()
    if DEBUG == True: print("------ Contents of ebaysdk.utils --------")
    if DEBUG == True: print(dir(ebaysdk.utils))
    print("------ Contents of ebaysdk.exception.Connection Error --------")
    print(dir(ebaysdk.exception.ConnectionError)) 
    print("--------------")

def checkSDKResults():

    if DEBUG == True: print("inside function checkSDKResults()")
    print("--------------")
    
    # turns dictionary into XML
    print("--------------")
    print("response in XML")
    print(ebaysdk.utils.dict2xml(response.dict()))
    
    # get_dom_tree() function is broken and I reported it.
    #tree = ebaysdk.utils.dict2xml(response.dict())
    
    print("--------------")
    #print(ebaysdk.utils.get_dom_tree(tree))
   
    print("response this dictionary:")
    print(response.dict())
    print("response JSON: use with tree viewer at https://countwordsfree.com/jsonviewer")
    print(response.json())
    print("response content string")
    print(response.content)
    print("response reply object")
    print(response.reply)
    print("response code")
    print(api.response_code())
    print("response status")
    print(api.response_status())
    print("response code list")
    print(api.response_codes())
    print("response dom object")
    print(response.dom())
    print("Total Hits")
    print(response.reply.paginationOutput.totalEntries)
    print("--------------------------")
    print("item member properties. Some may contain nested properties.")
    item_properties = dir(response.reply.searchResult.item)
    print(item_properties) # Thonny OBJ inspector calls them data
    print("--------------------------")
    print("item member methods. Some may contain nested methods.")
    item_methods = dir(response.reply.searchResult.item)
    print(item_methods) # Thonny OBJ inspector calls them attributes
    print("--------------")

def checkCategoryHistogram():
    
    print("--------------")
    print(type(response.reply.categoryHistogramContainer.categoryHistogram[0].categoryId))
    print(type(response.reply.categoryHistogramContainer.categoryHistogram[0].categoryName))
    print(type(response.reply.categoryHistogramContainer.categoryHistogram))
    print(response.reply.categoryHistogramContainer.categoryHistogram[0].categoryId)
    print(response.reply.categoryHistogramContainer.categoryHistogram[0].categoryName)
    print(response.reply.categoryHistogramContainer.categoryHistogram[0].childCategoryHistogram)
    print(len(response.reply.categoryHistogramContainer.categoryHistogram[0].childCategoryHistogram))
    print(f"Title: {response.reply.categoryHistogramContainer.categoryHistogram[0].childCategoryHistogram}")        
    print("--------------")
    
def checkChild(x):
    
    print("----- Check Child ---------")
    print(x)
    print(x.categoryId)
    print(x.categoryName)
    print(x.count)
    print("--------------")
    
def openCSVFiles():

    if DEBUG == True: print("inside function openCSVFiles()")
    print("--------------")
    
    # spreadsheet update once per day for graphing of Category wide trends.
    # not dependent on any particular itemId.
    # Since I don't close the CSV files until the very end of main, if there problems with the database
    # population code or any of the parsing, the spreadsheet values will not be written to disk file.
    # Other loops depend on these filenames so needs to be global.
    
    # categoryHistogramContainer 
    global country_testfile
    # called by retrieveCategoryHistogramContainer()
    country_testfile = open("C:\\Users\\eBay_Sandbox\\Documents\\Finding\\findItemsByCategory\\eBay_Stamp_Stats_World_Regions_Historical.txt", "at")
    
    query_time_stamp = datetime.datetime.now() # returns an object
    if DEBUG == True: print(type(query_time_stamp))
    
    time_check = query_time_stamp.strftime("%m/%d/%Y, %H:%M:%S")
    
    # Put this on all files's header
    standardOutputFields = response.reply.ack + "," + str(response.reply.timestamp) + "," + response.reply.version +  "\n"
    # Verbose header for each retrieved day. version field has its own \n so omit at end of header string below
    
    country_testfile.write("Access," + time_check + "," + response.reply.categoryHistogramContainer.categoryHistogram[0].categoryId + "," + response.reply.categoryHistogramContainer.categoryHistogram[0].categoryName + "," + standardOutputFields)
    
    global json_testfile
    # called inside this function. Saving JSON in a MongoDB for auditing and troubleshooting.
    json_testfile = open("C:\\Users\\eBay_Sandbox\\Documents\\Finding\\findItemsByCategory\\eBay_Stamp_Stats_World_Regions_JSON.txt", "at")
    json_testfile.write(response.json() + "\n") # keep just in case databases were not created correctly, to troubleshoot, or for auditing.
    
        
def retrieveCategoryHistogramContainer():

    if DEBUG == True: print("inside function retrieveCategoryHistogramContainer()")
    print("--------------")
    
    global dbEntries # holds the country/region name and hit entries pairs as key-value str-int
    dbEntries = dict() # error to set as global and assign a value in one step.
    
    for categoryHistogram in response.reply.categoryHistogramContainer.categoryHistogram:
                    
        # look to try and assign the list to a new variable and then loop over that
        if DEBUG == True: checkCategoryHistogram()
        
        # must make as explicit iterable otherwise you get attribute mismatch errors list vs object types??!!
        iterate_child = iter(categoryHistogram.childCategoryHistogram)
        for x in iterate_child:
            
            site_data = x.categoryId + "," + x.categoryName + "," + x.count +"\n"
            country_testfile.write(site_data) # Broad Region-of-World groupings
            
            # A dictionary is auto-appended by simply creating a new key-value pair as belw.
                        # key           # value
            dbEntries[x.categoryName] = int(x.count) # function return variable
                
            if DEBUG == True: checkChild(x)
            print("--------------")
            if DEBUG == True: print(dbEntries)
            print("--------------")
             
        # end of for x loop
        # end of for categoryHistogram loop
    return dbEntries

def retrieveSubCategoryIDs():

    if DEBUG == True: print("inside function retrieveSubCategoryIDs()")
    print("--------------")
    
    global  eBaySubIDs # holds the ebay defined subcategory SubIDs served up by eBay as long ints
    eBaySubIDs = []    # error to set as global and assign a value in one step.
    
    for categoryHistogram in response.reply.categoryHistogramContainer.categoryHistogram:
                    
        # look to try and assign the list to a new variable and then loop over that
        if DEBUG == True: checkCategoryHistogram()
        
        # must make as explicit iterable otherwise you get attribute mismatch errors list vs object types??!!
        iterate_child = iter(categoryHistogram.childCategoryHistogram)
        for x in iterate_child:
            
            # For a list, need to specify the position of insertion as 1st parameter and it is required as there is no default.
            
            eBaySubIDs.insert(0, x.categoryId) # add in the current long int subID, function's return variable
                
            if DEBUG == True: checkChild(x)
            if DEBUG == True: print(eBaySubIDs)
            
        # end of for x loop
        # end of for categoryHistogram loop
    return eBaySubIDs
        
def closeCSVFiles():
    
    if DEBUG == True: print("inside function closeCSVFiles()")
    print("--------------")
    
    # happens very late in __main__
    country_testfile.close()
    json_testfile.close()
   


def sqlHandshake():

    if DEBUG == True: print("inside function sqlHandshake()")
    print("--------------")
    
    global mydb
    mydb = mysql.connector.connect(
      host="localhost",
      user="eBay_Sandbox",
      password="pythonApp!",
      database="ebay_stamps_container"
    )
    mycursor = mydb.cursor() 
    return mycursor


    
def existsTable(table_name, mycursor):

    if DEBUG == True: print("inside function existsTable(table_name, mycursor)")
    print("--------------")
    
    # passes in a string -> root phrase of the table(s) to be verified
    name_stem = ("%" + str(table_name) + "%") # need pair of % to wildcard

    sql = "SHOW TABLES LIKE %s"

    val = (name_stem,) # needs to be tuple with a trailing comma ?!
    mycursor.execute(sql, val)

    mysqlresult = mycursor.fetchall()
    
    print("--------------")
    print(type(mysqlresult))
    print("list of tables")
    if DEBUG == True: print(mysqlresult)
    print("--------------")
    
    # Abort if the DB tables are not right
    if mysqlresult == []:
        print("no tables matching that name stem in the connected database. Check the spelling or try a different database.")
        raise Exception()
    else:
        print("the tables are legit")
        
    for x in mysqlresult:
        print(x) 
    print("--------------")
        
def updateRegionsMetadata():

    if DEBUG == True: print("inside updateRegionsMetadata()")
    print("--------------")
    
    # TRUNCATE TABLE regions_metadata to remove all rows and reset AUTO_INCREMENT
    # TO THE SYSTEM VARIABLE VALUE FOR AUTO_INCREMENT_OFFSET WHICH IS USUALLY = 1 IN WORKBENCH.
    # This is faster than DELETE, not reversible, and avoids leaving the Primary key at the most recent
    # increment value i.e. the max value before removing rows.
    
    # Talks to the database now after passing many error checks in previous functions
    # IMPORTANT- Only use 1 Primary Key and make sure it increments. Otherwise you will get a syntax error.
    # plus an error message saying to check your MySQL manual for the proper programming 
    
    strdate = str(response.reply.timestamp)
    sql = "INSERT INTO regions_metadata (timestamp, ack, version, category_name, JSON) VALUES (%s, %s, %s, %s, %s)"
                                                                        # as a string                           can not use double [][] to index the value of dict within a list.
    val = (strdate, response.reply.ack, response.reply.version, response.reply.categoryHistogramContainer.categoryHistogram[0].get("categoryName"), response.json(),)

    # It is Ok to mix and match single and double quote in the same call
    
    containerCursor.execute(sql, val) # if more than one row would need .executemany()

    mydb.commit()

def updateRegions(valuesToInsert):

    if DEBUG == True: print("inside function updateRegions(valuesToInsert)")
    print("--------------")
    
    # Talks to the database now after passing many error checks in previous functions

    strdate = str(response.reply.timestamp)
    
    # IMPORTANT: Behind the scenes the Python module will convert all your %s strings into the correct datatype as designed in the database.
    # Otherwise you will get this error: would be better if it told how many excess parameters it found or about the format
    # of not being %s is wrong.
    
    """ ProgrammingError: Not all parameters were used in the SQL statement
cursor_cext.py, line 260

entriesToInsert: {'United States': '1471141', 'Europe': '14152...}  | len(entriesToInsert): 16

    Exception raised on line 260 of file 'PYTHON:\site-packages\mysql\connector\cursor_cext.py'.
    
       258:                 stmt = RE_PY_PARAM.sub(psub, stmt)
       259:                 if psub.remaining != 0:
    -->260:                     raise errors.ProgrammingError(

    global errors: <module 'mysql.connector.errors' from 'c:\\us...> """

    # At first, Great Britian is a misspelling in the database so made it misspelled here too.
    # issue this in Workbench MyInstance Query window to chaneg permanently --> ALTER TABLE regions CHANGE Great_Britian Great_Britain BIGINT UNsigned

    sql = "INSERT INTO regions (date, category_ID, Africa, Asia, Australia_Oceania, British_Colonies_Territories, Canada, Caribbean, Europe, Great_Britain, Latin_America, Middle_East, Other_Stamps, Publications_Supplies, Specialty_Philately, Topical_Stamps, United_States, Worldwide) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    
    val = (strdate, str(request["categoryId"]), valuesToInsert["Africa"], valuesToInsert["Asia"], valuesToInsert["Australia & Oceania"], valuesToInsert["British Colonies & Territories"], valuesToInsert["Canada"], valuesToInsert["Caribbean"], valuesToInsert["Europe"], valuesToInsert["Great Britain"], valuesToInsert["Latin America"], valuesToInsert["Middle East"], valuesToInsert["Other Stamps"], valuesToInsert["Publications & Supplies"], valuesToInsert["Specialty Philately"], valuesToInsert["Topical Stamps"], valuesToInsert["United States"], valuesToInsert["Worldwide"],)

    # It is Ok to mix and match single and double quote in the same call
    
    containerCursor.execute(sql, val)

    mydb.commit()
    
def checkNumberRegions(checkKeys):

    # Confirm that today's keys for region names match historical ones, otherwise abort
    
    if DEBUG == True: print("inside function checkNumberRegions(checkKeys)")
    print("--------------")
    
    standardRegions = {"Africa", "Asia", "Australia & Oceania", "British Colonies & Territories", "Canada", "Caribbean", "Europe", "Great Britain", "Latin America", "Middle East", "Other Stamps", "Publications & Supplies", "Specialty Philately", "Topical Stamps", "United States", "Worldwide"}
    retrievedKeys = set(checkKeys.keys()) # passed in as a list
    
    if len(standardRegions) == len(retrievedKeys):
        print("correct number of keys")
        print("--------------")
        if len(standardRegions.difference(retrievedKeys)) == 0:
            print("All Retrieved keys exactly match the standard keys")
            print("--------------")
        else:
            print("At least one of the key names is a mismatch vs standard")
            print("--------------")
            raise Exception()
    else:
        print("Wrong number of Regions. Retrieved %s keys", len(retrievedKeys))
        print("--------------")
                
        
def checkRegionSubIDs(checkSubIDKeys):

    if DEBUG == True: print("inside function checkRegionSubIDs(checkSubIDKeys)")
    print("--------------")
    
    # not in this order --> "Africa", "Asia", "Australia & Oceania", "British Colonies & Territories", "Canada", "Caribbean", "Europe", "Great Britain", "Latin America", "Middle East", "Other Stamps", "Publications & Supplies", "Specialty Philately", "Topical Stamps", "United States", "Worldwide"
    # For example, United States is 261
    # AS pulled from eBay these are strings and NOT integers.
    # a call to a set's .difference() method will shwo everything as different if ints are compared to strings.
    
    standardRegionSubIDs = {'4742', '261', '3499', '4752', '65174', '181420', '181416', '3478', '181424', '181423', '181417', '181422', '7898', '181421', '179377', '170137'}
    testSubIDs = set(checkSubIDKeys) # checkSubIDKeys is passed in as a list
    retrievedSubIDKeys = testSubIDs 
    
    if len(standardRegionSubIDs) == len(retrievedSubIDKeys):
        print("correct number of SubIDs")
        print("--------------")
        if len(standardRegionSubIDs.difference(retrievedSubIDKeys)) == 0:
            print("All Retrieved Subcategory IDs exactly match the standard")
            print("--------------")
        else:
            print("At least one of the Subcategory IDs is a mismatch vs standard")
            print(standardRegionSubIDs.difference(retrievedSubIDKeys))
            print("--------------")
            raise Exception()
    else:
        print("Wrong number of Subcategory IDs. Retrieved %s SubID keys", len(retrievedSubIDKeys))
        print("--------------")
        raise Exception()        
        

def checkValidJSON(JSON_var):

    if DEBUG == True: print("inside function checkValidJSON(JSON_var)")
    print("--------------")
    
    # .loads() will raise an JSONDecodeError if the JSON is ill-formatted
    print("--------------")
    returned_dict = json.loads(JSON_var)
    if DEBUG == True: print(returned_dict)
    if DEBUG == True: print(type(returned_dict))
    print("JSON is valid")
    print("--------------")
        
if __name__ == '__main__':
    
       # Sandbox
       # Sandbox Server. Only contains small fraction of size of Production Server and listings are sparsely filled. Best to test parallel calls.
       # api = Connection(domain='svcs.sandbox.ebay.com', config_file=None, debug=True, warnings=True, appid="JohnQuag-StampFin-SBX-8c8e89b76-6f4c35d2", https=True)
       # Production
       # Production Server The real live thing Ok to do read only findng calls on it and bypass sandbox for small totl entries
       # api = Connection(config_file='C:\\Users\\eBay_Sandbox\\Documents\\Finding\\ebay.yaml', siteid="EBAY-US")
       # Sandbox - only contains test listings, a subset of production listings
    
    try:
        # check that ebay_stamps_container Schema has the correct tables.
        containerCursor = sqlHandshake()
        existsTable("region", containerCursor)
        existsTable("grade", containerCursor)
    except:
        print("something is missing or corrupt with DB tables. check the connection to DB via mydb too.")
        raise Exception()
    
    
    if DEBUG == True: checkModuleContents()
    
    try:
    
        # The SoldItemsOnly value of true is reserved for future use but false is the only value now.
        # The Histogram outputs are general Category wide ensemble data for Stamps = 260 categoryId
        # Production Calls
        # Input specifiers
        
        api = Connection(config_file='C:\\Users\\eBay_Sandbox\\Documents\\Finding\\ebay.yaml', siteid="EBAY-US")
        request = {
            'categoryId': 260,
            'itemFilter': {'name': 'SoldItemsOnly', 'value': 'false'},
            'paginationInput': {
                'entriesPerPage': 1,
                'pageNumber': 1
            },
            'sortOrder': 'WatchCountDecreaseSort',
            'outputSelector': 'CategoryHistogram'    
        } 
        
      
        # https://developer.ebay.com/DevZone/finding/CallRef/extra/fnditmsbyctgry.rqst.tptslctr.html
        
        
        # Talk to eBay
        response = api.execute('findItemsByCategory', request) 
        
        checkValidJSON(response.json()) # Abort if JSON is no good
        
        if response.reply.categoryHistogramContainer.categoryHistogram[0].get("categoryName") != "Stamps":
            print("This is no longer the Stamps Category!")
            print("------------------------")
            raise Exception()
        
        # totalEntries is a string.
        if int(response.reply.paginationOutput.totalEntries) > 0:
            
            if DEBUG == True: checkSDKResults()
            
            openCSVFiles()
            
            # World Regions: 16 of them
            # Only need this once per day because it is ensemble data
            
            entriesToInsert = retrieveCategoryHistogramContainer() # get country name and count pairs in a dictionary
            checkNumberRegions(entriesToInsert)                    # Aborts if fails
            regionSubIDs = retrieveSubCategoryIDs()                # get region subcategory IDs like United States is 261
            checkRegionSubIDs(regionSubIDs)                        # Aborts if fails
            updateRegionsMetadata()                                # DB dirty work 5 columns
            updateRegions(entriesToInsert)                         # populate the columns 18 columns
            closeCSVFiles()                                        # Without a flush() will not get values to csv file
            
        # end of if conditional for totalEntries
        
        else:
            print("no matches at all") # totalEntries = "0"
          
        
    # outer try call for api connect    
    except ConnectionError as e:
        print(e)
        print(e.response.dict())
        
    if api.error():
        print(api.error())
    else:
        print("script completed normally")
        
    print("Current version of ebaysdk is ", response.reply.version)    
    print(start_time_stamp.strftime("%m/%d/%Y, %H:%M:%S"))
    end_time_stamp = datetime.datetime.now() # returns an object
    print(end_time_stamp.strftime("%m/%d/%Y, %H:%M:%S"))





 
        