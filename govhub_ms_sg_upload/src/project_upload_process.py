import json
import pandas as pd
import os
import dbUtil as db
import pandas as pd 
import project_upload_helper as ms
import logging
import datetime



def main():
    # try to read the connection data
    try:
        with open("config/ConfigurationData.json") as f:
            DbConnectionData = json.load(f)
        logging.info('Retrieved connection data')
    # raise an exception if connection data cannot be read
    except Exception as err:
        logging.error(f'Error reading connection data - {err}')
        raise Exception

    # create a connection to the CEDW database
    CEDW_dbConn = db.dbConn(DbConnectionData, 'SourceConnection')
    logging.info('Opened CEDW DB connection')

    #This is the DEV location
    LOGGING_PATH = os.environ['LOGGING_PATH']

    #This is the Prod Location
    MS_UPLOAD_LOC = os.environ['MS_UPLOAD_LOC']
    files_in_import_dir = [os.path.join(MS_UPLOAD_LOC, file) for file in os.listdir(MS_UPLOAD_LOC)]

    
    for fullpath in files_in_import_dir:
        filename = fullpath.split("/")[-1]
        filename_no_ext = filename.split(".")[0]
        process_time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        try:
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)
            logging.basicConfig(filename=f'{LOGGING_PATH}/LOG_{filename_no_ext}_{process_time}.log',
                                level=logging.INFO,
                                format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
            logging.info("Logger Configured")
        except:
            logging.error("Logger Configuration failed")
            exit(1)


        # Filename Business Rules Check
        try:
            filename_checker = filename.split("_")
            # MS or SG only
            assert(filename_checker[0]=='MS' or filename_checker[0]=='SG'or filename_checker[0]=='SS')
            # PR values are valid 
            assert(filename_checker[1][:2] == 'PR')
            assert(len(filename_checker[1]) == 8)
            # Version number
            assert(len(filename_checker[2]) == 2)
            #Dates and filetype are correct
            assert(len(filename_checker[3]) == 12)
            assert(filename_checker[3][-3:].lower() == 'xml')
        except:
            logging.warning(f'Filename: {filename} does not conform to business rules')
            continue

        try:
            logging.info('Cleaning up Staging Tables')
            ms.execute_sqlfile(CEDW_dbConn, 'sql/cleanup_staging.sql',batch_run_id=filename)
            logging.info('Cleaning up Integrated Tables')
            ms.execute_sqlfile(CEDW_dbConn, 'sql/cleanup_integrated.sql',batch_run_id=filename)
        except:
            logging.error('Unable to clean up Staging or Integrated Tables')
            continue
        
        try:
            logging.info(f'Parsing XML into dictionary of (tablename, dataframe) of file {filename}')
            #Parse the XML and turn into a tree
            root = ms.XMLtoElementTree(fullpath)
            #Load configuration file containing XML to Staging Table mapping
            config = ms.initializeConfiguration('config/config.json')
            #Parse the element Tree into a dictionary of dataframes
            data = ms.ElementTreetoDataFrameDictionary(root,config,filename)
        except:
            logging.warning(f'Unable to parse XML into dictionary')
            continue
        

        for table_name,dataframe in data.items():

            column_list = [x for x in dataframe.columns]
            num_columns = len(column_list)
            if num_columns > 0: 
                insertStmt = f"""INSERT INTO {CEDW_dbConn.getDbName()}.{CEDW_dbConn.getDbOwner()}.{table_name}
                        ({','.join(column_list)}) VALUES ({','.join(["%s"]*num_columns)}) """
                CEDW_dbConn.insertMultipleRows(insertStmt, dataframe.values.tolist())
                logging.info(f"Inserted into {table_name}")


        for table_name,dataframe in data.items():
            try:
                column_list = [x for x in dataframe.columns]
                num_columns = len(column_list)
                if num_columns > 0: 
                    insertStmt = f"""INSERT INTO {CEDW_dbConn.getDbName()}.{CEDW_dbConn.getDbOwner()}.{table_name}
                            ({','.join(column_list)}) VALUES ({','.join(["%s"]*num_columns)}) """
                    CEDW_dbConn.insertMultipleRows(insertStmt, dataframe.values.tolist())
                    logging.info(f"Inserted into {table_name}")
            except:
                logging.error(f"Unable to insert into {table_name}")
                continue

        ms.execute_sqlfile(CEDW_dbConn, 'sql/update_staging.sql',batch_run_id=filename)

        try:
            logging.info('Updating Unique / Foreign Keys in Staging')
            ms.execute_sqlfile(CEDW_dbConn, 'sql/update_staging.sql',batch_run_id=filename)
        except:
            logging.error('Unable to update unique and FK in Staging')
            continue
        
        try:
            logging.info('Moving data from Staging to Integrated')
            ms.execute_sqlfile(CEDW_dbConn, 'sql/stage_to_integrated.sql',batch_run_id=filename)
        except:
            logging.error('Unable to move data from Staging to Integrated')
            continue
        
        try:
            logging.info("Moving source file into archive directory")
            #This is the path for PROD, double check path for DEV as absolute path is different between the two
            os.rename(fullpath,f'extra_sample_data\processed_{filename_no_ext}_{process_time}')
        except:
            logging.warning("Unable to move source file into archive directory")
            continue
    
        
if __name__ == '__main__':
    main()