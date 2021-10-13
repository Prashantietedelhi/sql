# MS SQL library
# Authors: <Singh, Prashant <Prashant.Singh@careerbuilder.com>>
'''
MS SQL library
e.x:
'''
# encoding: utf-8
# MSSQL library
# Authors: <Singh, Prashant <Prashant.Singh@careerbuilder.com>>

import sys,os,configparser
import pymssql
curpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
sys.path.insert(0,curpath)

# try:
#     from get_logger import GetLogger
# except:
from common.get_logger import GetLogger


####################### Config file reading
config_file_loc = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..","..","config","config.cfg")
config_obj = configparser.ConfigParser()

try:
    config_obj.read(config_file_loc)
    debugLevel = int(config_obj.get("MSSQL","debuglevel"))
    logfilename = config_obj.get("MSSQL","logfilename")
    server = config_obj.get("MSSQL","server")
    username = config_obj.get("MSSQL","username")
    password = config_obj.get("MSSQL","password")
    database = config_obj.get("MSSQL","database")
except Exception as e:
    raise Exception("Config file reading error: "+str(e))


####################### Logging Functionality
logfilename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..","..","logs",logfilename)
loggerobj = GetLogger("MSSQL",logfilename,debugLevel)
logger = loggerobj.getlogger()

logger.info("MSSQL Library")




class MSSQL():
    def __init__(self):
        try:
            self.connection = pymssql.connect(server,
                                              username,
                                               password,
                                              database)
        except Exception as e:
            logger.error("Failed to create the connection :"+str(e))
            raise Exception("Failed to create the connection :"+str(e))
        if self.connection==None:
            logger.error("Failed to create the connection :" + str(e))
            raise Exception("Failed to create the connection :" + str(e))

    def getCursor(self):
        try:
            cursor = self.connection.cursor()
        except Exception as e:
            logger.error("Failed to get the cursor: "+str(e))
            raise Exception("Failed to get the cursor: "+str(e))
        return cursor

    def executeQuery(self, cursor, query):
        try:
            cursor.execute(query)
        except Exception as e:
            logger.error("Failed to execute the query: "+str(query)+" REASON: " + str(e))
            raise Exception("Failed to execute the query: "+str(query)+" REASON: " + str(e))

    def fetchone(self, cursor):
        res = cursor.fetchone()
        return res

    def fetchall(self, cursor):
        res = cursor.fetchall()
        return res

    def close(self):
        self.connection.close()

    def fetchn(self,cursor, n):
        res = []
        for i in range(n):
            res.append(cursor.fetchone())
        return res
if __name__ == "__main__":
    msobj = MSSQL()
#     cur = (msobj.getCursor())
#     q='''SELECT EA.CompanyID, gender,employeeage,grossannualsalary, employeestatus,MaritalStatus,SpouseRelationShip,ChildCount,EA.PlandesignID,BenefitTypeID,PTI.BenefitTypeIdentifier,PA.PlanTypeIdentifier
# FROM EmployeeAndEnrollmentDetails_All1 EA
# INNER JOIN PlanCustomizationDetails_All1 PA ON PA.PlanDesignID = EA.PlanDesignID AND EA.CompanyID = PA.CompanyID
# INNER Join PlanTypeIdentifier PTI on PTI.PlanTypeIdentifier = PA.PlanTypeIdentifier'''
#     msobj.executeQuery(cur, q)
#     print(msobj.fetchn(cur,20))
#     msobj.close()