import requests
from dateutil.parser import parse
import xml.etree.ElementTree as et
import requests
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import *
import json as js
class ICIS_Validator:


    def __init__(self, name):
        self.name = name

    def getinput(self):
        global path
        path = "C://Users//anupam.soni//PycharmProjects//EIA//"

    def make_global(self):
        global server
        global user_name
        global pswd
        global database_name

    def model_call(self):
        from builtins import print
        global data

        import pymssql

        query_part_1 = """SELECT m.URI, p.NAME FROM LIB_MODEL m  JOIN LIB_PROFILE p ON m.ID=p.model_id where
m.category = '""" + dataset_name + """' and p.name=m.default_attribute;"""
        query_part_2 = ""
        global dhub_1_list
        global dhub_2_list
        global all_list
        dhub_1_list = []
        dhub_2_list = []
        all_list = []
        query = query_part_1

        # svr = "DHUB2"
        # server_name = "10.0.9.97"
        # user_name = "gdm"
        # pswd = "gdm"
        # database_name = "GDM"
        conn = pymssql.connect(server=server_name, user=user_name, password=pswd, database=database_name)
        cursor = conn.cursor()

        # script = "select DISTINCT m.category,m.uri from lib_model m where m.code like'%.[0-9][0-9][0-9][0-9]M%' and m.category like'%REL'"
        script = query
        # print(script)o
        cursor.execute(script)

        data = cursor.fetchall()
        # print(data)
        # for d in data:
        #     print(d)

    def model_checks(self):
        global invalid_models
        invalid_models = []
        for temp_model in data:
            if " " in temp_model[0]:
                invalid_models.append(temp_model[0])
                print(temp_model[0])
        temp_df = pd.DataFrame(invalid_models)
        temp_df.to_csv( path +"output//Invalid_models_"+dataset_name+".csv")

    def data_call(self):
        global final_df1
        def divide_chunks(l, n):
            print("running data_call")

            # looping till length l
            for i in range(0, len(l), n):
                yield l[i:i + n]

        # How many elements each
        # list should have
        n = 100

        chunks = list(divide_chunks(data, n))
        final_df1 = pd.DataFrame()
        for chunk in chunks:
            line = ""
            for aa in chunk:
                line = line +  "<dat:JavaLangstring>" + aa[0] + "/" + aa[1] +"/ALL</dat:JavaLangstring>"
            print(line)
            final_df = pd.DataFrame()

            # datetimeObject = datetime.strptime(start_date,"%d-%m-%Y")
            # new_start_date =datetimeObject.strftime("%Y-%m-%d")
            # datetimeObject = datetime.strptime(end_date,"%d-%m-%Y")
            # new_end_date =datetimeObject.strftime("%Y-%m-%d")
            # print(new_start_date,new_end_date)
            # a=pd.DatetimeIndex(start=new_start_date,end=new_end_date, freq=BDay())

            url = "http://10.0.9.61:80/gdm/DataActionsService"
            headers = {'content-type': 'application/soap+xml'}
            # headers = {'content-type': 'text/xml'}
            body1 = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:dat="http://www.datagenicgroup.com">
            <soapenv:Header>
                  <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                     <wsse:UsernameToken wsu:Id="UsernameToken-902788241" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                        <wsse:Username>adminuser@domain</wsse:Username>
                        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">adminuser@domain</wsse:Password>
                     </wsse:UsernameToken>
                  </wsse:Security>
                  <ns1:Client soapenv:actor="http://schemas.xmlsoap.org/soap/actor/next" soapenv:mustUnderstand="0" xmlns:ns1="gdm:http://www.datagenicgroup.com">
                     <ns1:ApplicationType>JavaClient</ns1:ApplicationType>
                  </ns1:Client>
               </soapenv:Header>
               <soapenv:Body>
                  <dat:getGenicDatas>
                     <dat:uris>"""

            body3 = """</dat:uris>
                     <dat:rangeUri>range://default/default</dat:rangeUri>
                  </dat:getGenicDatas>
               </soapenv:Body>
            </soapenv:Envelope>
            """
            body2 = line
            body = body1 + body2 + body3
            response = requests.post(url, data=body,headers=headers)  # ,username="adminuser@domain",password="adminuser@domain",)
            r = response.text
            # print(r)

            with open('GDMResponse.xml', 'w')as f:
                f.write(r)
            tree = et.ElementTree(et.fromstring(response.text))
            root = tree.getroot()
            for element in root:
                if element.tag == "{http://schemas.xmlsoap.org/soap/envelope/}Body":
                    for ns4 in element:
                        if ns4.tag == "{http://www.datagenicgroup.com}getGenicDatasResponse":
                            for ns4_temp in ns4:
                                if ns4_temp.tag == "{http://www.datagenicgroup.com}return":
                                    for ns4_temp_2 in ns4_temp:
                                        if ns4_temp_2.tag == "{http://www.datagenicgroup.com}GenicData":
                                            FullRangeUri = ""
                                            temp_model_data = []
                                            for genicData in ns4_temp_2:

                                                if genicData.tag == "{java:com.datagenicgroup.data}NumericSeries":
                                                    for properties in genicData:

                                                        for property in properties:
                                                            # print(property.text)
                                                            if property.text == "FullRangeUri":
                                                                for property in properties:
                                                                    # print(property.tag)
                                                                    if property.tag == "{java:com.datagenicgroup.data}Value":
                                                                        # print(property.text)
                                                                        FullRangeUri = property.text
                                                            if property.text == "ModelName":
                                                                for property in properties:
                                                                    if property.tag == "{java:com.datagenicgroup.data}Value":
                                                                        ModelName = property.text
                                                            if property.text == "ModelDescription":
                                                                for property in properties:
                                                                    if property.tag == "{java:com.datagenicgroup.data}Value":
                                                                        ModelDescription = property.text
                                                            if property.text == "ModelUri":
                                                                for property in properties:
                                                                    if property.tag == "{java:com.datagenicgroup.data}Value":
                                                                        ModelUri = property.text
                                                                        # print(ModelUri)
                                                        if properties.tag == "{java:com.datagenicgroup.data}Values":
                                                            temp_model_data.append(properties.text)
                                                            # print(properties.text)
                                            print(temp_model_data, ModelUri, len(str(ModelUri)))
                                            if len(temp_model_data) > 0 and len(str(ModelUri))>0:
                                                if ModelUri.split("/")[3].endswith("_M"):

                                                    start_date = "01-" + FullRangeUri.split("/")[-2]
                                                    end_date = "01-" + FullRangeUri.split("/")[-1]
                                                    datetimeObject = datetime.strptime(start_date, "%d-%m-%Y")
                                                    new_start_date = datetimeObject.strftime("%Y-%m-%d")
                                                    datetimeObject = datetime.strptime(end_date, "%d-%m-%Y")
                                                    new_end_date = datetimeObject.strftime("%Y-%m-%d")
                                                    a = pd.DatetimeIndex(start=new_start_date, end=new_end_date, freq=MonthBegin())

                                                    temp_datelist = []
                                                    for temp_date in a:
                                                        temp_datelist.append(temp_date)

                                                if ModelUri.split("/")[3].endswith("_A"):
                                                    start_date = "01-" + FullRangeUri.split("/")[-2]
                                                    end_date = "01-" + FullRangeUri.split("/")[-1]
                                                    datetimeObject = datetime.strptime(start_date, "%d-%m-%Y")
                                                    new_start_date = datetimeObject.strftime("%Y-%m-%d")
                                                    datetimeObject = datetime.strptime(end_date, "%d-%m-%Y")
                                                    new_end_date = datetimeObject.strftime("%Y-%m-%d")
                                                    a = pd.DatetimeIndex(start=new_start_date, end=new_end_date, freq=YearBegin())
                                                    # print(a)
                                                    temp_datelist = []
                                                    for temp_date in a:
                                                        temp_datelist.append(temp_date)
                                            print(ModelUri)
                                            print(FullRangeUri)
                                            print(len(temp_datelist))
                                            print(len(temp_model_data))
                                            # print("######################33")
                                            df = pd.DataFrame()
                                            #
                                            # print(len(temp_datelist))
                                            # print(len(temp_model_data))
                                            temp_datelist = temp_datelist
                                            temp_model_data = temp_model_data
                                            if len(temp_model_data) == len(temp_datelist):
                                                df["Date"] = temp_datelist
                                                df["value"] = temp_model_data
                                                df["Model Code"] = ModelUri
                                                df["Model check code"] = ModelUri.split("/")[3].split(".")[-1].replace("_",".")
                                                df.sort_values('Date', ascending=False)
                                                # df.append(pd.DataFrame(temp_datelist))
                                                # df.append(pd.DataFrame(temp_model_data))
                                                # print(df)
                                                final_df = final_df.append(df)
                                                # print(final_df.head(10))
                                                # print("###################################################################################################################################")

            final_df1 = final_df1.append(final_df)
        final_df1.to_csv(path + "EIA_MODEL_DATA.csv", index=False)

    def input_modifier(self):
        def is_date(string):
            try:
                parse(string)
                return True
            except ValueError:
                return False


        global temp_db
        temp_db = pd.DataFrame()

        global df_datafile
        df_datafile = pd.DataFrame()
        # df_datafile =
        df_input = pd.read_csv(path + "input/"+eia_data_file_name+".csv")

        for i in range(len(df_input.columns)):
            if i % 2 == 0:
                temp_db = df_input.iloc[0:, i:i + 2]

                column_list = temp_db.columns
                temp_db["Model code"] = column_list[0]
                temp_db.columns = ["date","value", "model code"]
                # print(temp_db)

                for index, row1 in temp_db.iterrows():
                    try:
                        if "Units" in row1["date"]:
                            unit = row1["value"]
                            temp_db["Unit"] = unit
                            desc = temp_db.iloc[3]['date']
                            temp_db["Description"] = desc
                        if is_date(str(row1["date"])) == True and index <6:
                           start_date = row1["value"].split(" ")[0]
                           end_date = row1["value"].split(" ")[2]
                           print(row1["value"])
                           temp_db["Start date"] = start_date
                           temp_db["End date"] = end_date
                           temp_db=  temp_db.loc[6:,["date","value","model code","Unit","Description","Start date","End date"] ]
                    except:pass
                df_datafile = pd.concat([df_datafile,temp_db])

        # df_input = df_input.append(temp_db)
        df_datafile.to_csv(path + "output.csv",index=False)

    def Validation(self):
        df_data_file = final_df1
        df_input_file  = df_datafile
        temp_model_list =list(set(list( df_input_file["model code"].apply(lambda x: x.split('.')[1]))))
        hub_models = list(set(list(df_data_file["Model check code"].apply(lambda x: x.split('.')[0]))))
        print(type(hub_models))
        for model in temp_model_list:
            if model in hub_models:
                # print(model)
                for group in df_data_file.groupby("Model check code"):
                    # print(group)
                    pass



            else:print(model + " is missing in GDM")

    def getmodeldatafromgdm(self):
        global df_getmodeldatafromgdm
        global data

        import pymssql

        query_part_1 = """select * from LIB_MODEL m where m.category like '"""+ dataset_name +"""'"""
        query_part_2 = ""
        global dhub_1_list
        global dhub_2_list
        global all_list
        dhub_1_list = []
        dhub_2_list = []
        all_list = []
        query = query_part_1

        # svr = "DHUB2"
        # server_name = "10.0.9.97"
        # user_name = "gdm"
        # pswd = "gdm"
        # database_name = "GDM"
        conn = pymssql.connect(server=server_name, user=user_name, password=pswd, database=database_name)
        cursor = conn.cursor()

        # script = "select DISTINCT m.category,m.uri from lib_model m where m.code like'%.[0-9][0-9][0-9][0-9]M%' and m.category like'%REL'"
        script = query
        # print(script)
        cursor.execute(script)

        data = cursor.fetchall()
        df_getmodeldatafromgdm = pd.DataFrame(data, columns=["ID","NAME","DESCRIPTION","MODEL_TYPE","CATEGORY","CODE","DEFAULT_ATTRIBUTE"," IS_TEMPLATE"," TEMPLATE_ID","STATUS","DATASET_PACKAGE_ID","URI"])
        df_getmodeldatafromgdm.to_csv(path + "getmodeldatafromgdm.csv",index=False)

    def description_check(self):
        temp_input_df = pd.DataFrame()
        temp_input_df["model code"] = df_datafile["model code"]
        temp_input_df["Description"] = df_datafile["Description"]
        temp_input_df = temp_input_df.drop_duplicates()
        # print(temp_input_df)
        temp_gdm_df = pd.DataFrame()
        temp_gdm_df["NAME"] = df_getmodeldatafromgdm["NAME"]
        temp_gdm_df["DESCRIPTION"] = df_getmodeldatafromgdm["DESCRIPTION"]
        temp_gdm_df = temp_gdm_df.drop_duplicates()
        # print(temp_gdm_df)
        temp_list = []
        for index,row in temp_input_df.iterrows():
            model_found = 0
            temp_input_model_code = row["model code"]
            temp_input_model_desc = row["Description"]
            for index,row2 in temp_gdm_df.iterrows():
                # print(row2)
                # print(row2[0])
                temp_gdm_model_code = row2[0]
                temp_gdm_model_desc = row2[1]
                # print(temp_input_model_code,temp_gdm_model_code)
                if temp_input_model_code.split('.')[-2] in temp_gdm_model_code and temp_input_model_code.split('.')[-1] == temp_gdm_model_code.split('_')[-1]:
                    model_found =1
                    print(temp_gdm_model_code,temp_gdm_model_desc)
                    print(temp_input_model_code,temp_input_model_desc)
                    temp_row = temp_input_model_code,temp_gdm_model_code,temp_input_model_desc,temp_gdm_model_desc,bool(temp_input_model_desc.split(",")[0] in temp_gdm_model_desc)
                    # print(temp_row)
                    temp_list.append(temp_row)
        df = pd.DataFrame(temp_list,columns=["EIA MODEL CODE","GDM MODEL CODE", "EIA MODEL DESCRIPTION","GDM MODEL DESCRPTION","OBSERVATION" ])
        df = df.drop_duplicates()
        df.to_csv(path + "output/Description_Check_Repot.csv", index = False)

    def getmodelrangefromgdm(self):
        global df_getmodelrangefromgdm
        global data

        import pymssql

        query_part_2 = ""
        global dhub_1_list
        global dhub_2_list
        global all_list
        dhub_1_list = []
        dhub_2_list = []
        all_list = []
        query = "select m.code , v.range_uri , m.default_attribute  from LIB_MODEL m join LIB_PROFILE p on m.id = p.model_id join LIB_VERSION V ON P.ID=V.PROFILE_ID WHERE M.CATEGORY = '"+dataset_name+"'"

        # svr = "DHUB2"
        # server_name = "10.0.9.97"
        # user_name = "gdm"
        # pswd = "gdm"
        # database_name = "GDM"
        conn = pymssql.connect(server=server_name, user=user_name, password=pswd, database=database_name)
        cursor = conn.cursor()

        # script = "select DISTINCT m.category,m.uri from lib_model m where m.code like'%.[0-9][0-9][0-9][0-9]M%' and m.category like'%REL'"
        script = query
        # print(script)
        cursor.execute(script)

        data = cursor.fetchall()
        df_getmodelrangefromgdm = pd.DataFrame(data,
                                              columns=["MODEL_CODE","RANGE_URI","DEFAULT_ATTRIBUTE"])
        df_getmodelrangefromgdm.to_csv( path + "getmodelrangefromgdm.csv", index=False)

    def range_checks(self):
        temp_list = []
        global temp_df
        global temp_input_df
        global temp_gdm_df
        temp_gdm_df = pd.DataFrame()
        temp_gdm_df = df_getmodelrangefromgdm
        temp_gdm_df["Period"] = df_getmodelrangefromgdm["MODEL_CODE"].apply(lambda x: x.split('_')[-1])
        temp_gdm_df["Start_date"] = "01-" + df_getmodelrangefromgdm["RANGE_URI"].apply(lambda x: x.split('/')[-2])
        temp_gdm_df["End_date"] = "01-" +df_getmodelrangefromgdm["RANGE_URI"].apply(lambda x: x.split('/')[-1])
        # print(temp_gdm_df.head())
        temp_input_df = pd.DataFrame()
        temp_input_df["model code"] = df_datafile["model code"]
        temp_input_df["Period"] = df_datafile["model code"].apply(lambda x: x.split(".")[-1])
        temp_input_df["Start_date"] = df_datafile["Start date"]
        temp_input_df["End_date"] = df_datafile["End date"]
        temp_input_df = temp_input_df.drop_duplicates()
        print(temp_input_df.head())
        for index, row  in temp_input_df.iterrows():
            temp_input_model_code = row["model code"].split('.')[-2]
            temp_input_start_date = row["Start_date"]
            datetimeObject = datetime.strptime(temp_input_start_date, "%Y-%m-%d")
            temp_input_start_date = datetimeObject.strftime("%d-%m-%Y")
            temp_input_end_date = row["End_date"]
            datetimeObject = datetime.strptime(temp_input_end_date, "%Y-%m-%d")
            temp_input_end_date = datetimeObject.strftime("%d-%m-%Y")

            temp_input_period = row["Period"]
            for index,row1 in temp_gdm_df.iterrows():
                temp_gdm_model_code = row1["MODEL_CODE"].split(".")[-1].split("_")[0]
                temp_gdm_start_date = row1["Start_date"]
                temp_gdm_end_date = row1["End_date"]
                temp_gdm_period = row1["Period"]
                # print(temp_input_model_code,temp_gdm_model_code)
                if temp_input_model_code == temp_gdm_model_code and temp_input_period == temp_gdm_period:
                    temp_row = row["model code"], row1["MODEL_CODE"],temp_input_start_date+" to " +temp_input_end_date,temp_gdm_start_date+" to " +temp_gdm_end_date, bool(temp_input_start_date+" to " +temp_input_end_date == temp_gdm_start_date+" to " +temp_gdm_end_date)
                    # print(temp_row)
                    temp_list.append(temp_row)
            df = pd.DataFrame(temp_list, columns=["EIA MODEL CODE", "GDM MODEL CODE", "EIA MODEL RANGE",
                                                      "GDM MODEL RANGE","OBSERVATION"])
            df = df.drop_duplicates()
            df.to_csv(path + "output/Range_Check_Repot.csv", index=False)

    def property_check(self):
        temp_df_list = []
        df = pd.read_csv(path + "PRODUCT.csv")
        temp_list = list(df["Column-A"])
        # print(temp_list)
        import pymssql
        # svr = "DHUB2"
        # server_name = "10.0.9.97"
        # user_name = "gdm"
        # pswd = "gdm"
        # database_name = "GDM"

        conn = pymssql.connect(server=server_name, user=user_name, password=pswd, database=database_name)
        cursor = conn.cursor()
        script = "Select m.category, m.uri, e.MARKETBASIS, e.PRODUCT from LIB_MODEL m join  LDT_EIA_US e on m.id = e.gpid where m.category = '"+dataset_name+"'"
        print(script)
        cursor.execute(script)
        data11 = cursor.fetchall()
        df = pd.DataFrame(data11,columns=["Category","Uri","Marketbasis","Product"])
        for index, row in df.iterrows():
            # print(row[3])
            # print(row[3])
            if row[3] not in temp_list:
                print(row[3])
                temp_df_list.append(row)
        temp_df = pd.DataFrame(temp_df_list)
        temp_df.to_csv(path+ 'output/Property_check.csv', index=False)

dbObj = ICIS_Validator("Connect MS SQL")
dataset_name = input("Enter dataset category name...")
eia_data_file_name = input("Enter EIA data file name...")
server = input("Enter server name...")
if server == "DHUB1":
    server_name = "10.0.9.97"
    user_name = "gdm"
    pswd = "gdm"
    database_name = "GDM"
elif server == "DHUB2":
    server_name = "10.0.9.97"
    user_name = "gdm"
    pswd = "gdm"
    database_name = "GDM"
elif server == "UAT-61":
    server_name = "DGUKENIDB01"
    user_name = "GDM_UAT"
    pswd = "GDM_UAT"
    database_name = "GDM_UAT"
# dataset_name = "EIA_CRLCPAGR"
dbObj.make_global()
dbObj.getinput()
dbObj.model_call()
dbObj.data_call()
dbObj.input_modifier()
dbObj.Validation()
dbObj.getmodeldatafromgdm()
dbObj.getmodelrangefromgdm()
dbObj.description_check()
dbObj.range_checks()
dbObj.property_check()
dbObj.model_checks()