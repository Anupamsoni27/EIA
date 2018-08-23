import pandas as pd
# df_input = pd.read_csv("import,export & movement-Landed cost of imported crude oil-by area.csv")
#
# for i in range(len(df_input.columns)):
#     if i  % 2 == 0:
#        # print(df_input.iloc[0:11,  i:i+2])
#        for index, row in df_input.iloc[0:11,  i:i+2].itterrows():
#             print(index)
#
#
#
# df_data = pd.read_csv("EIA_MODEL_DATA.csv")
# # for a  in df_data.groupby("Model check code"):
# #     print(a)
# from dateutil.parser import parse
#
# def is_date(string):
#     try:
#         parse(string)
#         return True
#     except ValueError:
#         return False
# print(is_date("1990-12-1"))
# if is_date("1990-12-1") == True:
#     print('TTTTTT')
from builtins import print
global data

import pymssql

query_part_1 = """SELECT m.URI FROM LIB_MODEL m where m.uri like '%EIA%'"""
query_part_2 = ""
global dhub_1_list
global dhub_2_list
global all_list
dhub_1_list = []
dhub_2_list = []
all_list = []
query = query_part_1

svr = "DHUB2"
server_name = "10.0.9.97"
user_name = "gdm"
pswd = "gdm"
database_name = "GDM"
conn = pymssql.connect(server=server_name, user=user_name, password=pswd, database=database_name)
cursor = conn.cursor()

# script = "select DISTINCT m.category,m.uri from lib_model m where m.code like'%.[0-9][0-9][0-9][0-9]M%' and m.category like'%REL'"
script = query
# print(script)o
cursor.execute(script)

data = cursor.fetchall()
temp_df = pd.DataFrame(data)
temp_df.to_csv("All_EIA_models.csv")
print(type(data))
