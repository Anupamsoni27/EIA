# import pandas as pd
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
list = ["ab", "abc"]
if "a" not in list:
    print("aaaaaaa")