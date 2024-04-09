import pandas as pd
import os
import pymongo


def duplication():
    folder_path = '../data_file'
    csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]
    # 假设您的CSV文件名列表是csv_files

    # 读取所有CSV文件并将它们合并成一个DataFrame
    df_list = [pd.read_csv(file, encoding='latin1') for file in csv_files]
    # combined_df = pd.concat(df_list, ignore_index=True)
    #
    # # 找出重复的title
    # grouped = combined_df.groupby('title')
    #
    # # 筛选出组中行数大于1的组
    # filtered_groups = grouped.filter(lambda x: len(x) > 1)
    # data = filtered_groups.to_dict('records')
    combined_df = pd.concat(df_list, ignore_index=True)

    # 根据'title'列的值分组，并将每个组转换为一个DataFrame
    grouped = combined_df.groupby('title')
    groups_with_multiple_rows = grouped.filter(lambda x: len(x) > 1)
    table_group_by_title = groups_with_multiple_rows.groupby("title")
    new_df = table_group_by_title.apply(lambda df: df).reset_index(drop=True)
    data_to_insert = new_df.to_dict('records')
    collection = pymongo.MongoClient()["thesis_data"]["international_movie_record"]
    collection.insert_many(data_to_insert)
    # collection.insert_many(data)
    # grouped_dfs = {title: group_df for title, group_df in grouped}
    # for title, group_df in grouped_dfs.items():
    #     collection = db[title]  # 您可以为每个title创建一个新的集合
    #     data_to_insert = group_df.to_dict('records')
    #     collection.insert_many(data_to_insert)
    # collection = pymongo.MongoClient()["thesis_data"]["international_movie_record"]
    # collection.insert_many(data)
    # 显示重复的title


duplication()