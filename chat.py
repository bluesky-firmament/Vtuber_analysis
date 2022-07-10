import matplotlib
import matplotlib.pyplot as plt

import pytchat
import time
import csv 
import pandas as pd
import numpy as np
import os
import datetime

debug_mode = 0 #1のときdebug用の出力




def main():
    print("読み込むcsvfileの名前を入力してください")
    load_csvname =  "./each_streamers_videoids/" + input() + ".csv"
    
    print("videoidからコメントを取得しますか? 取得する場合は1を入力してください")
    get_comment_mode = int(input())
    
    data_path = './data'
    comment_data_path = './comment_data'
    os.makedirs(data_path,exist_ok=True)
    os.makedirs(comment_data_path,exist_ok=True)


    video_id_lists = []
    video_name_lists = []
    video_time_lists = []
    video_state_lists = []
    video_id_lists = video_id_loads(video_id_lists,load_csvname)
    video_name_lists = video_name_loads(video_name_lists,load_csvname)
    video_state_lists = video_state_loads(video_state_lists,load_csvname)
    video_time_lists = video_duration_calculate(video_time_lists,load_csvname)
    # print(video_id_lists)
    # print(video_time_lists)
    video_comment_number = []
    video_comment_number_normalize = []
    unique_user_number = []
    iteration = 0


    if(get_comment_mode == 1): #videoidのリストを回して,各動画のコメントを取得する
        for iteration,video_id in enumerate(video_id_lists):
            if(video_state_lists[iteration] == "archive"):
                get_comment(video_id)
    

    for videoid in video_id_lists: #各コメントのデータからコメント数などの解析をする
        # print(videoid)
        comment_number = get_comment_number(videoid)
        video_comment_number.append(comment_number)
        if(comment_number != 0):
            video_comment_number_normalize.append(comment_number / video_time_lists[iteration])
        else:
            # continue
            video_comment_number_normalize.append(0)
        # print(video_comment_number)
        # print(video_comment_number_normalize)
        iteration = iteration + 1
    

    video_matrix = pd.merge(video_id_lists, video_name_lists, right_index=True, left_index=True)
    video_matrix = pd.merge(video_matrix, pd.DataFrame(video_time_lists,columns=['duration[s]']), right_index=True, left_index=True)
    video_matrix = pd.merge(video_matrix, pd.DataFrame(video_comment_number,columns=['comment number']), right_index=True, left_index=True)
    video_matrix = pd.merge(video_matrix, pd.DataFrame(video_comment_number_normalize,columns=['comment number normalize']), right_index=True, left_index=True)
    print(video_matrix)

    for iteration,video_id in enumerate(video_id_lists):
        if(video_state_lists[iteration] == "archive"):
            output_comment_users_list(video_id)
    # print(video_matrix)
    video_matrix.to_csv("matrix.csv")
    plot_normalize_dataframe(video_matrix)
    

def get_comment_number(csvname):
    try:
        csvname = "data/" + csvname + ".csv"
        df= pd.read_csv(csvname)
        return len(df)
    except:
        return 0

def plot_normalize_dataframe(video_matrix):
    plt.style.use('ggplot') 
    font = {'family' : 'meiryo'}
    matplotlib.rc('font', **font)
    plot_dataframe(video_matrix,"comment number normalize")
    plot_dataframe(video_matrix,"comment number")

def plot_dataframe(video_matrix,collum_name):
    ax = video_matrix.plot( y=[collum_name], bins=50, alpha=0.5, figsize=(16,4), kind='hist').figure
    ax.savefig(collum_name +"histogram.png")
    line_chart =video_matrix.plot( y=[collum_name], figsize=(16,4), alpha=0.5).figure
    line_chart.savefig(collum_name +"line.png")

def get_comment(video_id):
    # PytchatCoreオブジェクトの取得
    livechat = pytchat.create(video_id)# video_idはhttps://....watch?v=より後ろの
    csv_name = "data/" + video_id + ".csv"
    if(os.path.exists(csv_name)):
        print("this file is exist")
        return
    comment_matrix = []
    iteration = 0
    print(video_id)
    with open(csv_name, "w", encoding='utf-8',newline="") as file:
        while livechat.is_alive():
            # チャットデータの取得
            chatdata = livechat.get()   
            writer = csv.writer(file)
            for c in chatdata.items:
                Row = []
                Row.append(c.datetime)
                Row.append(c.author.name)
                Row.append(c.message)
                # print(f"{c.datetime} {c.author.name} {c.message} {c.amountString}")
                # print(c.json())
                writer.writerow(Row)
                
                # debug
                if (debug_mode == 1):
                    iteration = iteration + 1
                    if(iteration == 10):
                        break
            
            time.sleep(0.5)
            # break

def video_id_loads(video_id_lists,load_csvname):
    video_id_data = pd.read_csv(load_csvname)
    video_id_lists = video_id_data["id"]
    return video_id_lists

def video_name_loads(video_name_lists,load_csvname):
    video_id_data = pd.read_csv(load_csvname)
    video_name_lists = video_id_data["title"]
    return video_name_lists

def video_state_loads(video_state_lists,load_csvname):
    video_state_data = pd.read_csv(load_csvname)
    video_state_lists = video_state_data["state"]
    return video_state_lists


def video_duration_calculate(video_time_lists,load_csvname):
    video_id_data = pd.read_csv(load_csvname)
    # print(video_id_data)
    video_start = []
    video_end = []
    video_start = video_id_data["start"]
    video_end = video_id_data["end"]
    # print(type(video_end))
    video_duration = []
    for iteration in range(0,video_end.count()):
        begin = datetime.datetime.strptime(video_start[iteration], '%Y/%m/%d %H:%M:%S')
        end = datetime.datetime.strptime(video_end[iteration], '%Y/%m/%d %H:%M:%S')
        video_duration.append((end - begin).seconds)
    return video_duration

def output_comment_users_list(video_id):
    video_dataframe = pd.read_csv("data/" + video_id + ".csv", names=('time', 'user_name', 'comment'))
    video_dataframe["user_name"].value_counts().to_csv("./comment_data/" + video_id + ".csv")

if __name__ == "__main__":
    main()

