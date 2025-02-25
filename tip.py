import json
from bs4 import BeautifulSoup
import requests
import re

link='https://www.nhahangquangon.com/meo-vat-nau-an-257-meo-vat-che-bien-am-thuc-can-biet/'
response=requests.get(link)
bs=BeautifulSoup(response.content,'html.parser')
a=bs.find_all('p',attrs={'style':'text-align: justify;'})
title_pattern = re.compile(r"^\d+\.\s")


data = []          # List chứa các dict, mỗi dict có key 'mẹo' và value là chuỗi nội dung
current_content = []  # Danh sách để gom nội dung của nhóm hiện tại

for line in a:
    text = line.text.strip()
    # Nếu gặp một dòng bắt đầu bằng số: bắt đầu một nhóm mới
    if title_pattern.match(text):
        # Nếu current_content không rỗng, lưu nhóm trước đó vào data
        if current_content:
            # Ghép nội dung của nhóm trước và lưu vào dict với key 'mẹo'
            data.append({'mẹo': "".join(current_content)})
            current_content = []
    # Thêm dòng hiện tại vào current_content
    current_content.append(text+' ')

# Sau vòng lặp, lưu nhóm cuối nếu có nội dung
if current_content:
    data.append({'mẹo': "".join(current_content)})

# Lưu mục cuối cùng vào dictionary



with open('C:/Users/Admin/PycharmProjects/Data/Chatbot_Data/tip.jsonl','a',encoding='utf-8') as f:
    for datas in data:
        f.write(json.dumps(datas, ensure_ascii=False) + "\n")


