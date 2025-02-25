from bs4 import BeautifulSoup
import requests
import json

link='https://kodoshi.vn/bang-thanh-phan-dinh-duong-cua-mot-so-thuc-pham-pho-bien-tai-viet-nam/'
response=requests.get(link)
bs=BeautifulSoup(response.content,'html.parser')

nutrition1=bs.find_all('table')[0]
nutrition2=bs.find_all('table')[1]


nutrion_data=[]
for x in nutrition1.find_all('tr',attrs={'style':"height: 46px;"})[1:]:
    Glucid=x.find_all('td')[1].text.strip()
    Xo=x.find_all('td')[2].text.strip()
    Lipid=x.find_all('td')[3].text.strip()
    Protein=x.find_all('td')[4].text.strip()
    Calo=x.find_all('td')[5].text.strip()
    data={f"{x.find_all('td')[0].text.strip()}":{'glucid':Glucid,'xo':Xo,'lipid':Lipid,'protein':Protein,'calo':Calo}}
    nutrion_data.append(data)

for y in nutrition2.find_all('tr')[2:]:
    glucid2=y.find_all('td')[1].text.strip()
    lipid2=y.find_all('td')[2].text.strip()
    protein2=y.find_all('td')[3].text.strip()
    calo2=y.find_all('td')[4].text.strip()
    data={f"{y.find_all('td')[0].text.strip()}":{'glucid':glucid2,'lipid':lipid2,'protein':protein2,'calo':calo2}}
    nutrion_data.append(data)

def save_to_json(data):
    with open('C:/Users/Admin/OneDrive/Chatbot_Data/nutrition.json','a',encoding='utf-8') as f:
        json.dump(data,f,ensure_ascii=False,separators=(',', ':'))
        f.write('\n')
for x in nutrion_data:
    save_to_json(x)