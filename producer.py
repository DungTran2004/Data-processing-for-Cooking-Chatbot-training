import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from kafka import KafkaProducer



config='localhost:9092'
topic_name='recipes_topic'

producer=KafkaProducer(
    bootstrap_servers=config,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')

)

def requests_general_link(page):
    try:
        link=f'https://www.cooky.vn/directory/search?q=null&st=2&lv=1,2,3&cs=2,4,5,6,7,8,9,11,12,17,18,19,20&cm=&dt=&igt=&oc=&p=&crs=1&page={page}&pageSize=12&append=true&video=false'
        general_response=requests.get(link)
        general_response.raise_for_status()
        raw_data=general_response.json()
        return raw_data
    except requests.RequestException as e:
        print(f"Lỗi khi lấy dữ liệu page {page}: {e}")
        return 


def get_raw_recipes_data(raw_data):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures_recipe=[executor.submit(process_to_send,recipe) for recipe in raw_data.get('recipes')]
        for future in as_completed(futures_recipe):
            try:
                future.result()
            except Exception as e:
                print(f"Lỗi trong quá trình xử lý recipe: {e}")


def comment_data(recipe_id):
    try:
        link=f'https://marketapi.cooky.vn/recipereview/v1.3/list/made?checksum=79f4d9dbaed44325563850c8b2c656bb&recipeId={recipe_id}'
        comment_response=requests.get(link)
        comment_json=comment_response.json()
        message={'comment':comment_json}
        return message
    
    except requests.RequestException as e:
        print(f'Lỗi khi lấy dữ liệu comment cho recipe ID {recipe_id}: {e}')


def process_to_send(data):
    with ThreadPoolExecutor(max_workers=5) as executor:
        id = data.get('Id')
        meta_title = data.get('MetaTitle')
        level = data.get('Level')
        time = data.get('TotalTime')
        other_info = {'id': id, 'meta_title': meta_title, 'level': level, 'time': time}
        try:
            link = f'https://marketapi.cooky.vn/recipe/v1.3/detail?checksum=1&id={id}'
            future_recipes_response = executor.submit(requests.get,link,params={})
            future_comment = executor.submit(comment_data, id)
            recipe_response=future_recipes_response.result()
            comment = future_comment.result()
            if comment and recipe_response.status_code==200:
                recipes_data = recipe_response.json()
                message = {'info': other_info, 'data': recipes_data, 'comment': comment}
                producer.send(topic_name, message)
                print(f'Đã gửi thực đơn id {id} tới Kafka')

        except requests.RequestException as e:
            print(f"Lỗi khi lấy dữ liệu cho recipe ID {id}: {e}")



if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(requests_general_link, page) for page in range(1, 3)]
        futures_process = []

        for future in as_completed(futures):
            raw_data = future.result()
            if raw_data:
                future_process = executor.submit(get_raw_recipes_data, raw_data)
                futures_process.append(future_process)

        for future in as_completed(futures_process):
            future.result()

