from concurrent.futures.thread import ThreadPoolExecutor
from kafka import KafkaConsumer
import json
from concurrent.futures import as_completed
from bs4 import BeautifulSoup
config = 'localhost:9092'
topic_name = 'recipes_topic'
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=config,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=10000
)

def description_clean(raw_description):
    soup = BeautifulSoup(raw_description, "html.parser")
    description_text = soup.get_text()
    return  description_text

def transform_data(data):
    try:
        data = data.value
        info = data.get('info')
        recipe_detail = data.get('data').get('data')
        avgrating = recipe_detail.get('avgRating')
        servings = recipe_detail.get('servings')
        totalrating = recipe_detail.get('totalRating')
        ingredients = recipe_detail.get("ingredients", [])
        steps = recipe_detail.get("steps", [])
        recipe_name = recipe_detail.get('name')
        description = recipe_detail.get('description')
        cleaning_description=description_clean(description)
        ingredient_list = []
        steps_data = []
        for ingredient in ingredients:
            unit = ingredient.get('unit')
            name = ingredient.get('name')
            quantity = ingredient.get('quantity')
            new_unit = unit.copy()
            new_unit.update({"name": name, "quantity": quantity})
            ingredient_list.append(new_unit)
        for number, step_data in enumerate(steps):
            content = step_data.get('content')
            steps_data.append(f'Bước {number + 1}: ' + content)
        comment_data=data.get('comment').get('data')
        if comment_data:
            for comment_content in comment_data:
                comment=comment_content.get('content')
        else:
            comment='Hiện chưa có bình luận về món ăn này, hãy để lại bình luận của bạn, cho chúng tôi biết cảm nhận của bạn về món ăn này'


        return {'Tên':recipe_name,'Thông tin': info,'Mô tả':cleaning_description, 'Đánh giá trung bình': avgrating, 'Khẩu phần ăn': servings, 'Tổng đánh giá': totalrating,
                'Nguyên liêu': ingredient_list, 'Các bước thực hiện': steps_data,'Bình luận':comment}
    except Exception as e:
        print(f"Lỗi khi xử lý data: {e}")
        return None

def save_to_json(data):
    with open('C:/Users\Admin\PycharmProjects\Data\Chatbot_Data\cooking_data.jsonl', 'a', encoding='utf-8') as f:
        json.dump(data,f,ensure_ascii=False,separators=(',', ':'))
        f.write('\n')

def process_batch(batch):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(transform_data, data) for data in batch]
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                save_to_json(result)

def send_to_transform(consumer, batch_size=20):
    batch = []
    for data in consumer:
        batch.append(data)
        if len(batch) >= batch_size:
            process_batch(batch)
            batch = []
    if batch:
        process_batch(batch)


# def save_to_json(data):
#    with open('C:/Users\Admin\PycharmProjects\Data\Chatbot_Data\cooking_data.json','a',encoding='utf-8') as f:
#            json.dump(data.value,f,ensure_ascii=False,separators=(',', ':'))
#            f.write('\n')

if __name__ == '__main__':
    send_to_transform(consumer)
    # for data in consumer:
    #     save_to_json(data)
