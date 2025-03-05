import os
import sys
import time
import logging
import mysql.connector
from dotenv import load_dotenv
from flask import Flask, request, abort
from apscheduler.schedulers.background import BackgroundScheduler
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi, TextMessage , PushMessageRequest

# โหลดตัวแปรจาก .env
load_dotenv()
LINE_CHANNEL_ACCESS_TOKEN = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
LINE_CHANNEL_SECRET = os.getenv('LINE_CHANNEL_SECRET')
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# กำหนด Logging
logging.basicConfig(level=logging.DEBUG)

# สร้าง Flask app
app = Flask(__name__)
handler = WebhookHandler(LINE_CHANNEL_SECRET)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)

def connect_db():
    try:
        return mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to database: {err}")
        return None

@app.route("/webhook", methods=["POST"])
def webhook():
    signature = request.headers.get("X-Line-Signature")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)

def handle_message(event):
    user_message = event.message.text.lower()
    if user_message == "test":
        try:
            check_alarms()
        except Exception as e:
            logging.error(f"Error in check alarm: {e}")
    elif user_message == "group id":
        try :
            if event.source.type == 'group':
                send_line_message(event.source.group_id, f"Group ID: {event.source.group_id} ")
                

        except Exception as e:
            logging.error(f"Error in sending group id: {e}")

def check_alarms():
    now = time.time() - (60 * 5)
    # print(time.ctime(now))
    conn = connect_db()
    if conn is None:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""SELECT DISTINCT p.product_id, cp.user_id, cp.linecode, cp.name, p.name
                        FROM company AS cp
                        JOIN product AS p ON cp.company_id = p.company_id
                        JOIN product_measurement AS pm ON p.product_id = pm.product_id
                        WHERE cp.linecode !='' AND p.deleted = '0' AND pm.line_alarm = 1""")
        users = cursor.fetchall()
        cursor.execute(f"""SELECT pm.name, pm.product_id , pm.measurement_id  ,pm.h1, pm.h2 , pm.l1, pm.l2
                        FROM company AS cp
                        JOIN product AS p ON cp.company_id = p.company_id
                        JOIN product_measurement AS pm ON p.product_id = pm.product_id
                        WHERE cp.linecode !='' AND p.deleted = '0' AND line_alarm = 1
                        ORDER BY pm.measurement_id ASC""")
        measurement_list = cursor.fetchall() 
        # สร้าง dict สำหรับเก็บข้อมูล โดยใช้ (product_id, measurement_id) เป็น key และ pm.name ,pm.h1, pm.h2 , pm.l1, pm.l2 เป็น value 
        measurement_dict = {   # pm.product_id , pm.measurement_id :: pm.name ,pm.h1, pm.h2 , pm.l1, pm.l2
        (measurement[1] , measurement[2] ) : {    
            'name': measurement[0],
            'h1': measurement[3],
            'h2': measurement[4],
            'l1': measurement[5],
            'l2': measurement[6]
        }
        for measurement in measurement_list
        }
        # for key, value in measurement_dict.items():
        #     print(key, value)

        if not users:
            logging.info("No users found.")
            return
        
        for user in users:
            message = f"📢 Alarm {user[3]} ({user[4]})\n({user[2]} {user[0]})\n"
            p_id = user[0]
            cursor.execute(f"""SELECT DISTINCT lu.measurement_id, lu.value, lu.alarm_type
                            FROM zpd{p_id}_lastupdate AS lu
                            JOIN product_measurement AS pm ON lu.measurement_id = pm.measurement_id
                            WHERE lu.alarm_type != '' AND lu.device_timestamp > {now} AND pm.line_alarm = 1""")
            alarms = cursor.fetchall()
            
            if not alarms:
                continue
            
            for al in alarms:
                alarm_type = al[2]
                measurement_data = measurement_dict.get((p_id, al[0]))  # ใช้ .get() ป้องกัน KeyError
                if not measurement_data:
                    continue  # ถ้าไม่มี key ตรงกัน ให้ข้าม iteration นี้ไป
                if alarm_type == 'h1':
                    message += f"⚠️High level 1: {measurement_dict[(p_id,al[0])]['name']} ({al[1]}) > {measurement_dict[(p_id,al[0])]['h1']} \n"
                elif alarm_type == 'h2':
                    message += f"🚨High level 2: {measurement_dict[(p_id,al[0])]['name']} ({al[1]}) > {measurement_dict[(p_id,al[0])]['h2']} \n"
                elif alarm_type == 'l1':
                    message += f"⚠️Low level 1: {measurement_dict[(p_id,al[0])]['name']} ({al[1]}) < {measurement_dict[(p_id,al[0])]['l1']} \n"
                elif alarm_type == 'l2':
                    message += f"🚨Low level 2: {measurement_dict[(p_id,al[0])]['name']} ({al[1]}) < {measurement_dict[(p_id,al[0])]['l2']} \n"

            if message:
                logging.info(f"Sending message to {user[2]} \n {message}")
                # send_line_message("Ca2c997a16b3d4b14b4b7a848576a622e", message)
                send_line_message(user[2], message) # ส่งข้อความไปยัง LINE ID ของผู้ใช้
    except mysql.connector.Error as err:
        logging.error(f"Error executing query: {err}")
    finally:
        conn.close()

def send_line_message(group_id, message):
    try:
        if not message:
            raise ValueError("Message cannot be empty")
        if not group_id:
            group_id = os.getenv("LINE_USER_ID")
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.push_message_with_http_info(
                PushMessageRequest(
                    to=group_id,
                    messages=[TextMessage(text=message)]
                )
            )
    except Exception as e:
        logging.error(f"Error sending message to LINE: {e}")

scheduler = BackgroundScheduler()
scheduler.add_job(func=check_alarms, trigger="interval", minutes=5)
scheduler.start()

if __name__ == "__main__":
    app.run(port=5000)