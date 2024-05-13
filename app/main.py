import random
import asyncio
from datetime import datetime,time
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from paho.mqtt import client as mqtt_client
from dotenv import dotenv_values

from database import pg_async_session
from publish.accumulate import AccumulateETL
from manager.mqtt import MqttManager

config = dotenv_values(".env")
scheduled_times = [
    "07:35:00",
    "08:30:00",
    "09:40:00",
    "10:30:00",
    "11:30:00",
    "11:47:00",
    "11:48:00",
    "11:49:00",
    "11:50:00",
    "11:51:00",
    "11:52:00",
    "13:30:00",
    "14:40:00",
    "15:30:00",
    "16:30:00",
    "17:50:00",
    "19:20:00",
    "20:30:00",
    "21:30:00",
    "22:30:00",
    "23:30:00",
    "01:30:00",
    "02:30:00",
    "03:30:00",
    "04:30:00",
    "05:50:00",
    "07:20:00",
]
current_datetime = datetime.now()

# Loop through scheduled times to find the next scheduled time
for scheduled_time_str in scheduled_times:
    scheduled_time = time.fromisoformat(scheduled_time_str)
    scheduled_datetime = datetime.combine(current_datetime.date(), scheduled_time)

    # Check if the scheduled time is in the future
    if scheduled_datetime > current_datetime:
        break

# Use the scheduled_datetime for precision
current_date = scheduled_datetime.strftime("%Y-%m-%d %H:%M:%S")


def sub_on_message_func(topic, payload):
    if "energy" in topic:
        publish.power_receive(topic, payload)


def pub_on_connect_func(manager):
    publish.set_pub_client(manager)


# sub_broker = config["SUB_BROKER"]
# sub_port = int(config["SUB_PORT"])
sub_topics = "263315ab48dd4982971f157cd97faa4a/rotor/linenotify"
sub_client_id = f"subscribe-{random.randint(0, 100)}"
sub_mqtt = MqttManager(
    broker="broker.emqx.io",
    port=8083,
    sub_topics=sub_topics,
    pub_topics=None,
    client_id=sub_client_id,
    on_connect_func=None,
    on_message_func=sub_on_message_func,
    name="subscriber",
)

# pub_broker = config["PUB_BROKER"]
# pub_port = int(config["PUB_PORT"])
pub_topics = "263315ab48dd4982971f157cd97faa4a/rotor/linenotify"
pub_client_id = f"publish-{random.randint(0, 100)}"
pub_mqtt = MqttManager(
    broker="broker.emqx.io",
    port=8083,
    sub_topics=pub_topics,
    pub_topics=None,
    client_id=pub_client_id,
    on_connect_func=pub_on_connect_func,
    on_message_func=None,
    name="publisher",
)

publish = AccumulateETL(
    mqtt_client=pub_mqtt,
    db=pg_async_session(),
    date=current_date,
    # id=62,
    section_code=414273,
    line_id=25,
    machine_no="6TM-0315",
)

scheduler = AsyncIOScheduler()


def print_jobs():
    for job in scheduler.get_jobs():
        print(f"job: {job.name}, next run time: {job.next_run_time}")


async def fetch_data_wrapper():
    try:
        await asyncio.sleep(1)
        data = await publish.get_data()
        print("Data fetched successfully:")
        for row in data:
            print(row)
    except Exception as e:
        print("Error fetching data:", e)




# Schedule data fetching at specified times
for time_str in scheduled_times:
    scheduler.add_job(
        fetch_data_wrapper,
        "cron",
        hour=int(time_str[:2]),
        minute=int(time_str[3:5]),
        second=int(time_str[6:]),
    )


def main():
    # scheduler.add_job(print_jobs, "interval", seconds=5)
    scheduler.start()


if __name__ == "__main__":
    sub_mqtt.initialize()
    pub_mqtt.initialize()
    main()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        scheduler.shutdown()
