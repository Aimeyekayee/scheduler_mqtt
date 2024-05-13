import json
from datetime import datetime

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from typing import Optional

from manager.mqtt import MqttManager


class AccumulateETL:
    def __init__(
        self,
        mqtt_client: MqttManager,
        db: AsyncSession,
        date: str,
        section_code: int,
        line_id: int,
        machine_no: str,
    ):
        self.mqtt_client = mqtt_client
        self.db = db
        self.date = date
        self.section_code = section_code
        self.line_id = line_id
        self.machine_no = machine_no

    def set_pub_client(self, client):
        self.pub_client = client

    async def get_data(self):
        print(self.date)
        try:
            stmt = f"""
                SELECT machine_no, date, data
                FROM data_baratsuki
                WHERE machine_no = '{self.machine_no}' AND section_code = {self.section_code}
                AND line_id = {self.line_id} AND date = '2024-05-13 10:30:00'
                """
            print(stmt)
            result = await self.db.execute(text(stmt))
            data = result.fetchall()
            if data:
                machine_no, date, raw_data = data[0]
                formatted_data = {
                    "section_code": self.section_code,
                    "line_id": self.line_id,
                    "machine_no": machine_no,
                    "date": date.strftime("%Y-%m-%d %H:%M:%S"),
                    "data": raw_data,
                }
                # Prepare the payload
                payload = {
                    "section_code": formatted_data["section_code"],
                    "line_id": formatted_data["line_id"],
                    "machine_no": formatted_data["machine_no"],
                    "date": formatted_data["date"],
                    "data": formatted_data["data"],
                }
                payload = json.dumps(payload)
                self.mqtt_client.publish(
                    "263315ab48dd4982971f157cd97faa4a/rotor/linenotify", payload
                )
            else:
                print("error")
            return data
        except Exception as e:
            print("get_data error:", e)
