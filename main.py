import asyncio
import pandas as pd
import aiosqlite
import aiohttp


database_path = "database.db"


async def create_db():
    async with aiosqlite.connect(database_path) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS weather (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                temperature REAL,
                wind_direction TEXT,
                wind_speed REAL,
                pressure INTEGER,
                precipitation TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.commit()


async def get_weather():
    lat = 55.693668
    lon = 37.349749
    API_key = "b12f28de4df5146532f62a07cce84d02"
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()

    temperature = int((data['main']['temp'] - 273.15))
    wind_speed = data['wind']['speed']
    wind_direction = data['wind']['deg']
    pressure = data['main']['pressure']
    weather_description = data["weather"][0]['description']

    print(f"Температура: {(temperature)} по C")
    print(f"Скорость ветра: {wind_speed} м/с")
    print(f"Напрвление ветра: {wind_direction} degrees")
    print(f"Атмолсферное давление: {pressure} hPa")
    print(f"Тип погоды: {weather_description}")

    async with aiosqlite.connect(database_path) as db:
        await db.execute('''
            INSERT INTO weather (temperature, wind_direction, wind_speed, pressure, precipitation)
            VALUES (?, ?, ?, ?, ?)
        ''', (temperature, wind_direction, wind_speed, pressure, weather_description))
        await db.commit()


async def export_to_excel():
    async with aiosqlite.connect(database_path) as db:
        cursor = await db.execute("SELECT * FROM weather ORDER BY id DESC LIMIT 10")
        rows = await cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

    filename = 'weather.xlsx'
    df.to_excel(filename, index=False, engine='openpyxl')
    print(f'Сохранено в exel {filename}')


async def main():
    await create_db()
    while True:
        try:
            await get_weather()
            await export_to_excel()
            await asyncio.sleep(10)
        except Exception as ex:
            print(f"An error occurred: {ex}")


if __name__ == "__main__":
    asyncio.run(main())
